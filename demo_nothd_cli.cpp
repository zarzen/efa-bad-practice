#include "sock_cli_serv.h"
// #include "efa_thd.h"
#include "util.h"
#include "efa_ep.h"
#include <iostream>
#include <thread>
#include <queue>

using namespace trans;
int batch_p_size = 1 * 1024 * 1024; // 2MB
int total_size = 200 * 1024 * 1024; // 200MB
char *p_buf ;
char *send_buf;

void cli_pingpong(trans::EFAEndpoint *efa) {
  char *ping_send = new char[batch_p_size];
  char *ping_recv = new char[batch_p_size];
  fi_send(efa->ep, ping_send, batch_p_size, NULL, efa->peer_addr, NULL);
  wait_cq(efa->txcq, 1);
  fi_recv(efa->ep, ping_recv, batch_p_size, NULL, efa->peer_addr, NULL);
  wait_cq(efa->rxcq, 1);
  delete[] ping_send;
  delete[] ping_recv;
};

void cli_efa_address_exchange(std::string ip, std::string port, trans::EFAEndpoint *efa) {
  int numThd = 1; // ignore third parameter
  size_t addr_size = 64;
  // launch socket client
  SockCli cli(ip, port);
  // exchange EFA address with socket server
  char local_ep_addrs[64] = {};
  char remote_ep_addrs[64] = {};
  char readable[64] = {};

  efa->get_name(local_ep_addrs, addr_size);
  size_t len = 64;
  fi_av_straddr(efa->av, local_ep_addrs, readable, &len);
  std::cout << "Local ep addresses: \n"
            << readable << "\n";

  cli._send(local_ep_addrs, addr_size);
  cli._recv(remote_ep_addrs, addr_size);
  len = 64;
  fi_av_straddr(efa->av, remote_ep_addrs, readable, &len);
  std::cout << "Get remote EFA address \n"
            << readable << "\n";

  // insert remote addrs
  fi_av_insert(efa->av, remote_ep_addrs, 1, &(efa->peer_addr), 0, NULL);
  efa->av_ready = true;

  // verify
  char name_buf[addr_size];
  len = 64;
  fi_av_lookup(efa->av, efa->peer_addr, name_buf, &len);
  len = 64;
  std::fill_n(readable, addr_size, 0);
  fi_av_straddr(efa->av, name_buf, readable, &len);
  std::cout << "verified inserted: " << readable << "\n";

};

void fake_param_trans(trans::EFAEndpoint *efa) {
  cli_pingpong(efa); // assume pingpong background message always exist
    // send a fake request
  int inst_size = 64;
  std::string req_msg = "<fake-request-for-parameters>";
  memcpy(send_buf, req_msg.c_str(), req_msg.length());

  fi_send(efa->ep, send_buf, inst_size, NULL, efa->peer_addr, NULL);
  wait_cq(efa->txcq, 1);

  // receiving tasks
  auto s = std::chrono::high_resolution_clock::now();
  std::cout << "start new fi_recv tasks " << s.time_since_epoch().count() << "\n";

  for (int i = 0; i < total_size/batch_p_size; ++i) {
    // char* _buf_s = p_buf + i * batch_p_size;
    // fi_recv(efa->ep, _buf_s, batch_p_size, NULL, 0, NULL);
    fi_recv(efa->ep, p_buf, batch_p_size, NULL, 0, NULL);
  }

  std::cout << "after launch fi_recv s " 
            << std::chrono::high_resolution_clock::now().time_since_epoch().count() << "\n";
  wait_cq(efa->rxcq, total_size / batch_p_size);

  auto e = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> cost_t = e - s;
  float dur = cost_t.count();
  float bw = (total_size * 8 / (dur / 1000)) / 1e9;
  std::cout << "Recv bw: " << bw << " Gbps\n"
            << "Dur " << dur << " ms \n";

};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage ./msg_cli <listen-ip> <listen-port> <numThd>\n";
    return 1;
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  
  trans::EFAEndpoint efa("Cli-efa-ep");
  
  cli_efa_address_exchange(ip, port, &efa);
  p_buf = new char[total_size];
  send_buf = new char[64];
  
  for (int i = 0; i < 10; i ++ ){
    std::cout << i << " :";
    
    fake_param_trans(&efa);
    // std::this_thread::sleep_for(std::chrono::seconds(5));
  }
};