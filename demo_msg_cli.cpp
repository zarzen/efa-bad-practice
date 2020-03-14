#include "sock_cli_serv.h"
#include "efa_thd.h"
#include "util.h"
#include <iostream>
#include <thread>
#include <queue>

using namespace trans;
int batch_p_size = 2 * 1024 * 1024; // 2MB
int total_size = 200 * 1024 * 1024; // 200MB
char *p_buf ;
char *send_buf;


void cli_efa_address_exchange(std::string ip, std::string port, trans::EFAEndpoint *efa) {
  int numThd = 1; // ignore third parameter
  size_t addr_size = 64;
  // launch socket client
  SockCli cli(ip, port);
  // exchange EFA address with socket server
  char local_ep_addrs[64] = {0};
  char remote_ep_addrs[64] = {0};
  char readable[64] = {0};

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

void fake_param_trans(trans::EFAEndpoint *efa, std::queue<Tasks*> *task_q,
                      std::mutex *task_m) {
    // send a fake request
  int inst_size = 64;
  Tasks *send_once = new Tasks();
  send_once->type = SEND;
  send_once->numTask  = 1;
  std::string req_msg = "<fake-request-for-parameters>";
  memcpy(send_buf, req_msg.c_str(), req_msg.length());
  send_once->bufs.push_back(send_buf);
  send_once->sizes.push_back(inst_size);
  // put the task into queue
  put_tasks(task_q, task_m, send_once);
  wait_cq(efa->txcq, 1);

  // receiving tasks
  auto s = std::chrono::high_resolution_clock::now();
  std::cout << "start new tasks " << s.time_since_epoch().count() << "\n";
  Tasks *recv_p = new Tasks();
  recv_p->type = RECV;
  recv_p->numTask = total_size / batch_p_size;
  for (int i = 0; i < recv_p->numTask; ++i) {
    char* _buf_s = p_buf + i * batch_p_size;
    recv_p->bufs.push_back(_buf_s);
    recv_p->sizes.push_back(batch_p_size);
  }
  put_tasks(task_q, task_m, recv_p);
  std::cout << "after put tasks " 
            << std::chrono::high_resolution_clock::now().time_since_epoch().count() << "\n";
  wait_cq(efa->rxcq, total_size / batch_p_size);

  auto e = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> cost_t = e - s;
  float dur = cost_t.count();
  float bw = (total_size * 8 / (dur / 1000)) / 1e9;
  std::cout << "Recv bw: " << bw << " Gbps\n"
            << "Dur " << dur << " ms \n";

  delete send_once;
  delete recv_p;

};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage ./msg_cli <listen-ip> <listen-port> <numThd>\n";
    return 1;
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  

  trans::EFAEndpoint *efa;
  std::queue<Tasks*> task_q;
  std::mutex task_m;
  // launch thread for EFA endpoint
  std::thread efa_operator(efa_worker_thd, "cli-efa-worker", &efa, &task_q, &task_m);
  // make sure EFAEndpoint created
  std::this_thread::sleep_for(std::chrono::seconds(1));

  cli_efa_address_exchange(ip, port, efa);
  p_buf = new char[total_size];
  send_buf = new char[64];
  
  for (int i = 0; i < 10; i ++ ){
    std::cout << i << " :";
    fake_param_trans(efa, &task_q, &task_m);
    // std::this_thread::sleep_for(std::chrono::seconds(5));
  }
};