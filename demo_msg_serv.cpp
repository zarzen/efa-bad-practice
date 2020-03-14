#include "sock_cli_serv.h"
#include "efa_thd.h"
#include "util.h"
#include <iostream>
#include <thread>
#include <queue>

using namespace trans;

int batch_p_size = 2 * 1024 * 1024; // 2MB
int total_size = 200 * 1024 * 1024; // 200MB
char *req_buf;
char *p_buf;

static const char integ_alphabet[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length =
    (sizeof(integ_alphabet) / sizeof(*integ_alphabet)) - 1;
void ft_fill_buf(void *buf, int size) {

  char *msg_buf;
  int msg_index;
  static unsigned int iter = 0;
  int i;

  msg_index = ((iter++) * 7) % integ_alphabet_length;
  msg_buf = (char *)buf;
  for (i = 0; i < size; i++) {
    msg_buf[i] = integ_alphabet[msg_index++];
    if (msg_index >= integ_alphabet_length)
      msg_index = 0;
  }
};

void serv_efa_address_exchange(std::string ip, std::string port, trans::EFAEndpoint *efa) {
  int numThd = 1; // ignore third parameter
  size_t addr_size = 64;
  // launch socket client
  SockServ serv(port);
  serv._listen();

  // exchange EFA address with socket server
  char local_ep_addrs[64] = {0};
  char remote_ep_addrs[64] = {0};
  char readable[64] = {0};

  efa->get_name(local_ep_addrs, addr_size);
  size_t len = 64;
  fi_av_straddr(efa->av, local_ep_addrs, readable, &len);
  std::cout << "Local ep addresses: \n"
            << readable << "\n";

  serv._recv(remote_ep_addrs, addr_size);
  serv._send(local_ep_addrs, addr_size);
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

void fake_serv_param(trans::EFAEndpoint *efa, std::queue<Tasks*> *task_q,
                      std::mutex *task_m) {
// recv a fake request from client
  int inst_size = 64;
  Tasks *recv_once = new Tasks();
  recv_once->type = RECV;
  recv_once->bufs.push_back(req_buf);
  recv_once->sizes.push_back(inst_size);
  recv_once->numTask = 1;
  put_tasks(task_q, task_m, recv_once);
  wait_cq(efa->rxcq, 1);
  printf("Recv request msg: %s\n", req_buf);

  // send parameter tasks
  
  // ft_fill_buf(p_buf, total_size);

  double st = time_now();
  std::cout << "-- start send tasks at " << st << "\n";
  Tasks *send_p = new Tasks();
  send_p->type = SEND;
  send_p->numTask = total_size / batch_p_size;
  for (int i = 0; i < send_p->numTask; ++i) {
    char* _buf_s = p_buf + i * batch_p_size;
    send_p->bufs.push_back(_buf_s);
    send_p->sizes.push_back(batch_p_size);
  }
  std::cout << "-- right before put tasks to queue " << time_now() << "\n";
  put_tasks(task_q, task_m, send_p);
  std::cout << "-- start to wait tasks completion " << time_now() << "\n";
  wait_cq(efa->txcq, send_p->numTask);

  double et = time_now();
  double dur = et - st;
  float bw = (total_size * 8 / dur) / 1e9;
  std::cout << "Send bw: " << bw << " Gbps\n";

  delete recv_once;
  delete send_p;

};


int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage ./msg_serv <listen-ip> <listen-port> <numThd>\n";
    return 1;
  }

  std::string ip(argv[1]);
  std::string port(argv[2]);

  trans::EFAEndpoint *efa;
  std::queue<Tasks*> task_q;
  std::mutex task_m;
  // launch thread for EFA endpoint
  std::thread efa_operator(efa_worker_thd, "cli-efa-worker", &efa, &task_q, &task_m);
  efa_operator.detach();
  // make sure EFAEndpoint created
  std::this_thread::sleep_for(std::chrono::seconds(1));

  serv_efa_address_exchange(ip, port, efa);
  p_buf = new char[total_size];
  req_buf = new char[64];
  ft_fill_buf(p_buf, total_size);
  for (int i = 0; i < 10; i ++ ){
    fake_serv_param(efa, &task_q, &task_m);
    // std::this_thread::sleep_for(std::chrono::seconds(5));
  }
};