#include <cstring>
#include <iostream>
#include <vector>

#include "shm_worker.cpp"
#include "sock_cli_serv.h"
#include "util.h"

void get_worker_efa_addr(trans::WorkerMemory* shm_w, char* addr_buf) {
  memcpy(addr_buf, shm_w->efa_add_ptr, shm_w->efa_addr_size);
};

void cli_efa_addr_exchange(std::string& ip,
                           std::string& port,
                           std::vector<trans::WorkerMemory*>& workers) {
  trans::SockCli cli(ip, port);
  for (int i = 0; i < workers.size(); i++) {
    char add_buf[64];
    get_worker_efa_addr(workers[i], add_buf);
    cli._send(add_buf, 64);
  }

  // recv remote EFA addrs
  for (int i = 0; i < workers.size(); i++) {
    char addr_buf[64];
    cli._recv(addr_buf, 64);

    // insert the address into worker
    *(int*)((char*)workers[i]->instr_ptr + 8) = 1;  // SET EFA ADDR
    memcpy((char*)workers[i]->instr_ptr + 12, addr_buf, 64);
    // set timestamp at last,
    double timestamp = trans::time_now();
    *((double*)(workers[i]->instr_ptr)) = timestamp;
  }
};

void fake_param_trans(std::vector<trans::WorkerMemory*>& workers) {
  int cur_w = 0;
  int n_w = workers.size();

  while (*(int*)(workers[cur_w]->status_ptr) != 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // set fake request
  *(int*)((char*)workers[cur_w]->instr_ptr + 8) = 3;  // EFA send instr
  int cur_cntr = *(int*)workers[cur_w]->cntr_ptr;
  std::string req_msg = "<fake-request-for-parameters>";
  memcpy((char*)workers[cur_w]->instr_ptr + 12, req_msg.c_str(),
         req_msg.length());
  double ts = trans::time_now();
  *((double*)(workers[cur_w]->instr_ptr)) = ts;
  // wait
  while (*(int*)(workers[cur_w]->status_ptr) != 1 &&
         *(int*)workers[cur_w]->cntr_ptr != cur_w + 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // start receive
  for (int i = 0; i < n_w; i++) {
    trans::WorkerMemory* w = workers[i];
    *(int*)((char*)workers[cur_w]->instr_ptr + 8) =
        trans::reverse_map(trans::RECV_PARAM);
    *((double*)(w->instr_ptr)) = trans::time_now();
  }

  while (1) {
    bool all_done = true;
    for (int i = 0; i < n_w; i++) {
      trans::WorkerMemory* w = workers[i];
      if (*(int*)w->status_ptr != 1) {
        all_done = false;
      }
    }
    if (all_done)
      break;
  }

  
};

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: ./shm_cli <ip> <port>";
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  int n_workers;
  std::cout << "input number of workers:\n";
  std::cin >> n_workers;

  std::vector<std::string> worker_names;
  std::vector<trans::WorkerMemory*> sharedWorkers;
  for (int i = 0; i < n_workers; ++i) {
    std::cout << "input name of worker: \n";
    std::string name;
    std::cin >> name;
    worker_names.push_back(name);
    sharedWorkers.push_back(new trans::WorkerMemory(name, false));
  }
  cli_efa_addr_exchange(ip, port, sharedWorkers);

  fake_param_trans(sharedWorkers);
  // delete workers memory
}