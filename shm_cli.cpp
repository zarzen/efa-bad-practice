#include <cstring>
#include <iostream>
#include <vector>

#include "shm_common.h"
#include "sock_cli_serv.h"
#include "util.h"

using namespace trans;

void cli_efa_addr_exchange(std::string& ip,
                           std::string& port,
                           std::vector<shm::WorkerMemory*>& workers) {
  trans::SockCli cli(ip, port);
  std::cout << "socket cli connected to server send local addr to remote\n";
  for (int i = 0; i < workers.size(); i++) {
    char add_buf[64];
    workers[i]->print_sem_mutex_val();
    get_worker_efa_addr(workers[i], add_buf);
    std::cout << "cli got local efa addr\n";
    cli._send(add_buf, 64);
  }

  // recv remote EFA addrs
  for (int i = 0; i < workers.size(); i++) {
    char addr_buf[64];
    cli._recv(addr_buf, 64);

    // insert the address into worker
    set_peer_addr(workers[i], addr_buf);
  }
};


void put_efa_send_instr(shm::WorkerMemory* w) {
  w->mem_lock("efa send request, lock err");
  // EFA send instr
  *(int*)((char*)w->instr_ptr + 8) = shm::reverse_map(shm::SEND_INSTR);
  std::string req_msg = "<fake-request-for-parameters>";
  memcpy((char*)w->instr_ptr + 12, req_msg.c_str(), req_msg.length());
  *((double*)(w->instr_ptr)) = trans::time_now();
  std::cout << "fake request " << req_msg << "\n";
  w->mem_unlock("efa send request, unlock err");
};

void put_efa_recv_params(shm::WorkerMemory* w) {
  w->mem_lock("efa recv params, lock err");
  *(int*)((char*)w->instr_ptr + 8) = shm::reverse_map(shm::RECV_PARAM);
  *((double*)(w->instr_ptr)) = trans::time_now();
  w->mem_unlock("efa recv params, unlock err");
};

void fake_param_trans(std::vector<shm::WorkerMemory*>& workers) {
  int cur_w = 0;
  int n_w = workers.size();

  while (get_worker_status(workers[cur_w]) != 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // set fake request
  int cur_cntr = get_worker_cntr(workers[cur_w]);
  std::cout << "cur cntr " << cur_cntr << "\n";
  put_efa_send_instr(workers[cur_w]);
  std::cout << "send a fake request\n";
  // wait
  while (get_worker_status(workers[cur_w]) != 1 ||
         get_worker_cntr(workers[cur_w]) != cur_cntr + 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  cur_cntr = get_worker_cntr(workers[cur_w]);


  double st = trans::time_now();
  int total_cur_cntr = 0;
  for (int i = 0; i < n_w; i++) {
    total_cur_cntr += get_worker_cntr(workers[i]);
  }
  int target_cntr = total_cur_cntr + n_w * 20; // 100MB/5MB
  // start receive
  for (int i = 0; i < n_w; i++) {
    put_efa_recv_params(workers[i]);
  }

  while (1) {
    int progress_cntr = 0;
    for (int i = 0; i < n_w; i++) {
      shm::WorkerMemory* w = workers[i];
      progress_cntr += get_worker_cntr(w);
    }
    if (progress_cntr == target_cntr)
      break;
    else
      std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  double et = trans::time_now();
  double bw = (workers.size() *100 * 1024 * 1024 * 8 / (et - st)) / 1e9;
  std::cout << "Recv params bw: " << bw << " Gbps\n"
            << "dur: " << et - st << " s\n";
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
  std::vector<shm::WorkerMemory*> sharedWorkers;
  for (int i = 0; i < n_workers; ++i) {
    std::cout << "input name of worker: \n";
    std::string name;
    std::cin >> name;
    worker_names.push_back(name);
    sharedWorkers.push_back(new shm::WorkerMemory(name, false));
  }
  cli_efa_addr_exchange(ip, port, sharedWorkers);
  // make sure addrs inserted
  std::this_thread::sleep_for(std::chrono::seconds(1));

  for (int i = 0; i < 5; i++) {
    fake_param_trans(sharedWorkers);
  }
  // delete workers memory
}