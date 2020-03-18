#include <cstring>
#include <iostream>
#include <vector>

#include "shm_common.h"
#include "sock_cli_serv.h"
#include "util.h"

using namespace trans;
void serv_efa_addr_exchange(std::string& ip,
                            std::string& port,
                            std::vector<shm::WorkerMemory*>& workers) {
  trans::SockServ serv(port);
  std::cout << "server waiting for socket connection\n";
  serv._listen();
  // recv remote efa addrs
  std::cout << "server got connection, recv addr from remote\n";
  for (int i = 0; i < workers.size(); i++) {
    char addr_buf[64];
    serv._recv(addr_buf, 64);
    std::cout << "server got set_peer_addr of worker " << i << "\n";
    set_peer_addr(workers[i], addr_buf);
  }

  // send local efa addrs
  for (int i = 0; i < workers.size(); i++) {
    char addr_buf[64];
    get_worker_efa_addr(workers[i], addr_buf);
    serv._send(addr_buf, 64);
  }
};

void put_efa_recv_instr(shm::WorkerMemory* w) {
  w->mem_lock("efa send request, lock err");
  // recv fake instruction e.g. request for parameters of model xxx
  *(int*)((char*)w->instr_ptr + 8) = shm::reverse_map(shm::RECV_INSTR);
  double ts = trans::time_now();
  *((double*)(w->instr_ptr)) = ts;

  w->mem_unlock("efa send request, unlock err");
};

void put_efa_send_params(shm::WorkerMemory* w) {
  w->mem_lock("efa send params, lock err");
  *(int*)((char*)w->instr_ptr + 8) = shm::reverse_map(shm::SEND_PARAM);
  *((double*)(w->instr_ptr)) = trans::time_now();
  w->mem_unlock("efa send params, unlock err");
}

void fake_serv_params(std::vector<shm::WorkerMemory*>& workers) {
  int cur_w = 0;
  int n_w = workers.size();

  while (get_worker_status(workers[cur_w]) != 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  // recv fake request
  int cur_cntr = get_worker_cntr(workers[cur_w]);
  std::cout << "current cntr " << cur_cntr << "\n";
  put_efa_recv_instr(workers[cur_w]);
  std::cout << "put a recv instr task\n";
  // wait for completion
  while (get_worker_status(workers[cur_w]) != 1 ||
         get_worker_cntr(workers[cur_w]) != cur_cntr + 1) {
    std::cout << "worker status " << get_worker_status(workers[cur_w]) << "\n" 
              << "counter " << get_worker_cntr(workers[cur_w]) << "\n";
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  double st = trans::time_now();
  int total_cur_cntr = 0;
  for (int i = 0; i < n_w; i++) {
    total_cur_cntr += get_worker_cntr(workers[i]);
  }
  int target_cntr = total_cur_cntr + n_w * 20; // 100MB/5MB
  // start send parameters
  for (int i = 0; i < n_w; i++) {
    put_efa_send_params(workers[i]);
  }

  // wait for job completion
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
  double bw = (workers.size() * 100 * 1024 * 1024 * 8 / (et - st)) / 1e9;
  std::cout << "Send params bw: " << bw << " Gbps\n"
            << "dur: " << et - st << " s\n";
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: ./shm_serv <ip> <port>";
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

  serv_efa_addr_exchange(ip, port, sharedWorkers);
  // make sure addrs inserted
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // exp 5 time
  for (int i = 0; i < 5; i++) {
    fake_serv_params(sharedWorkers);
  }
  
}