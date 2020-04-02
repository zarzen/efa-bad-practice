#include <cstring>
#include <iostream>
#include <vector>

#include "shm_common.h"
#include "sock_cli_serv.h"
#include "util.h"

using namespace trans;

void cli_efa_addr_exchange(std::string& ip,
                           std::string& port,
                           shm::SHMCommunicator& comm) {
  // make sure local workers are registered their efa addrs
  while (!comm.local_efa_addrs_ready()) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  
  trans::SockCli cli(ip, port);
  std::cout << "socket cli connected to server send local addr to remote\n";
  int addrs_size = comm.nw * shm::EFA_ADDR_SIZE;
  char* local_addrs_buf = new char[addrs_size];
  char* remote_addrs_buf = new char[addrs_size];

  comm.get_local_efa_addrs(local_addrs_buf);
  cli._send(local_addrs_buf, addrs_size);

  cli._recv(remote_addrs_buf, addrs_size);
  comm.set_local_peer_addrs(remote_addrs_buf);

  // make sure addrs inserted
  std::this_thread::sleep_for(std::chrono::seconds(1));
};


int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: ./shm_cli <ip> <port> <shm-prefix> <num-workers> <data-buf-size>";
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  std::string shm_prefix(argv[3]);
  int nw = std::atoi(argv[4]);
  unsigned long long data_buf_size = std::stoull(std::string(argv[5]));
  trans::shm::SHMCommunicator comm(nw, "cli-comm-0", shm_prefix, data_buf_size);

  cli_efa_addr_exchange(ip, port, comm);
  
  comm.run();
}