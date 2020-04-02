#include <cstring>
#include <iostream>
#include <vector>

#include "shm_common.h"
#include "sock_cli_serv.h"
#include "util.h"

using namespace trans;

void serv_efa_addr_exchange(std::string& ip,
                            std::string& port,
                            shm::SHMCommunicator& comm) {
  // make sure local workers are registered their efa addrs
  while (!comm.local_efa_addrs_ready()) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  // open socket server for EFA addrs exchange
  trans::SockServ serv(port);
  std::cout << "server waiting for socket connection\n";
  serv._listen();

  // recv remote efa addrs
  int addrs_size = comm.nw * shm::EFA_ADDR_SIZE;
  char* remote_addrs_buf = new char[addrs_size];
  serv._recv(remote_addrs_buf, comm.nw*shm::EFA_ADDR_SIZE);
  comm.set_local_peer_addrs(remote_addrs_buf);

  // send local efa addrs
  char* local_addrs_buf = new char[addrs_size];
  comm.get_local_efa_addrs(local_addrs_buf);
  serv._send(local_addrs_buf, addrs_size);

  // make sure addrs inserted
  std::this_thread::sleep_for(std::chrono::seconds(1));
  delete[] remote_addrs_buf;
  delete[] local_addrs_buf;
};


int main(int argc, char* argv[]) {
  std::cout << "server side communicator daemon\n";
  if (argc < 7) {
    std::cerr << "Usage: ./shm_serv <ip> <port> <comm-name> <num-workers> <data-buf-name> <data-buf-size>";
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  std::string comm_name(argv[3]);
  int nw = std::atoi(argv[4]);
  std::string data_buf_name(argv[5]);
  size_t data_buf_size = std::stoull(std::string(argv[6]));

  trans::shm::SHMCommunicator comm(nw, comm_name, data_buf_name, data_buf_size);
  serv_efa_addr_exchange(ip, port, comm);

  // daemon
  comm.run();
  
}