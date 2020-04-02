#include "shm_common.h"
using namespace trans;

int main(int argc, char* argv[]) {
  if (argc < 6) {
    std::cerr << "must specify worker name\n"
              << "Usage: ./worker <comm-name> <world-size> <rank> <data-buf-name> <data-size>\n";
    return -1;
  }
  std::string comm_name(argv[1]);
  int nw = std::stoi(argv[2]);
  int rank = std::atoi(argv[3]);
  std::string data_buf_name(argv[4]);
  size_t data_buf_size = std::stoull(argv[5]);
  shm::SHMWorker w(comm_name, nw, rank, data_buf_name, data_buf_size);
  w.run();

  return 0;
}