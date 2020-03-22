#include "shm_common.h"
using namespace trans;

int main(int argc, char* argv[]) {
  if (argc < 5) {
    std::cerr << "must specify worker name\n"
              << "Usage: ./worker <shm-prefix> <world-size> <rank> <data-size>\n";
    return -1;
  }
  std::string shm_prefix(argv[1]);
  int nw = std::stoi(argv[2]);
  int rank = std::atoi(argv[3]);
  unsigned long long int data_buf_size = std::stoull(argv[3]);
  shm::SHMWorker w(shm_prefix, nw, rank, data_buf_size);
  w.run();

  return 0;
}