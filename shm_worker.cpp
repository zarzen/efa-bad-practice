#include "shm_common.h"
using namespace trans;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "must specify worker name\n"
              << "Usage: ./worker <shm-prefix> <rank> <data-size>\n";
  }
  std::string shm_prefix(argv[1]);
  int rank = std::atoi(argv[2]);
  unsigned long long int data_buf_size = std::stoull(argv[3]);
  shm::SHMWorker w(shm_prefix, rank, data_buf_size);
  w.run();

  return 0;
}