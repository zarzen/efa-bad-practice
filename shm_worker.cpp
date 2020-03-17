#include "shm_common.h"
using namespace trans;

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "must specify worker name\n"
              << "Usage: ./worker <worker-name>\n";
  }
  std::string worker_name(argv[1]);
  shm::SHMWorker w(worker_name);
  w.run();

  return 0;
}