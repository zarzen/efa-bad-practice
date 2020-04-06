#include "storage.hpp"
#include <iostream>

// typedef unsigned long long size_t;

int main(int argc, char const* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: ./store_serv <port>\n";
    return -1;
  }
  std::string port(argv[1]);
  size_t mem_size = 10UL * 1024UL * 1024UL * 1024UL;
  std::cout << "mem size: " << mem_size << "\n";
  std::cout << "size of size_t " << sizeof(size_t) << "\n";
  pipeps::store::ParamStore s("mem-storage", port, mem_size, 4);
  s.run();
  return 0;
}