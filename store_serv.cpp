#include "storage.hpp"
#include <iostream>

int main(int argc, char const* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: ./store_serv <port>\n";
    return -1;
  }
  std::string port(argv[1]);
  size_t mem_size = 10 * 1024 * 1024 * 1024;
  pipeps::store::ParamStore s("mem-storage", port, mem_size, 4);
  s.run();
  return 0;
}