#include "storage.hpp"
#include <iostream>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

void seghandler(int sig) {
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

// typedef unsigned long long size_t;

int main(int argc, char const* argv[]) {
  signal(SIGSEGV, seghandler);
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