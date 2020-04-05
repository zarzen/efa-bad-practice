#include "storage.hpp"

int main(int argc, char const *argv[])
{
  size_t mem_size = 10 * 1024 * 1024 * 1024;
  pipeps::store::ParamStore s("mem-storage", "8888", mem_size);
  s.run();
  return 0;
}