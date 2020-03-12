
#include "util.h"
#include <iostream>

namespace trans {

Tasks::Tasks(){};

void wait_cq(fid_cq *cq, int count) {
  struct fi_cq_err_entry entry;
  int ret, completed = 0;
  int timeout = 100000;
  fi_addr_t from;
  // printf("wait_cq cq addr %p\n", cq);
  auto s = std::chrono::high_resolution_clock::now();
  while (completed < count) {
    // ret = fi_cq_readfrom(cq, &entry, 1, &from);
    // ret = fi_cq_sread(cq, &entry, 1, NULL, timeout);
    ret = fi_cq_read(cq, &entry, 1);
    if (ret == -FI_EAGAIN) {
      // std::this_thread::sleep_for(std::chrono::nanoseconds(100));
      continue;
    }

    if (ret == -FI_EAVAIL) {
      ret = fi_cq_readerr(cq, &entry, 1);
      CHK_ERR("fi_cq_readerr", (ret != 1), ret);

      printf("Completion with error: %d\n", entry.err);
      // if (entry.err == FI_EADDRNOTAVAIL)
      // 	get_peer_addr(entry.err_data);
    }

    CHK_ERR("fi_cq_read ????", (ret < 0), ret);
    completed++;
    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> cost_t = e - s;
    std::cout << completed << " job cost : " << cost_t.count() << " ms\n";
    s = std::chrono::high_resolution_clock::now();
  }
};

void put_tasks(std::queue<Tasks *> *q, std::mutex *m, Tasks *t) {
  std::lock_guard<std::mutex> _lock(*m);
  q->emplace(t);
};

}; // namespace trans
