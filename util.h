#ifndef UTIL_H
#define UTIL_H

#include <chrono>
#include <mutex>
#include <queue>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thread>

#define CHK_ERR(name, cond, err)                                               \
  do {                                                                         \
    if (cond) {                                                                \
      fprintf(stderr, "%s: %s\n", name, strerror(-(err)));                     \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

namespace trans {

enum task_t { SEND, RECV };
enum complete_t { C_SEND, C_RECV };

void wait_cq(fid_cq *cq, int count);

class Tasks {
public:
  task_t type;
  std::vector<char *> bufs;
  std::vector<int> sizes;
  int numTask;
  Tasks();
};

void put_tasks(std::queue<Tasks *> *q, std::mutex *m, Tasks *t);
double time_now();
}; // namespace trans

#endif