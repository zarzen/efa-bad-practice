#ifndef WORKER_H
#define WORKER_H

#include "efa_ep.h"
#include "util.h"
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace trans {


class Worker {
public:
  std::string name;
  EFAEndpoint *ep;
  std::mutex *task_mutex;
  std::queue<Tasks*> *task_q;
  std::mutex *cq_mutex;
  std::queue<complete_t> *cq;
  std::thread *t;

  Worker(std::string name, std::queue<Tasks*> *task_q_, std::mutex *task_m_,
         std::queue<complete_t> *cq_, std::mutex *cq_mutex_);
  void run();
};

}; // namespace trans

#endif /* WORKER_H */