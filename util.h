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

template <typename T>
class ThdSafeQueue {
 public:
  ThdSafeQueue(){};
  ~ThdSafeQueue(){};

  /**
   * \brief push an value into the end. threadsafe.
   * \param new_value the value
   */
  void push(T new_value) {
    mu_.lock();
    queue_.push(std::move(new_value));
    mu_.unlock();
    cond_.notify_all();
  }

  /**
   * \brief wait until pop an element from the beginning, threadsafe
   * \param value the poped value
   */
  void pop(T* value) {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return !queue_.empty(); });
    *value = std::move(queue_.front());
    queue_.pop();
  }

 private:
  mutable std::mutex mu_;
  std::queue<T> queue_;
  std::condition_variable cond_;
};


namespace trans {

// from ps-lite

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