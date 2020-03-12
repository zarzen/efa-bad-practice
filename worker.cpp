#include "worker.h"

namespace trans {

Worker::Worker(std::string name, std::queue<Tasks*> *task_q_, std::mutex *task_m_,
               std::queue<complete_t> *cq_, std::mutex *cq_mutex_) {
  this->task_mutex = task_m_;
  this->task_q = task_q_;
  this->cq_mutex = cq_mutex_;
  this->cq = cq_;
  this->name = name;
};


void operation(trans::Worker *w) {
  EFAEndpoint *efa_ep = new EFAEndpoint(w->name + "-ep");
  w->ep = efa_ep;
  try {
    while (1) {
      // only pop here, delete lock
      std::lock_guard<std::mutex> _lock(*(w->task_mutex));
      if (!w->task_q->empty()) {
        Tasks *t = w->task_q->front();
        w->task_q->pop();

        auto s = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < t->numTask; ++i) {
          size_t len = t->sizes[i];
          void* _buf = t->bufs[i];
          if (t->type == SEND) {
            int err =
                fi_send(efa_ep->ep, _buf, len, NULL, efa_ep->peer_addr, NULL);
            if (err < 0)
              std::cerr << "fi_send Err: " << err << "\n";
          } else if (t->type == RECV) {
            int err =
                fi_recv(efa_ep->ep, _buf, len, NULL, FI_ADDR_UNSPEC, NULL);
            if (err < 0)
              std::cerr << "fi_recv Err: " << err << "\n";
          } else {
            std::cerr << "impossible task type encoutered\n";
          }
        }
        // delete Tasks pointer
        // delete t;
      }
    }
  } catch (...) {
    std::cout << "Raise exception Worker-operation-thread;\n";
    std::cout << "Terminating current thread\n";
    std::terminate();
  }
};

void Worker::run() {
  t = new std::thread(operation, this);
  t->detach();
  std::this_thread::sleep_for(std::chrono::seconds(1));
};
}; // namespace trans