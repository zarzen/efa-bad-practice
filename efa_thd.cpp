#include "efa_thd.h"

namespace trans {

void efa_worker_thd(std::string thd_name, trans::EFAEndpoint **efa,
                    std::queue<Tasks *> *task_q, std::mutex *task_m) {
  *efa = new trans::EFAEndpoint(thd_name + "-efa-ep");
  struct fid_ep *ep = (*efa)->ep;
  // assume always communicate to peer 0
  // but make sure address already inserted through fi_av_insert()
  fi_addr_t peer_addr = 0;
  try {
    while (1) {
      // only pop here, delete lock
      std::lock_guard<std::mutex> _lock(*(task_m));
      if (!task_q->empty()) {
        if (!(*efa)->av_ready) {
          std::cerr << "address vector of peer is not inserted\n";
          exit(1);
        }

        Tasks *t = task_q->front();
        task_q->pop();

        auto s = std::chrono::high_resolution_clock::now();
        std::cout << "got new tasks " 
                  << s.time_since_epoch().count() << "\n";
        for (int i = 0; i < t->numTask; ++i) {
          size_t len = t->sizes[i];
          void *_buf = t->bufs[i];
          if (t->type == SEND) {
            int err = fi_send(ep, _buf, len, NULL, peer_addr, NULL);
            if (err < 0)
              std::cerr << "fi_send Err: " << err << "\n";
          } else if (t->type == RECV) {
            int err = fi_recv(ep, _buf, len, NULL, FI_ADDR_UNSPEC, NULL);
            if (err < 0)
              std::cerr << "fi_recv Err: " << err << "\n";
          } else {
            std::cerr << "impossible task type encoutered\n";
          }
        }
        auto e = std::chrono::high_resolution_clock::now();
        std::cout << "async tasks complete " 
                  << e.time_since_epoch().count() << "\n";
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
}; // namespace trans