#ifndef THD_EFA_WORKER_H
#define THD_EFA_WORKER_H

#include "efa_ep.h"
#include "thd_comm.hpp"

namespace pipeps {

void workerWaitCq(std::string& workerName, fid_cq* cq, int count) {
  struct fi_cq_err_entry entry;
  int ret, completed = 0;
  double s = time_now();
  while (completed < count) {
    ret = fi_cq_read(cq, &entry, 1);
    if (ret == -FI_EAGAIN) {
      continue;
    }

    if (ret == -FI_EAVAIL) {
      spdlog::error("Error while checking completion");
      ret = fi_cq_readerr(cq, &entry, 1);
      char _err_buf[100];
      fi_cq_strerror(cq, entry.prov_errno, entry.err_data, _err_buf, 100);
      spdlog::error(
          "Error while calling fi_cq_readerr, err code {:d}, err msg {:s}", ret,
          _err_buf);
    }

    if (ret < 0)
      spdlog::error("{:s} fi_cq_read err", workerName);
    completed++;

    double cost_t = time_now() - s;
    spdlog::debug("{:s} completes {:d} job cost: {:lf} ms", workerName,
                  completed, cost_t * 1e3);
    s = time_now();  // update start time
  }
};

void workerConvertMsg(TransMsg& msg,
                      std::vector<std::pair<void*, size_t>>& ptrs);

void efaWorkerThdFun(std::string workerName,
                     int rank,
                     ThdSafeQueue<TransMsg>* taskq,
                     std::atomic<size_t>* cntr,
                     char* efaAddrs,
                     std::atomic<int>* addrReady) {
  trans::EFAEndpoint efa_ep(workerName + "-efa-ep");
  while (true) {
    TransMsg _m;
    taskq->pop(&_m);


  }
};
};  // namespace pipeps

#endif