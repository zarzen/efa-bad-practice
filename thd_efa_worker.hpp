#ifndef THD_EFA_WORKER_H
#define THD_EFA_WORKER_H

#include <rdma/fi_tagged.h>
#include "efa_ep.h"
#include "thd_comm.hpp"

namespace pipeps {

void workerWaitCq(std::string& caller, fid_cq* cq, int count) {
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
      spdlog::error("{:s} fi_cq_read err", caller);
    completed++;

    double cost_t = time_now() - s;
    spdlog::debug("{:s} completes {:d} job cost: {:lf} ms", caller, completed,
                  cost_t * 1e3);
    s = time_now();  // update start time
  }
};

void workerConvertMsg(TransMsg& msg,
                      std::vector<std::pair<void*, size_t>>& ptrs);

void verifyEFAPeerAddr(trans::EFAEndpoint& efa) {
  char name_buf[ThdCommunicator::efaAddrSize];
  size_t len = 64;
  char readable[64] = {0};
  fi_av_lookup(efa.av, efa.peer_addr, name_buf, &len);
  len = 64;
  std::fill_n(readable, 64, 0);
  fi_av_straddr(efa.av, name_buf, readable, &len);
  spdlog::debug("{:s} verify peer addr {:s}", efa.nickname, readable);
};

void efaSendRecv(trans::EFAEndpoint& efa,
                 TransMsg& msg,
                 std::vector<std::pair<void*, size_t>>& dataLoc,
                 std::atomic<size_t>* cntr);

void efaWorkerThdFun(std::string workerName,
                     int rank,
                     ThdSafeQueue<TransMsg>* taskq,
                     std::atomic<size_t>* cntr,
                     char* efaAddrs,
                     std::atomic<int>* addrReady) {
  trans::EFAEndpoint efa_ep(workerName + "-efa-ep");
  char* addrPtr = efaAddrs + rank * pipeps::ThdCommunicator::efaAddrSize;
  efa_ep.get_name(addrPtr, pipeps::ThdCommunicator::efaAddrSize);
  addrReady += 1;
  spdlog::debug("{:s} :: EFA address ready");

  bool exit = false;
  spdlog::debug("{:s} :: Event process loop start");
  while (!exit) {
    TransMsg _msg;
    taskq->pop(&_msg);
    spdlog::debug("{:s} got task {:s} delayed {:lf}s", workerName,
                  MsgTyepStr(_msg.t), trans::time_now() - _msg.ts);

    switch (_msg.t) {
      case SHUTDOWN:
        exit = true;
        break;
      case INS_EFA_ADDR_INFO:
        efa_ep.insert_peer_address(_msg.data);
        verifyEFAPeerAddr(efa_ep);
        (*cntr)++;  // inidcate the insertion done
        break;
      case SEND_ONE:
      case SEND_BATCH:
      case RECV_ONE:
      case RECV_BATCH:
        // do EFA jobs
        std::vector<std::pair<void*, size_t>> dataLoc;
        workerConvertMsg(_msg, dataLoc);
        // this is a synchronize fun
        efaSendRecv(efa_ep, _msg, dataLoc, cntr);
        break;
      default:
        break;
    }
  }
  spdlog::debug("{:s} :: Exit event loop");
};

void workerConvertMsg(TransMsg& msg,
                      std::vector<std::pair<void*, size_t>>& ptrs) {
  double start = trans::time_now();
  // first 4 bytes
  int nblock = *(int*)msg.data;
  // remaining a pairs
  char* pairBuf = msg.data + 4;
  for (int i = 0; i < nblock; i++) {
    char* _pairData = pairBuf + i * 16;
    void* _ptr;
    memcpy(&_ptr, _pairData, 8);
    size_t _size = *(size_t*)(_pairData + 8);
    ptrs.push_back(std::make_pair<void*, size_t>(_ptr, _size));
  }
  spdlog::debug("workerConvertMsg cost: {:lf}", trans::time_now() - start);
}

void fi_tsend_or_trecv(MsgType& mType,
                       struct fid_ep* ep,
                       char* bufPtr,
                       size_t len,
                       fi_addr_t dest_addr,
                       uint64_t tag) {
  if (mType == SEND_ONE || mType == SEND_BATCH) {
    fi_tsend(ep, bufPtr, len, NULL, dest_addr, tag, NULL);
  } else {
    fi_trecv(ep, bufPtr, len, NULL, dest_addr, tag, 0, NULL);
  }
}

void efaSendRecv(trans::EFAEndpoint& efa,
                 TransMsg& msg,
                 std::vector<std::pair<void*, size_t>>& dataLoc,
                 std::atomic<size_t>* cntr) {
  //
  size_t slice_threshold = 2 * 1024 * 1024;  // 2MB
  int task_seq = 0;
  // std::vector<int> waitSizes;
  // get task specific cq
  fid_cq* cq;
  if (msg.t == SEND_ONE || msg.t == SEND_BATCH) {
    cq = efa.txcq;
  } else {
    cq = efa.rxcq;
  }

  // process send/recv tasks
  for (int i = 0; i < dataLoc.size(); i++) {
    // for each batch
    void* _ptr = dataLoc[i].first;
    size_t _size = dataLoc[i].second;

    int n_subtasks = _size / slice_threshold;
    size_t processed = 0;
    for (int j = 0; j < n_subtasks; j++) {
      char* _bufPtr = (char*)_ptr + j * slice_threshold;
      fi_tsend_or_trecv(msg.t, efa.ep, _bufPtr, slice_threshold, efa.peer_addr,
                        task_seq);
      task_seq++;
    }
    size_t remainSize = _size - n_subtasks * slice_threshold;
    if (remainSize > 0) {
      n_subtasks += 1;
      char* _bufPtr = (char*)_ptr + (n_subtasks - 1) * slice_threshold;
      fi_tsend_or_trecv(msg.t, efa.ep, _bufPtr, remainSize, efa.peer_addr,
                        task_seq);
      task_seq++;
    } else if (remainSize < 0) {
      spdlog::error("!!!not possible to have remain size lower than 0");
    }
    // later if we use different strategy to wait
    // waitSizes.push_back(n_subtasks);

    // wait for each data block transmission
    spdlog::debug("{:s} :: waiting for {:d} sub-tasks of data block {:d}",
                  efa.nickname, n_subtasks, i);
    workerWaitCq(efa.nickname, cq, n_subtasks);
    (*cntr)++;  // increase worker counter
  }
}

};  // namespace pipeps

#endif