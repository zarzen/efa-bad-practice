#include "thd_comm.hpp"

namespace trans {

void ThdCommunicator::setListenPort(std::string port) {
  this->listenPort = port;
}

void ThdCommunicator::randomName() {
  char name_buf[5] = {'\0'};
  for (int i = 0; i < 4; i++) {
    int idx = i % RAND_STR.length();
    *(name_buf + i) = RAND_STR.at(idx);
  }
  this->name = "thd-comm-" + std::string(name_buf);
}

void ThdCommunicator::init() {
  if (this->name == "") {
    randomName();
  }

  efaAddrs = new char[nw * efaAddrSize];
  addrReadyC = new std::atomic<int>(0);
  this->startEFAWorkers(nw);
  this->waitLocalAddrs();
  spdlog::debug("workers' EFA addresses are ready");

  // launch socket listener
  sockThdPtr = new std::thread(socketListenerThdFun, this, listenPort, efaAddrs,
                               nw * efaAddrSize);
  spdlog::debug("EFA address server started");
  // launch cntr
  cntrThdPtr = new std::thread(cntrMonitorThdFun, this);
  spdlog::debug("Communicator cntr update thread started");
}

void ThdCommunicator::startEFAWorkers(int nw) {
  // start workers
  for (int i = 0; i < nw; i++) {
    std::string _wn = this->name + "-worker-" + std::to_string(i);
    ThdSafeQueue<TransTask>* _wtq = new ThdSafeQueue<TransTask>();
    std::atomic<size_t>* _wc = new std::atomic<size_t>();
    std::thread _wt(efaWorkerThdFun, _wn, i, _wtq, _wc, efaAddrs, addrReadyC);
    workerThds.push_back(std::move(_wt));
    workerCntrs.push_back(_wc);
    workerTaskQs.push_back(_wtq);
    spdlog::debug("{:s} started worker {:s} ", this->name, _wn);
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

void ThdCommunicator::waitLocalAddrs() {
  // make sure EFA addr ready via addrReadyC
  while (*addrReadyC < nw) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

ThdCommunicator::ThdCommunicator(std::string listenPort,
                                 std::string dstIP,
                                 std::string dstPort,
                                 int nw)
    : nw(nw),
      listenPort(listenPort),
      dstIP(dstIP),
      dstPort(dstPort),
      name("thd-comm-" + listenPort) {
  this->init();
};

ThdCommunicator::ThdCommunicator(int nw)
    : nw(nw), listenPort(listenPort), dstIP(""), dstPort(""), name("") {
  this->init();
}

// set the communicator status to stop for socket thread to quit
// send msg to workers to stop
// wait for worker threads to join
// wait for socket thread to stop
// clean resource
ThdCommunicator::~ThdCommunicator() {
  // 0: ===========
  this->exit = true;
  // 0: ===========

  // 1: ======= hack to stop the socketListener
  trans::SockCli cli("127.0.0.1", listenPort);
  char* _t = new char[nw * efaAddrSize];
  cli._recv(_t, nw * efaAddrSize);
  sockThdPtr->join();
  delete[] _t;
  // 1: ======= end stopping socket Listener

  // 2: ======= wait for workers to complete
  for (int i = 0; i < nw; i++) {
    TransTask _stop_msg(SHUTDOWN, 1);
    workerTaskQs[i]->push(std::move(_stop_msg));
    workerThds[i].join();
  }
  // 2: ======= wait for workers end

  // 3: stop wait cntr thd
  cntrThdPtr->join();
  // 3: end

  // clean resources
  delete[] efaAddrs;
  delete cntrThdPtr;
  delete sockThdPtr;
  for (int i = 0; i < nw; i++) {
    delete workerCntrs[i];
    delete workerTaskQs[i];
  }
  delete addrReadyC;
};

// only used for
void ThdCommunicator::socketListenerThdFun(ThdCommunicator* comm,
                                           std::string port,
                                           char* addrsBuf,
                                           size_t addrsLen) {
  trans::SockServ* serv;
  if (port != "") {
    serv = new trans::SockServ(port);
  } else {
    serv = new trans::SockServ();
    comm->setListenPort(serv->getListenPort());
  }

  spdlog::debug("socketListenerThdFun listend at {:s}", port);
  while (!comm->exit) {
    // quick hack to stop this thread:
    // set the comm->exit = true;
    // then connect to this sock and recv
    int cli_fd = serv->acceptCli();  // here will block;
    spdlog::debug("socketListenerThdFun got a connection sock fd: {:d}",
                  cli_fd);
    size_t ret = send(cli_fd, addrsBuf, addrsLen, 0);
    if (ret == -1) {
      spdlog::critical("Err, while sending out EFA addrs");
    }
  }

  delete serv;
  spdlog::info("socketListenerThdFun exit");
};

void ThdCommunicator::cntrMonitorThdFun(ThdCommunicator* comm) {
  spdlog::info("cntrMonitorThdFun started");
  while (!comm->exit) {
    bool inc = true;
    // this is bounded because of the task splitting
    // check whether all worker cntrs greater than current communicator cntr
    for (auto c_ptr : comm->workerCntrs) {
      // spdlog::debug("comm cntr: {:d}, worker cntr {:d}", comm->cntr, *c_ptr);
      if (*c_ptr <= comm->cntr) {
        inc = false;
      }
    }
    if (inc) {
      comm->cntr += 1;
      spdlog::debug("cntrMonitorThdFun increase communicator cntr to {:d}",
                    comm->cntr);
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  spdlog::info("cntrMonitorThdFun ended");
};

void ThdCommunicator::_sendTask(
    TaskType t,
    std::vector<std::pair<char*, size_t>>& dataLoc) {
  for (int wi = 0; wi < this->nw; wi++) {
    TransTask msg(t, 4 + dataLoc.size() * 16);
    // fill msg content
    *(int*)msg.data = dataLoc.size();
    spdlog::debug("assemble TransTask of worker {:d}", wi);
    for (int j = 0; j < dataLoc.size(); j++) {
      char* _ptr = dataLoc[j].first;
      size_t _size = dataLoc[j].second;
      size_t _worker_size = _size / nw;
      _ptr = _ptr + wi * _worker_size;
      // last worker has more respon
      if (wi == nw - 1) {
        _worker_size += (_size - _worker_size * nw);
      }
      // save value of _ptr to char array
      memcpy(msg.data + 4 + j * 16, &_ptr, 8);
      *(size_t*)(msg.data + 4 + j * 16 + 8) = _worker_size;
    }
    workerTaskQs[wi]->push(std::move(msg));
    spdlog::debug("TransTask enqueued of worker {:d}", wi);
  }
}

void ThdCommunicator::asendBatch(
    std::vector<std::pair<char*, size_t>> dataLoc) {
  while (!peerAddrReady) {
    getPeerAddrs();
  }
  spdlog::debug("asendBatch EFA addrs are ready");
  this->_sendTask(SEND_BATCH, dataLoc);
}

void ThdCommunicator::arecvBatch(
    std::vector<std::pair<char*, size_t>> dataLoc) {
  while (!peerAddrReady) {
    getPeerAddrs();
  }
  spdlog::debug("arecvBatch EFA addrs are ready");
  _sendTask(RECV_BATCH, dataLoc);
}

bool ThdCommunicator::getPeerAddrs() {
  spdlog::debug("enter ThdCommunicator::getPeerAddrs");
  if (this->dstIP == "" || this->dstPort == "") {
    spdlog::error("Did not set peer IP and Port, unable to get peer EFA addrs");
    return false;
  }
  try {
    trans::SockCli sCli(dstIP, dstPort);
    spdlog::debug("connected to {:s}:{:s}", dstIP, dstPort);
    char* peerAddrs = new char[nw * efaAddrSize];
    sCli._recv(peerAddrs, nw * efaAddrSize);
    for (int wi = 0; wi < nw; wi++) {
      char* _addr = peerAddrs + wi * efaAddrSize;
      TransTask m(INS_EFA_ADDR_INFO, efaAddrSize);
      memcpy(m.data, _addr, efaAddrSize);
      workerTaskQs[wi]->push(std::move(m));
      spdlog::debug("INS_EFA_ADDR_INFO enqueued into task q of worker {:d}",
                    wi);
    }
    // wait for insertion complete
    // insert EFA addrs must be the first job to complete
    while (*addrReadyC < 2 * nw) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    spdlog::debug("worker all inserted peer Addrs");
    peerAddrReady = true;
  } catch (...) {
    spdlog::error("Error occur while getPeerAddrs; should retry");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
};

// =================== end of ThdCommunicator implementations ======

void workerWaitCq(std::string& caller, fid_cq* cq, int count) {
  struct fi_cq_err_entry entry;
  int ret, completed = 0;
  double s = trans::time_now();
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

    double cost_t = trans::time_now() - s;
    spdlog::debug("{:s} completes {:d} job cost: {:f} ms", caller, completed,
                  cost_t * 1e3);
    s = trans::time_now();  // update start time
  }
};

void workerConvertMsg(trans::TransTask& msg,
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
                 trans::TransTask& msg,
                 std::vector<std::pair<void*, size_t>>& dataLoc,
                 std::atomic<size_t>* cntr);

void efaWorkerThdFun(std::string workerName,
                     int rank,
                     ThdSafeQueue<TransTask>* taskq,
                     std::atomic<size_t>* cntr,
                     char* efaAddrs,
                     std::atomic<int>* addrReady) {
  trans::EFAEndpoint efa_ep(workerName + "-efa-ep");
  char* addrPtr = efaAddrs + rank * trans::ThdCommunicator::efaAddrSize;
  efa_ep.getAddr(addrPtr, trans::ThdCommunicator::efaAddrSize);
  char readable[64];
  size_t len = 64;
  fi_av_straddr(efa_ep.av, addrPtr, readable, &len);
  (*addrReady) += 1;
  spdlog::debug("{:s} :: EFA address ready {:s}", workerName, readable);

  bool exit = false;
  spdlog::debug("{:s} :: Event process loop start", workerName);
  while (!exit) {
    TransTask _msg;
    taskq->pop(&_msg);
    std::string _tstr = type2str(_msg.t);
    spdlog::debug("{:s} got task {:s} delayed {:f} s", workerName, _tstr,
                  trans::time_now() - _msg.ts);

    switch (_msg.t) {
      case SHUTDOWN:
        exit = true;
        break;
      case INS_EFA_ADDR_INFO: {
        efa_ep.insertPeerAddr(_msg.data);
        verifyEFAPeerAddr(efa_ep);
        spdlog::debug("{:s} EFA peer addrs verify DONE", workerName);
        (*addrReady) += 1;  // increase again to indicate peer inserted
        break;
      }
      case SEND_ONE:
      case SEND_BATCH:
      case RECV_ONE:
      case RECV_BATCH: {
        // do EFA jobs
        std::vector<std::pair<void*, size_t>> dataLoc;
        workerConvertMsg(_msg, dataLoc);
        // this is a synchronize fun
        efaSendRecv(efa_ep, _msg, dataLoc, cntr);
        break;
      }
      default:
        break;
    }
  }
  spdlog::debug("{:s} :: Exit event loop", workerName);
};

void workerConvertMsg(TransTask& msg,
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
    ptrs.push_back(std::make_pair(_ptr, _size));
  }
  spdlog::debug("workerConvertMsg cost: {:f} s", trans::time_now() - start);
}

void fi_tsend_or_trecv(TaskType& mType,
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
                 TransTask& msg,
                 std::vector<std::pair<void*, size_t>>& dataLoc,
                 std::atomic<size_t>* cntr) {
  //
  size_t slice_threshold = 512 * 1024;  // 1MB
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
    double _ts = trans::time_now();
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
    spdlog::debug("{:s} :: data block {:d} cost {:f} s", efa.nickname, i,
                  trans::time_now() - _ts);
  }
}
}  // namespace trans