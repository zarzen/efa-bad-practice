#ifndef THD_COMM_H
#define THD_COMM_H

#include <iostream>
#include <memory>
#include "sock_cli_serv.h"
#include "spdlog/spdlog.h"
#include "thd_efa_worker.hpp"
#include "util.h"

namespace pipeps {

enum MsgType {
  INS_EFA_ADDR_INFO,
  SEND_ONE,
  SEND_BATCH,
  RECV_ONE,
  RECV_BATCH,
  SHUTDOWN
};

inline const char* MsgTyepStr(MsgType& t) {
  switch (t) {
    case INS_EFA_ADDR_INFO:
      return "INS_EFA_ADDR_INFO";
    case SEND_ONE:
      return "SEND_ONE";
    case SEND_BATCH:
      return "SEND_BATCH";
    case RECV_ONE:
      return "RECV_ONE";
    case RECV_BATCH:
      return "RECV_BATCH";
    case SHUTDOWN:
      return "SHUTDOWN";
    default:
      spdlog::error("MsgTyepStr::error:: Unknow type");
      return "";
  }
};

class TransMsg {
 public:
  MsgType t;
  char* data;
  size_t len;
  double ts;

  TransMsg(MsgType _t, size_t data_len) {
    t = _t;
    data = new char[data_len];
    len = data_len;
    ts = trans::time_now();
  };

  TransMsg(){};

  TransMsg(const TransMsg& obj) {
    this->data = new char[obj.len];
    this->len = obj.len;
    this->t = obj.t;
    this->ts = obj.ts;
    memcpy(this->data, obj.data, len);
  }

  // move constructor
  TransMsg(TransMsg&& obj) {
    t = obj.t;
    data = obj.data;
    len = obj.len;
    ts = obj.ts;
    obj.data = nullptr;
  };

  // move assignment operator
  TransMsg& operator=(TransMsg&& other) {
    if (this != &other) {
      // Free the existing resource.
      delete[] data;

      // Copy the data pointer and its length from the
      // source object.
      t = other.t;
      data = other.data;
      len = other.len;
      ts = other.ts;
      // Release the data pointer from the source object so that
      // the destructor does not free the memory multiple times.
      other.data = nullptr;
      other.len = 0;
    }
    return *this;
  }

  ~TransMsg() { delete[] data; }
};

class ThdCommunicator {
 public:
  const static int efaAddrSize{64};
  // for workers
  std::vector<std::thread> workerThds;
  std::vector<ThdSafeQueue<TransMsg>*> workerTaskQs;
  std::vector<std::atomic<size_t>*> workerCntrs;
  std::atomic<int>* addrReadyC;
  // potential usage for worker to report msg to communicator
  // std::vector<ThdSafeQueue<TransMsg>*> workerMsgQs;

  // communicator vars
  std::string name;
  int nw;
  std::string listenPort;
  std::string dstIP;
  std::string dstPort;
  bool _ready{false};  // peer EFA addrs is not ready at first
  char* efaAddrs;
  std::atomic<size_t> cntr;
  std::atomic<bool> exit;
  std::thread* sockThdPtr;
  std::thread* cntrThdPtr;

  ThdCommunicator(std::string listenPort,
                  std::string dstIP,
                  std::string dstPort,
                  int nw);
  ~ThdCommunicator();

  void asendBatch(std::vector<std::pair<char*, size_t>> dataLoc);
  void arecvBatch(std::vector<std::pair<char*, size_t>> dataLoc);

  void _sendTask(MsgType t, std::vector<std::pair<char*, size_t>>& dataLoc);

  // will be invoked at the first time asend/arecv is called
  // it is a block function will retry several times
  // set ready = true;
  bool getPeerAddrs();

  // always listening for others to query
  static void socketListenerThdFun(ThdCommunicator* comm,
                                   std::string port,
                                   char* addrsBuf,
                                   size_t addrsLen);

  static void cntrMonitorThdFun(ThdCommunicator* comm);
};

ThdCommunicator::ThdCommunicator(std::string listenPort,
                                 std::string dstIP,
                                 std::string dstPort,
                                 int nw) {
  this->nw = nw;
  this->listenPort = listenPort;
  this->dstIP = dstIP;
  this->dstPort = dstPort;
  this->name = listenPort + "comm";

  efaAddrs = new char[nw * efaAddrSize];
  addrReadyC = new std::atomic<int>(0);
  // start workers
  for (int i = 0; i < nw; i++) {
    std::string _wn = this->name + "-worker-" + std::to_string(i);
    ThdSafeQueue<TransMsg>* _wtq = new ThdSafeQueue<TransMsg>();
    std::atomic<size_t>* _wc = new std::atomic<size_t>();
    std::thread _wt(efaWorkerThdFun, _wn, i, _wtq, _wc, efaAddrs, addrReadyC);
    workerThds.push_back(std::move(_wt));
    workerCntrs.push_back(_wc);
    workerTaskQs.push_back(_wtq);
    spdlog::info("{:s} started worker {:s} in thread", this->name, _wn);
  }
  // make sure EFA addr ready via addrReadyC
  while (*addrReadyC < nw) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  spdlog::info("workers' EFA addresses are ready");
  // launch socket listener
  sockThdPtr = new std::thread(socketListenerThdFun, this, listenPort, efaAddrs,
                               nw * efaAddrSize);
  spdlog::info("EFA address server started");
  // launch cntr
  cntrThdPtr = new std::thread(cntrMonitorThdFun, this);
  spdlog::info("Communicator cntr update thread started");
};

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
  std::unique_ptr<char> _t(new char[nw * efaAddrSize]);
  cli._recv(_t.get(), nw * efaAddrSize);
  sockThdPtr->join();
  // 1: ======= end stopping socket Listener

  // 2: ======= wait for workers to complete
  for (int i = 0; i < nw; i++) {
    TransMsg _stop_msg(SHUTDOWN, 1);
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
  trans::SockServ serv(port);
  while (!comm->exit) {
    // quick hack to stop this thread:
    // set the comm->exit = true;
    // then connect to this sock and recv
    int cli_fd = serv._listen();  // here will block;
    size_t ret = send(cli_fd, addrsBuf, addrsLen, 0);
    if (ret == -1) {
      spdlog::critical("Err, while sending out EFA addrs");
    }
  }
};

void ThdCommunicator::cntrMonitorThdFun(ThdCommunicator* comm) {
  while (!comm->exit) {
    bool inc = true;
    // this is bounded because of the task splitting
    // check whether all worker cntrs greater than current communicator cntr
    for (auto c_ptr : comm->workerCntrs) {
      if (*c_ptr <= comm->cntr) {
        inc = false;
      }
    }
    if (inc) {
      comm->cntr += 1;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
};

void ThdCommunicator::_sendTask(
    MsgType t,
    std::vector<std::pair<char*, size_t>>& dataLoc) {
  for (int wi = 0; wi < this->nw; wi++) {
    TransMsg msg(t, 4 + dataLoc.size() * 8);
    // fill msg content
    *(int*)msg.data = dataLoc.size();
    spdlog::debug("assemble TransMsg of worker {:d}", wi);
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
    spdlog::debug("TransMsg enqueued of worker {:d}", wi);
  }
}

void ThdCommunicator::asendBatch(
    std::vector<std::pair<char*, size_t>> dataLoc) {
  while (!_ready) {
    getPeerAddrs();
  }

  this->_sendTask(SEND_BATCH, dataLoc);
}

void ThdCommunicator::arecvBatch(
    std::vector<std::pair<char*, size_t>> dataLoc) {
  while (!_ready) {
    getPeerAddrs();
  }
  _sendTask(RECV_BATCH, dataLoc);
}

bool ThdCommunicator::getPeerAddrs() {
  try {
    trans::SockCli sCli(dstIP, dstPort);
    char* peerAddrs = new char[nw * efaAddrSize];
    sCli._recv(peerAddrs, nw * efaAddrSize);
    for (int wi = 0; wi < nw; wi++) {
      char* _addr = peerAddrs + wi * efaAddrSize;
      TransMsg m(INS_EFA_ADDR_INFO, efaAddrSize);
      memcpy(m.data, _addr, efaAddrSize);
      workerTaskQs[wi]->push(std::move(m));
      spdlog::debug("INS_EFA_ADDR_INFO enqueued into task q of worker {:d}",
                    wi);
    }
    // wait for insertion complete
    // insert EFA addrs must be the first job to complete
    while (this->cntr != 1) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    spdlog::debug("worker all inserted peer Addrs");
    _ready = true;
  } catch (...) {
    spdlog::error("Error occur while getPeerAddrs; should retry");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

};  // namespace pipeps

#endif