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
  EFA_ADDR_INFO,
  SEND_ONE,
  SEND_BATCH,
  RECV_ONE,
  RECV_BATCH,
  SHUTDOWN
};

class TransMsg {
 public:
  MsgType t;
  char* data;
  size_t len;

  TransMsg(MsgType _t, size_t data_len) {
    t = _t;
    data = new char[data_len];
    len = data_len;
  };

  TransMsg(){};

  TransMsg(const TransMsg& obj) {
    this->data = new char[obj.len];
    this->len = obj.len;
    this->t = obj.t;
    memcpy(this->data, obj.data, len);
  }

  // move constructor
  TransMsg(TransMsg&& obj) {
    t = obj.t;
    data = obj.data;
    len = obj.len;

    obj.data = nullptr;
  };

  // move assignment operator
  TransMsg& operator=(TransMsg&& other) {
    if (this != &other) {
      // Free the existing resource.
      delete[] data;

      // Copy the data pointer and its length from the
      // source object.
      data = other.data;
      len = other.len;

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

  void asend(char* buf, size_t len);
  void arecv(char* buf, size_t len);

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

    spdlog::debug("{:s} started worker {:s} in thread", this->name, _wn);
  }
  // make sure EFA addr ready via addrReadyC
  while (*addrReadyC < nw) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  // launch socket listener
  sockThdPtr = new std::thread(socketListenerThdFun, this, listenPort, efaAddrs,
                               nw * efaAddrSize);
  spdlog::debug("EFA address server started");
  // launch cntr
  cntrThdPtr = new std::thread(cntrMonitorThdFun, this);
  spdlog::debug("Communicator cntr update thread started");
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

  // 2: =======
  for (int i = 0; i < nw; i++) {
    TransMsg _stop_msg(SHUTDOWN, 1);
    workerTaskQs[i]->push(std::move(_stop_msg));
    workerThds[i].join();
  }
  // 2: =======

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
  }
};

};  // namespace pipeps

#endif