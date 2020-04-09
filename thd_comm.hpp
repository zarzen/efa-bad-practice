#ifndef THD_COMM_H
#define THD_COMM_H

#include "spdlog/spdlog.h"
#include "util.h"
#include <iostream>
#include <memory>

namespace pipeps {

namespace trans {

enum MsgType { EFA_ADDR_INFO, SEND_ONE, SEND_BATCH, RECV_ONE, RECV_BATCH };

class TransMsg {
public:
  MsgType t;
  char *data;
  size_t len;
  TransMsg(MsgType _t, size_t data_len) {
    t = _t;
    data = new char[data_len];
    len = data_len;
  };

  TransMsg(const TransMsg &obj) {
    this->data = new char[obj.len];
    this->len = obj.len;
    this->t = obj.t;
    memcpy(this->data, obj.data, len);
  }

  TransMsg(TransMsg &&obj) {
    t = obj.t;
    data = obj.data;
    len = obj.len;

    obj.data = nullptr;
  };

  ~TransMsg() { delete[] data; }
};

class ThdCommunicator {
public:
  const static int efaAddrSize{64};
  // for workers
  std::vector<std::thread> workerThds;
  std::vector<ThdSafeQueue<TransMsg> *> workerTaskQs;
  std::vector<std::atomic<size_t> *> workerCntrs;
  std::atomic<int> addrReadyC{0};
  // potential usage for worker to report msg to communicator
  // std::vector<ThdSafeQueue<TransMsg>*> workerMsgQs;

  // communicator vars
  std::string dstIP;
  std::string dstPort;
  bool ready{false}; // peer EFA addrs is not ready at first
  char* efaAddrs;
  std::atomic<bool> cntr;

  ThdCommunicator(std::string listenPort, std::string dstIP,
                  std::string dstPort, int nw);
  ~ThdCommunicator();

  void asend(char *buf, size_t len);
  void arecv(char *buf, size_t len);

  // will be invoked at the first time asend/arecv is called
  // it is a block function will retry several times
  // set ready = true;
  bool getPeerAddrs();

  // always listening for others to query
  static void socketListener(std::string port, char *addrsBuf, size_t addrsLen);

  static void workerThdFun(std::string workerName, int rank,
                           ThdSafeQueue<TransMsg>* taskq, std::atomic<size_t>* cntr,
                            char *efaAddrs);
};

ThdCommunicator::ThdCommunicator(std::string listenPort, std::string dstIP,
                                 std::string dstPort, int nw) {
                                   efaAddrs = new char[nw * efaAddrSize];
                                   // start workers
                                   // make sure EFA addr ready via addrReadyC
                                   // launch socket listener
                                 };

// send msg to workers to stop
// set the communicator status to stop for socket thread to quit
// wait for worker threads to join
// wait for socket thread to stop
// clean resource
ThdCommunicator::~ThdCommunicator(){
  //

  // 
  delete[] efaAddrs;
};
}; // namespace trans
}; // namespace pipeps

#endif