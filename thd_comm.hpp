#ifndef THD_COMM_H
#define THD_COMM_H

#include <rdma/fi_tagged.h>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include "efa_ep.h"
#include "sock_cli_serv.h"
#include "spdlog/spdlog.h"
#include "util.h"

namespace trans {

enum MsgType {
  INS_EFA_ADDR_INFO,
  SEND_ONE,
  SEND_BATCH,
  RECV_ONE,
  RECV_BATCH,
  SHUTDOWN
};

inline std::string MsgTyepStr(MsgType& t) {
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

  TransMsg():data(nullptr){};

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

  ~TransMsg() { 
    if (data != nullptr){
      delete[] data;
    }
  }
};

void efaWorkerThdFun(std::string workerName,
                     int rank,
                     ThdSafeQueue<TransMsg>* taskq,
                     std::atomic<size_t>* cntr,
                     char* efaAddrs,
                     std::atomic<int>* addrReady);

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
  std::atomic<size_t> cntr{0};
  std::atomic<bool> exit{false};
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


};  // namespace trans

#endif