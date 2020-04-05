#ifndef STORE_H
#define STORE_H

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "shm_common.h"
#include "sock_cli_serv.h"

namespace pipeps {
namespace store {

// from ps-lite
template <typename T>
class ThdSafeQueue {
 public:
  ThreadsafeQueue() {}
  ~ThreadsafeQueue() {}

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

inline void check_err(bool cond, std::string msg) {
  if (cond)
    std::cerr << msg;
};

enum InstrType { PUSH, PULL };
class Instr {
 public:
  int commIdx;  // idx of communicator
  InstrType type;
  std::string key;  // model name or partition id
  int nBatch;       // num of parameter batches for

  // useful when Pushing parameters not previous stored in
  std::vector<size_t> bufs;
  Instr() {
    // set it for sentinel check
    // valid idx must greater than 0;
    commIdx = -1;
  }
};

// communicator agent manage multiple communicators
class CommAgent {
 public:
  std::string name;
  ParamStore* store;
  int nComm;
  // params for creating communicator
  int nw;
  std::string data_buf_name;
  size_t data_buf_size;

  std::vector<void*> commInstrPtrs;
  std::vector<sem_t*> commInstrMtxs;
  std::vector<void*> commCntrPtrs;
  std::vector<sem_t*> commCntrMtxs;

  // thread management
  std::vector<std::thread> commThds;
  std::vector<std::thread> commWorkerThds;
  // communicator mangement
  std::vector<trans::shm::SHMCommunicator*> commPtrs;
  CommAgent(int nw,
            std::string agentName,
            std::string data_buf_name,
            size_t data_buf_size,
            ParamStore* s);
  // create communicator and setup peer addrs
  // return the idx of new created communicator
  int getCommunicator(char* peerAddrs, char* commAddrs);

  // recv data through EFA
  // put recv instr in comms[commIdx]'s shm
  void EFARecv(Instr& ins);

  // put send instr in comms[commIdx]'s shm
  void EFASend(Instr& ins);

  void _setEFAInstr(Instr& ins);
};

class ParamStore {
 public:
  std::string storeName;
  std::string port;
  std::atomic<bool> _exit{false};
  std::unordered_map<std::string, std::vector<std::pair<size_t, size_t>>>
      memStore;
  std::atomic<size_t> curOffset{
      0};  // the current offset; will be changed by getBuf operation
  ThdSafeQueue<Instr> taskq;
  CommAgent* cAgent;

  ParamStore(std::string name, std::string port, size_t mem_size, int commNw);
  ~ParamStore();
  // get offsets based on the keyname in ins
  void getBufs(Instr& ins, std::vector<std::pair<size_t, size_t>>& bufs);
  // wrapper logics to start threads, initialization environments;
  void run();
};

class StoreCli {
 public:
  std::string name, dstIP, dstPort, nameOfCache;
  int nw;
  size_t cacheSize;

  std::vector<std::thread> wThds;
  std::thread cThd;
  trans::SockCli sCli;

  // set instr and get status from comm
  void* shmCommInst;
  void* shmCommCntr;
  sem_t* semCommInst;
  sem_t* semCommCntr;

  StoreCli(std::string cliName,
           std::string servIP,
           std::string servPort,
           std::string nameOfCache,
           size_t sizeOfCache,
           int nw);
  // internal init function
  void _init();

  //
  void _open_shm_sem(std::string& commName);

  int getCommCntr();

  // push params
  void push(std::string& key, std::vector<std::pair<size_t, size_t>>& dataLoc);

  void pull(std::string& key, std::vector<std::pair<size_t, size_t>>& dataLoc);

  void _setEFAInstr(int ops, std::vector<std::pair<size_t, size_t>>& dataLoc);
};

StoreCli::StoreCli(std::string cliName,
                   std::string servIP,
                   std::string servPort,
                   std::string nameOfCache,
                   size_t sizeOfCache,
                   int nw) {
  this->name = cliName;
  this->dstIP = servIP;
  this->dstPort = servPort;
  this->nameOfCache = nameOfCache;
  cacheSize = sizeOfCache;
  this->nw = nw;  // num of workers for communicator
}

void StoreCli::_init() {
  // create communicator
  std::string commName = name + "-storecli-comm";
  trans::shm::SHMCommunicator* comm =
      new trans::shm::SHMCommunicator(nw, commName, nameOfCache, cacheSize);
  // create workers of communicators
  for (int i = 0; i < nw; i++) {
    std::thread wt(workerThd, commName, nw, i, nameOfCache, cacheSize);
    wt.detach();
    wThds.push_back(std::move(wt));
  }
  // wait for workers ready to get EFA address
  while (!comm->local_efa_addrs_ready()) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  // EFA addrs exchange
  size_t addrs_size = nw * trans::shm::EFA_ADDR_SIZE;
  char* localAddrs = new char[addrs_size];
  char* peerAddrs = new char[addrs_size];
  comm->get_local_efa_addrs(localAddrs);

  sCli = trans::SockCli(dstIP, dstPort);
  sCli._send(localAddrs, addrs_size);
  sCli._recv(peerAddrs, addrs_size);
  comm->set_local_peer_addrs(peerAddrs);

  std::thread _ct(commThd, comm);
  _ct.detach();
  cThd = std::move(_ct);

  // init communicator memory
  _open_shm_sem(commName);
}

void StoreCli::_open_shm_sem(std::string& commName) {
  std::string _instr_shm_name = commName + "-comm-instr-mem";
  std::string _cntr_shm_name = commName + "-comm-cntr-mem";
  int instr_fd = shm_open(_instr_shm_name.c_str(), O_RDWR, 0666);
  int cntr_fd = shm_open(_cntr_shm_name.c_str(), O_RDWR, 0666);

  this->shmCommInst = mmap(0, trans::shm::INSTR_SIZE, PROT_READ | PROT_WRITE,
                           MAP_SHARED, instr_fd, 0);
  this->shmCommCntr = mmap(0, trans::shm::CNTR_SIZE, PROT_READ | PROT_WRITE,
                           MAP_SHARED, cntr_fd, 0);
  std::string _sem_comm_instr("/" + commName + "-comm-instr-mtx");
  std::string _sem_comm_cntr("/" + commName + "-comm-cntr-mtx");

  this->semCommInst = sem_open(_sem_comm_instr.c_str(), 0);
  this->semCommCntr = sem_open(_sem_comm_cntr.c_str(), 0);
}

void StoreCli::_setEFAInstr(int ops,
                            std::vector<std::pair<size_t, size_t>>& dataLoc) {
  trans::shm::shm_lock(semCommInst, "StoreCli::_setEFAInstr: lock err");
  *(int*)((char*)shmCommInst + 8) = ops;
  *(int*)((char*)shmCommInst + 12) = dataLoc.size();
  char* _batch_data_s = (char*)shmCommInst + 16;
  for (int i = 0; i < dataLoc.size(); i++) {
    *(size_t*)(_batch_data_s + i * 16) = dataLoc[i].first;
    *(size_t*)(_batch_data_s + i * 16 + 8) = dataLoc[i].second;
  }
  *(double*)shmCommInst = trans::time_now();
  trans::shm::shm_unlock(semCommInst, "StoreCli::_setEFAInstr: unlock err");
}

void StoreCli::push(std::string& key,
                    std::vector<std::pair<size_t, size_t>>& srcDataLoc) {
  // =============== send ctl msg via TCP =================
  // send instr type
  char type[4];
  *(int*)type = 0;
  sCli._send(type, 4);

  // send key length
  char keylen[4];
  *(int*)keylen = key.size();
  sCli._send(keylen, 4);

  // send key
  auto keybuf = std::make_unique<char>(key.size());
  memcpy(keybuf.get(), key.c_str(), key.size());
  sCli._send(keybuf.get(), key.size());

  // send nBatch
  char nb[4];
  *(int*)nb = srcDataLoc.size();
  sCli._send(nb, 4);

  // send trunk sizes of data
  for (int i = 0; i < srcDataLoc.size(); i++) {
    char _bs[8];
    *(size_t*)_bs = srcDataLoc[i].second;
    sCli._send(_bs, 8);
  }
  // =============== end ctl msg via TCP =================

  // =============== Info local EFA
  _setEFAInstr(trans::shm::reverse_map(trans::shm::SEND_BATCH), srcDataLoc);
  // =============== End EFA instruction
}

void StoreCli::pull(std::string& key,
                    std::vector<std::pair<size_t, size_t>>& dataLoc) {
  // =============== send ctl msg via TCP =================
  // send instr type
  char type[4];
  *(int*)type = 0;
  sCli._send(type, 4);

  // send key length
  char keylen[4];
  *(int*)keylen = key.size();
  sCli._send(keylen, 4);

  // send key
  auto keybuf = std::make_unique<char>(key.size());
  memcpy(keybuf.get(), key.c_str(), key.size());
  sCli._send(keybuf.get(), key.size());

  // send nBatch = 0
  char nb[4];
  *(int*)nb = 0;
  sCli._send(nb, 4);
  // =============== end ctl msg via TCP =================

  // =============== Info local EFA
  _setEFAInstr(trans::shm::reverse_map(trans::shm::RECV_BATCH), dataLoc);
  // =============== End EFA instruction
}

int StoreCli::getCommCntr() {
  trans::shm::shm_lock(semCommCntr, "StoreCli::getCommCntr: lock err");
  int _c = *(int*)shmCommCntr;
  trans::shm::shm_unlock(semCommCntr, "StoreCli::getCommCntr: unlock err");
  return _c;
}

void cliConnHandlerThd(ParamStore* store, int cli) {
  // read the remote efa address from remote
  // assume addrs takes 64 bytes;
  size_t addr_size = store->cAgent->nw * 64;
  char* peer_addrs = new char[addr_size];
  char* local_addrs = new char[addr_size];

  // received from remote
  size_t ret = read(cli, peer_addrs, addr_size);
  check_err((ret != addr_size), "socket read size not match\n");
  int commIdx = store->cAgent->getCommunicator(peer_addrs, local_addrs);
  // send to remote
  ret = send(cli, local_addrs, addr_size, 0);
  check_err((ret != addr_size), "socket send err\n");

  // handle other instructions
  while (!store->_exit) {
    Instr ins;
    ins.commIdx = commIdx;
    // read the type: 0 == PUSH; > 0 PULL
    char type[4];
    read(cli, type, 4);
    ins.type = *(int*)type > 0 ? PULL : PUSH;
    // read len for key
    char keylen[4];
    read(cli, keylen, 4);
    // read key
    size_t _len = *(int*)keylen;
    char* key = new char[_len];
    read(cli, key, _len);
    ins.key = std::string(key);
    delete[] key;
    // read nbatch
    char nb[4];
    read(cli, nb, 4);
    ins.nBatch = *(int*)nb;
    for (int i = 0; i < ins.nBatch; i++) {
      // if nb is greater than 0
      char s[8];
      read(cli, s, 8);
      ins.bufs.push_back(*(size_t*)s);
    }

    store->taskq.push(ins);
  }
};

// this thread can be removed
// this thread is intend to reduce the computation while receiving instructions
void instrHandlerThd(ParamStore* store) {
  while (!store->_exit) {
    Instr* ins = new Instr();
    store->taskq.pop(ins);
    if (ins->type == PUSH) {
      store->cAgent->EFARecv(*ins);
    } else if (ins->type == PULL) {
      store->cAgent->EFASend(*ins);
    }

    delete ins;
  }
};

void sockServThd(ParamStore* store, std::string& port) {
  // always listen for new connections
  trans::SockServ serv(port);
  std::vector<std::thread> handles;
  while (true && !store->_exit) {
    int sockfd = serv._listen();  // get new connection
    std::thread _t(cliConnHandlerThd, store, sockfd);
    _t.detach();
    handles.push_back(std::move(_t));
  }
  for (int i = 0; i < handles.size(); i++) {
    if (handles[i].joinable())
      handles[i].join();
  }
};

// run communicator inside it
void commThd(trans::shm::SHMCommunicator* comm) {
  comm->run();
};
// run worker inside
void workerThd(std::string& comm_name,
               int& nw,
               int& rank,
               std::string& data_buf_name,
               size_t& data_buf_size) {
  trans::shm::SHMWorker w(comm_name, nw, rank, data_buf_name, data_buf_size);
  w.run();
};

// Arg nw: number of workers for each communicator
CommAgent::CommAgent(int nw,
                     std::string agentName,
                     std::string data_buf_name,
                     size_t data_buf_size,
                     ParamStore* s) {
  this->nw = nw;
  this->nComm = 0;
  this->data_buf_name = data_buf_name;
  this->data_buf_size = data_buf_size;
  this->store = s;
  this->name = agentName;
};

int CommAgent::getCommunicator(char* peerAddrs, char* commAddrs) {
  int idx = nComm;
  // prepare hyper parameters
  std::string commName = this->name + "-comm-" + std::to_string(idx);
  // create communicator
  trans::shm::SHMCommunicator* comm = new trans::shm::SHMCommunicator(
      nw, commName, data_buf_name, data_buf_size);

  // create workers
  for (int i = 0; i < nw; i++) {
    std::thread wt(workerThd, commName, nw, i, data_buf_name, data_buf_size);
    wt.detach();
    commWorkerThds.push_back(std::move(wt));
  }

  // wait for local workers ready to push their EFA addrs
  while (!comm->local_efa_addrs_ready()) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  comm->set_local_peer_addrs(peerAddrs);
  comm->get_local_efa_addrs(commAddrs);

  this->commPtrs.push_back(comm);
  std::thread ct(commThd, comm);
  ct.detach();
  this->commThds.push_back(std::move(ct));
  // done: get comm Instr, cntr shm
  std::string _instr_shm_name = commName + "-comm-instr-mem";
  std::string _cntr_shm_name = commName + "-comm-cntr-mem";
  int instr_fd = shm_open(_instr_shm_name.c_str(), O_RDWR, 0666);
  int cntr_fd = shm_open(_cntr_shm_name.c_str(), O_RDWR, 0666);

  void* _instr_ptr = mmap(0, trans::shm::INSTR_SIZE, PROT_READ | PROT_WRITE,
                          MAP_SHARED, instr_fd, 0);
  void* _cntr_ptr = mmap(0, trans::shm::CNTR_SIZE, PROT_READ | PROT_WRITE,
                         MAP_SHARED, cntr_fd, 0);
  commInstrPtrs.push_back(_instr_ptr);
  commCntrPtrs.push_back(_cntr_ptr);

  std::string _sem_comm_instr("/" + commName + "-comm-instr-mtx");
  std::string _sem_comm_cntr("/" + commName + "-comm-cntr-mtx");

  sem_t* _instr_mtx = sem_open(_sem_comm_instr.c_str(), 0);
  sem_t* _cntr_mtx = sem_open(_sem_comm_cntr.c_str(), 0);
  commInstrMtxs.push_back(_instr_mtx);
  commCntrMtxs.push_back(_cntr_mtx);

  nComm++;
  return idx;
}

void CommAgent::_setEFAInstr(Instr& ins) {
  int ops;  // operation code
  if (ins.type == PUSH) {
    ops = trans::shm::reverse_map(trans::shm::RECV_BATCH);
  } else {
    ops = trans::shm::reverse_map(trans::shm::SEND_BATCH);
  }
  // get buf
  std::vector<std::pair<size_t, size_t>> bufs;
  this->store->getBufs(ins, bufs);
  // write instruction to corresponding communicator mem
  trans::shm::shm_lock(commInstrMtxs[ins.commIdx],
                       "_setEFAInstr put inst: lock err");
  void* _instr_ptr = commInstrPtrs[ins.commIdx];
  *(int*)((char*)_instr_ptr + 8) = ops;
  *(int*)((char*)_instr_ptr + 12) = ins.nBatch;
  char* _batch_data_s = (char*)_instr_ptr + 16;
  for (int i = 0; i < ins.nBatch; i++) {
    *(size_t*)(_batch_data_s + i * 16) = bufs[i].first;
    *(size_t*)(_batch_data_s + i * 16 + 8) = bufs[i].second;
  }
  *(double*)_instr_ptr = trans::time_now();
  trans::shm::shm_unlock(commInstrMtxs[ins.commIdx],
                         "_setEFAInstr put inst: unlock err");
}

void CommAgent::EFARecv(Instr& ins) {
  _setEFAInstr(ins);
}

void CommAgent::EFASend(Instr& ins) {
  _setEFAInstr(ins);
}

ParamStore::ParamStore(std::string name, std::string port, size_t mem_size, int commNw) {
  this->storeName = name;
  this->port = port;

  // hyper parameter
  std::string data_buf_name = "mem-store-serv";

  cAgent =
      new CommAgent(commNw, name + "-cAgent", data_buf_name, mem_size, this);
}

void ParamStore::getBufs(Instr& ins,
                         std::vector<std::pair<size_t, size_t>>& bufs) {
  // if the key is not exist; need to move the cur pointer
  auto _it = memStore.find(ins.key);
  if (_it != memStore.end()) {
    bufs = _it->second;
  } else {
    // key is not exist
    for (size_t& bs : ins.bufs) {
      bufs.push_back(std::pair<size_t, size_t>(curOffset, bs));
      curOffset += bs;
    }
  }
};

void ParamStore::run() {
  std::thread sockServ(sockServThd, this, port);
  std::thread instrHandle(instrHandlerThd, this);

  sockServ.join();
  instrHandle.join();
}

};  // namespace store
};  // namespace pipeps
#endif
