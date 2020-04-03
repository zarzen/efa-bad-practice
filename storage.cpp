#include <atomic>
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
};

class ParamStore {
 public:
  std::string storeName;
  std::string port;
  std::atomic<bool> _exit{false};
  std::unordered_map<std::string, std::vector<std::pair<size_t, size_t>>>
      memStore;
  size_t curOffset;  // the current offset; will be changed by getBuf operation
  ThdSafeQueue<Instr> taskq;
  CommAgent* cAgent;

  ParamStore(std::string name, std::string& port);
  ~ParamStore();
  // get offsets based on the keyname in ins
  void getBufs(Instr& ins, std::vector<std::pair<size_t, size_t>>& bufs);
  // wrapper logics to start threads, initialization environments;
  void run();
};

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
      // if nb is 0
      char* s[8];
      read(cli, nb, 8);
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

  nComm++;
  return idx;
}

void CommAgent::EFARecv(Instr& ins) {
  // 
}

ParamStore::ParamStore(std::string name, std::string& port) {
  this->storeName = name;
  this->port = port;

  // hyper parameters
  int commNw = 4;
  std::string data_buf_name = "mem-store-serv";
  size_t data_buf_size = 10 * 1024 * 1024 * 1024;  // 10GB

  cAgent = new CommAgent(commNw, name + "-cAgent", data_buf_name, data_buf_size,
                         this);
}

void ParamStore::getBufs(Instr& ins, std::vector<std::pair<size_t, size_t>>& bufs) {
  // TODO 
};

void ParamStore::run() {
  std::thread sockServ(sockServThd, this, port);
  std::thread instrHandle(instrHandlerThd, this);

  sockServ.join();
  instrHandle.join();
}

};  // namespace store
};  // namespace pipeps