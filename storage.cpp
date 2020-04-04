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

  void _setEFAInstr(Instr& ins);
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
  // TODO: get comm Instr, cntr shm
  std::string _instr_shm_name = commName + "-comm-instr-mem";
  std::string _cntr_shm_name = commName + "-comm-cntr-mem";
  int instr_fd = shm_open(_instr_shm_name.c_str(), O_RDWR, 0666);
  int cntr_fd = shm_open(_cntr_shm_name.c_str(), O_RDWR, 0666);

  void* _instr_ptr = 
      mmap(0, trans::shm::INSTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, instr_fd, 0);
  void* _cntr_ptr = 
      mmap(0, trans::shm::CNTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);
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

void CommAgent::_setEFAInstr(Instr& ins){
  int ops; // operation code
  if (ins.type == PUSH) {
    ops = trans::shm::reverse_map(trans::shm::RECV_BATCH);
  } else {
    ops = trans::shm::reverse_map(trans::shm::SEND_BATCH);
  }
  // get buf
  std::vector<std::pair<size_t, size_t>> bufs;
  this->store->getBufs(ins, bufs);
  // write instruction to corresponding communicator mem
  trans::shm::shm_lock(commInstrMtxs[ins.commIdx], "_setEFAInstr put inst: lock err");
  void* _instr_ptr = commInstrPtrs[ins.commIdx];
  *(int*)((char*)_instr_ptr + 8) = ops;
  *(int*)((char*)_instr_ptr + 12) = ins.nBatch;
  char* _batch_data_s = (char*)_instr_ptr + 16;
  for (int i = 0; i < ins.nBatch; i++) {
    *(size_t*)(_batch_data_s + i * 16) = bufs[i].first;
    *(size_t*)(_batch_data_s + i * 16 + 8) = bufs[i].second;
  }
  *(double*)_instr_ptr = trans::time_now();
  trans::shm::shm_unlock(commInstrMtxs[ins.commIdx], "_setEFAInstr put inst: unlock err");
}

void CommAgent::EFARecv(Instr& ins) {
  _setEFAInstr(ins);
}

void CommAgent::EFASend(Instr& ins) {
  _setEFAInstr(ins);
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
  // if the key is not exist; need to move the cur pointer
  auto _it = memStore.find(ins.key);
  if (_it != memStore.end()){
    bufs = _it->second;
  } else {
    // key is not exist
    for (size_t& bs: ins.bufs){
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