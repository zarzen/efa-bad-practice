#ifndef SHM_COMMON_H
#define SHM_COMMON_H

#include <fcntl.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h> /* For mode constants */
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "efa_ep.h"
#include "util.h"
#include <rdma/fi_tagged.h>

namespace trans {
namespace shm {
void check_err(bool cond, std::string msg) {
  if (cond) {
    std::cerr << msg;
  }
};

bool check_all_zero(char* buf, size_t size) {
  bool flg = true;
  for (int i = 0; i < size; i++) {
    if (*(buf + i) != 0) {
      flg = false;
    }
  }
  return flg;
};

const int INSTR_OFFSET = 12;
const int INSTR_SIZE = 10 * 1024;
const int INSTR_DATA_SIZE = INSTR_SIZE - INSTR_OFFSET;
const int EFA_ADDR_SIZE = 64;
const int CNTR_SIZE = 4;
const int STATUS_SIZE = 4;

const std::string SHM_SUFFIX_INSTR = "-instr-mem";
const std::string SHM_SUFFIX_CNTR = "-cntr-mem";
const std::string SHM_SUFFIX_EFA_ADDR = "-efa-addr-mem";
const std::string SHM_SUFFIX_W_STAT = "-worker-status-mem";
const std::string SHM_SUFFIX_DATA_BUF = "-data-buf-mem";

const std::string SEM_SUFFIX_INSTR = "-instr-mtx-";
const std::string SEM_SUFFIX_CNTR = "-cntr-mtx-";
const std::string SEM_SUFFIX_EFA_ADDR = "-efa-addr-mtx-";
const std::string SEM_SUFFIX_W_STAT = "-worker-status-mtx-";
const std::string SEM_SUFFIX_DATA_BUF = "-data-buf-mtx-";

void shm_lock(sem_t* s, std::string msg_if_err) {
  check_err(sem_wait(s) < 0, msg_if_err);
};

void shm_unlock(sem_t* s, std::string msg_if_err) {
  check_err(sem_post(s) < 0, msg_if_err);
};

void print_sem_mutex_val(sem_t* s) {
  int v;
  sem_getvalue(s, &v);
  std::cout << "sem_mutex val: " << v << "\n";
};

int sem_mutex_val(sem_t* s) {
  int v;
  sem_getvalue(s, &v);
  return v;
};

enum INSTR_T { ERR_INSTR, SET_EFA_ADDR, RECV_BATCH, SEND_BATCH };

class Instruction {
 public:
  INSTR_T type;
  double timestamp;
  char data[INSTR_DATA_SIZE] = {0};
  Instruction(){};
};

INSTR_T instr_map(int idx) {
  switch (idx) {
    case 1:
      return SET_EFA_ADDR;
    case 2:
      return RECV_BATCH;
    case 3:
      return SEND_BATCH;
    default:
      return ERR_INSTR;
  };
};

int reverse_map(INSTR_T t) {
  switch (t) {
    case SET_EFA_ADDR:
      return 1;
    case RECV_BATCH:
      return 2;
    case SEND_BATCH:
      return 3;
    default:
      return -1;
  };
};

class WorkerMemory {
 public:
  // 8 bytes for timestamp;
  // 4 bytes for instr-code; the remaining bytes for instr data
  int instr_size = INSTR_SIZE;
  int efa_addr_size = EFA_ADDR_SIZE;
  int cntr_size = CNTR_SIZE;             // 4 Bytes
  int status_size = STATUS_SIZE;         // indicate the status of worker
  unsigned long long int data_buf_size;  // input from user
  // based on rank to move the pointer of instr, efa_addr, cntr, status
  int rank;
  int nw;  // number of workers

  // shm identifiers
  std::string shm_instr;
  std::string shm_cntr;
  std::string shm_data_buf;
  std::string shm_efa_addr;
  std::string shm_w_status;

  // mutex names
  std::string sem_name_instr;
  std::string sem_name_cntr;
  std::string sem_name_data;
  std::string sem_name_efa_addr;
  std::string sem_name_w_status;

  // mutex pointers
  sem_t* sem_instr;
  sem_t* sem_cntr;
  sem_t* sem_data;
  sem_t* sem_efa_addr;
  sem_t* sem_w_status;

  // pointers
  void* instr_ptr;
  void* cntr_ptr;
  void* data_buf_ptr;
  void* efa_add_ptr;
  void* status_ptr;  // 1: idle; 2: working;

  /* rank start from 0
    nw: total workers
   */
  WorkerMemory(std::string prefix,
               int nw,
               int rank,
               std::string data_buf_name,
               size_t data_size) {
    data_buf_size = data_size;
    this->rank = rank;
    this->nw = nw;

    shm_instr = std::string(prefix + SHM_SUFFIX_INSTR);        // instruction
    shm_cntr = std::string(prefix + SHM_SUFFIX_CNTR);          // counter
    shm_efa_addr = std::string(prefix + SHM_SUFFIX_EFA_ADDR);  // local efa addr
    shm_w_status = std::string(prefix + SHM_SUFFIX_W_STAT);    // worker status
    shm_data_buf = data_buf_name;                              // data buf

    sem_name_instr =
        std::string("/" + prefix + SEM_SUFFIX_INSTR + std::to_string(rank));
    sem_name_cntr =
        std::string("/" + prefix + SEM_SUFFIX_CNTR + std::to_string(rank));
    sem_name_data =
        std::string("/" + prefix + SEM_SUFFIX_DATA_BUF + std::to_string(rank));
    sem_name_efa_addr =
        std::string("/" + prefix + SEM_SUFFIX_EFA_ADDR + std::to_string(rank));
    sem_name_w_status =
        std::string("/" + prefix + SEM_SUFFIX_W_STAT + std::to_string(rank));

    this->open_shm_sem();
  };

  void open_shm_sem() {
    // only open shm not create
    // creation is handled by controller side
    int instr_fd = shm_open(shm_instr.c_str(), O_RDWR, 0666);
    int cntr_fd = shm_open(shm_cntr.c_str(), O_RDWR, 0666);
    int data_buf_fd = shm_open(shm_data_buf.c_str(), O_RDWR, 0666);
    int efa_add_fd = shm_open(shm_efa_addr.c_str(), O_RDWR, 0666);
    int worker_status_fd = shm_open(shm_w_status.c_str(), O_RDWR, 0666);
    // map memory pointer
    instr_ptr = mmap(0, nw * INSTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                     instr_fd, 0);
    cntr_ptr =
        mmap(0, nw * CNTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);
    efa_add_ptr = mmap(0, nw * EFA_ADDR_SIZE, PROT_READ | PROT_WRITE,
                       MAP_SHARED, efa_add_fd, 0);
    status_ptr = mmap(0, nw * STATUS_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                      worker_status_fd, 0);
    data_buf_ptr = mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        data_buf_fd, 0);
    // move the pointer
    instr_ptr = (void*)((char*)instr_ptr + (this->rank * instr_size));
    cntr_ptr = (void*)((char*)cntr_ptr + this->rank * cntr_size);
    efa_add_ptr = (void*)((char*)efa_add_ptr + this->rank * efa_addr_size);
    status_ptr = (void*)((char*)status_ptr + this->rank * status_size);

    std::cout << "garbe at instr_ptr " << (char*)instr_ptr << "\n";
    // check memory all zeros
    std::cout << "instr memory all zeros: "
              << check_all_zero((char*)instr_ptr, instr_size) << "\n";

    // open mutexs
    // controller will create the semaphore
    // sem_instr = sem_open(sem_name_instr.c_str(), O_CREAT, S_IRUSR | S_IWUSR,
    // 1);
    sem_instr = sem_open(sem_name_instr.c_str(), 0);
    sem_cntr = sem_open(sem_name_cntr.c_str(), 0);
    sem_data = sem_open(sem_name_data.c_str(), 0);
    sem_efa_addr = sem_open(sem_name_efa_addr.c_str(), 0);
    sem_w_status = sem_open(sem_name_w_status.c_str(), 0);
  };

  ~WorkerMemory() {
    // un map
    munmap(instr_ptr, instr_size);
    munmap(cntr_ptr, cntr_size);
    munmap(data_buf_ptr, data_buf_size);
    munmap(efa_add_ptr, efa_addr_size);
    munmap(status_ptr, status_size);

    // un link
    shm_unlink(shm_instr.c_str());
    shm_unlink(shm_cntr.c_str());
    shm_unlink(shm_data_buf.c_str());
    shm_unlink(shm_efa_addr.c_str());
    shm_unlink(shm_w_status.c_str());

    // un link semaphore
    sem_unlink(sem_name_instr.c_str());
    sem_unlink(sem_name_cntr.c_str());
    sem_unlink(sem_name_data.c_str());
    sem_unlink(sem_name_efa_addr.c_str());
    sem_unlink(sem_name_w_status.c_str());
  };
};

class SHMWorker {
  std::string comm_name;
  int rank;
  EFAEndpoint* efa_ep;
  WorkerMemory* mem;

 public:
  SHMWorker(std::string comm_name,
            int nw,
            int rank,
            std::string data_buf_name,
            size_t shared_data_size) {
    efa_ep =
        new EFAEndpoint(this->comm_name + "-efa-ep-" + std::to_string(rank));
    mem =
        new WorkerMemory(comm_name, nw, rank, data_buf_name, shared_data_size);
    this->comm_name = comm_name;
    this->rank = rank;
    this->set_local_efa_addr(efa_ep);
    // init worker status
    *(int*)(mem->status_ptr) = 1;
  };

  Instruction* read_instr() {
    shm_lock(mem->sem_instr, "read_instr: sem_wait err");
    // read first 8 bytes for time stamp
    double ts = *((double*)mem->instr_ptr);
    Instruction* i = NULL;
    if (ts == 0) {
    } else {
      i = new Instruction();
      i->timestamp = ts;
      // start from 9th byte to 12th stand for instr
      INSTR_T i_type = instr_map(*((int*)((char*)mem->instr_ptr + 8)));
      i->type = i_type;
      memcpy(i->data, ((char*)mem->instr_ptr) + INSTR_OFFSET,
             mem->instr_size - INSTR_OFFSET);
      // wipe first 12 bytes to inidicate message already read
      std::fill_n((char*)mem->instr_ptr, INSTR_OFFSET, 0);
    }
    shm_unlock(mem->sem_instr, "read_instr: sem_post err");
    // mem->print_sem_mutex_val();
    return i;
  };

  void set_local_efa_addr(EFAEndpoint* efa) {
    char local_ep_addrs[64] = {0};
    char readable[64] = {0};
    efa->get_name(local_ep_addrs, mem->efa_addr_size);
    size_t len = mem->efa_addr_size;
    fi_av_straddr(efa->av, local_ep_addrs, readable, &len);
    std::cout << "Local ep addresses: \n" << readable << "\n";

    shm_lock(mem->sem_efa_addr, "set_local_efa_addr: sem_wait err");
    std::memcpy(mem->efa_add_ptr, local_ep_addrs, mem->efa_addr_size);
    shm_unlock(mem->sem_efa_addr, "set_local_efa_addr: sem_post err");
    print_sem_mutex_val(mem->sem_efa_addr);
  };

  void _wait_cq(fid_cq* cq, int count) {
    struct fi_cq_err_entry entry;
    int ret, completed = 0;
    double s = time_now();
    while (completed < count) {
      ret = fi_cq_read(cq, &entry, 1);
      if (ret == -FI_EAGAIN) {
        continue;
      }

      if (ret == -FI_EAVAIL) {
        ret = fi_cq_readerr(cq, &entry, 1);
        CHK_ERR("fi_cq_readerr", (ret != 1), ret);

        printf("Completion with error: %d\n", entry.err);
        char _err_buf[100];
        printf("!!! err: %s\n", fi_cq_strerror(cq, entry.prov_errno,
                                               entry.err_data, _err_buf, 100));
        printf("%s\n", _err_buf);
        // if (entry.err == FI_EADDRNOTAVAIL)
        // 	get_peer_addr(entry.err_data);
      }

      CHK_ERR("fi_cq_read ????", (ret < 0), ret);
      completed++;

      double cost_t = time_now() - s;
      std::cout << completed << " job cost : " << cost_t * 1e3 << " ms\n";
      s = time_now();  // update start time
    }
  };

  /* insert remote efa addr into address vector */
  void set_remote_efa_addr(EFAEndpoint* efa, Instruction* i) {
    // instruction data already copied into i->data
    fi_av_insert(efa->av, i->data, 1, &(efa->peer_addr), 0, NULL);

    efa->av_ready = true;

    char name_buf[mem->efa_addr_size];
    char readable[64] = {0};
    size_t len = 64;
    fi_av_lookup(efa->av, efa->peer_addr, name_buf, &len);
    len = 64;
    std::fill_n(readable, mem->efa_addr_size, 0);
    fi_av_straddr(efa->av, name_buf, readable, &len);
    std::cout << "verified inserted: " << readable << "\n";
  };

  void efa_send_recv_batch(EFAEndpoint* efa, Instruction* instr) {
    std::cout << "enter worker efa_send_recv_batch\n";
    int batch_n = *(int*)instr->data;  // 4 bytes for number of batches
    int* wait_sizes = new int[batch_n];
    size_t slice_threshold = 2 * 1024 * 1024;  // 4MB
    int task_seq = 0;
    // the rest of them: 8 bytes for offset; 4 bytes for size
    for (int i = 0; i < batch_n; i++) {
      unsigned long long int offset =
          *(unsigned long long int*)((instr->data) + 4 + 16 * i);
      size_t batch_p_size = *(size_t*)((instr->data) + 4 + 16 * i + 8);
      size_t _offset_add = 0;
      int n_subtasks = batch_p_size / slice_threshold;
      for (int j = 0; j < n_subtasks; j++) {
        // sub tasks for smaller slice
        char* _buf_s = (char*)mem->data_buf_ptr + offset + _offset_add;
        if (instr->type == SEND_BATCH) {
          fi_tsend(efa->ep, _buf_s, slice_threshold, NULL, efa->peer_addr,
                   task_seq, NULL);
          task_seq++;
        } else {
          fi_trecv(efa->ep, _buf_s, slice_threshold, NULL, efa->peer_addr,
                   task_seq, 0, NULL);
          task_seq++;
        }
        // increase offset
        _offset_add += slice_threshold;
      }
      size_t remain_size = batch_p_size - _offset_add;
      if (remain_size > 0) {
        n_subtasks += 1;
        wait_sizes[i] = n_subtasks;
        char* _buf_s = (char*)mem->data_buf_ptr + offset + _offset_add;
        if (instr->type == SEND_BATCH) {
          fi_tsend(efa->ep, _buf_s, remain_size, NULL, efa->peer_addr, task_seq,
                   NULL);
          task_seq++;
        } else {
          fi_trecv(efa->ep, _buf_s, remain_size, NULL, efa->peer_addr, task_seq,
                   0, NULL);
          task_seq++;
        }
      } else if (remain_size < 0) {
        std::cerr << "!!!not possible to have remain size lower than 0\n";
      } else {
        wait_sizes[i] = n_subtasks;
      }

      if (instr->type == SEND_BATCH) {
        std::cout << comm_name << "::" << rank
                  << " worker:: wait for SEND_BATCH tasks:" << wait_sizes[i]
                  << std::endl;
        this->_wait_cq(efa->txcq, wait_sizes[i]);
      } else {
        std::cout << comm_name << "::" << rank
                  << " worker:: wait for RECV_BATCH tasks:" << wait_sizes[i]
                  << std::endl;
        this->_wait_cq(efa->rxcq, wait_sizes[i]);
      }
      std::cout << comm_name << "::" << rank
                << " worker:: wait DONE:" << wait_sizes[i] << std::endl;
      // update worker mem cntr
      shm_lock(mem->sem_cntr, "sem cntr lock, err\n");
      (*(int*)mem->cntr_ptr) += 1;
      shm_unlock(mem->sem_cntr, "sem cntr unlock, err\n");
    }

    // std::cout << "launched tagged msgs \n";
    // for (int i = 0; i < batch_n; i++) {
    //   std::cout << name << " worker:: wait for n sub tasks:" << wait_sizes[i]
    //   << std::endl;
    //   if (instr->type == SEND_BATCH) {
    //     this->_wait_cq(efa->txcq, wait_sizes[i]);
    //   } else {
    //     this->_wait_cq(efa->rxcq, wait_sizes[i]);
    //   }
    //   // update worker mem cntr
    //   shm_lock(mem->sem_cntr, "sem cntr lock, err\n");
    //   (*(int*)mem->cntr_ptr) += 1;
    //   shm_unlock(mem->sem_cntr, "sem cntr unlock, err\n");
    // }

    delete[] wait_sizes;
  };

  // main function of the shm worker
  void run() {
    while (1) {
      Instruction* i = read_instr();
      if (i) {
        shm_lock(mem->sem_w_status, "set worker status: sem_wait err");
        std::cout << "instr type " << i->type << "\n";

        // set worker status as working
        *(int*)mem->status_ptr = 2;
        shm_unlock(mem->sem_w_status, "set worker status: sem_post err");
        switch (i->type) {
          case SET_EFA_ADDR:
            std::cout << "task for set efa addr\n";
            this->set_remote_efa_addr(efa_ep, i);
            break;
          case SEND_BATCH:
          case RECV_BATCH:
            std::cout << std::to_string(rank) + "worker get task SEND_BATCH/RECV_BATCH \n";
            this->efa_send_recv_batch(efa_ep, i);
            break;
          case ERR_INSTR:
            std::cerr << "err instr encountered \n";
            break;
        }
        shm_lock(mem->sem_w_status, "set worker status: sem_wait err");
        // change worker status
        *(int*)mem->status_ptr = 1;  // back to idle
        shm_unlock(mem->sem_w_status, "set worker status: sem_post err");
        // clear instruction
        delete i;
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  };

  ~SHMWorker() {
    delete mem;
    delete efa_ep;
  }
};

class SHMCommunicator {
 public:
  int nw;
  std::string name;
  std::string data_buf_name;
  size_t data_buf_size;
  // shm for workers
  std::string ws_instr_shm_name;
  std::string ws_cntr_shm_name;
  std::string ws_efa_addr_shm_name;
  std::string ws_status_shm_name;

  void* ws_instr_ptr;
  void* ws_cntr_ptr;
  void* ws_efa_add_ptr;
  void* ws_status_ptr;
  void* data_buf_ptr;

  // mutex
  std::vector<sem_t*> mtxs_instr;
  std::vector<sem_t*> mtxs_cntr;
  std::vector<sem_t*> mtxs_efa_addr;
  std::vector<sem_t*> mtxs_w_status;
  std::vector<sem_t*> mtxs_w_data_buf;

  // instr memory for communicator
  std::string shm_comm_instr;
  // cntr memory for communicator
  std::string shm_comm_cntr;
  // comm shm ptrs
  void* comm_instr_ptr;
  void* comm_cntr_ptr;
  // comm sem mtx
  sem_t* mtx_comm_instr;
  sem_t* mtx_comm_cntr;

  SHMCommunicator(int num_workers,
                  std::string name,
                  std::string data_buf_name,
                  size_t data_buf_size) {
    this->name = name;
    this->data_buf_name = data_buf_name;
    this->nw = num_workers;
    this->data_buf_size = data_buf_size;

    this->create_workers_shm_sem();
    this->create_self_shm_sem();
  };

  void create_workers_shm_sem() {
    // ------------ workers shm part
    ws_instr_shm_name = std::string(name + SHM_SUFFIX_INSTR);  // instruction
    ws_cntr_shm_name = std::string(name + SHM_SUFFIX_CNTR);    // counter
    ws_efa_addr_shm_name =
        std::string(name + SHM_SUFFIX_EFA_ADDR);  // local efa addr
    ws_status_shm_name =
        std::string(name + SHM_SUFFIX_W_STAT);  // worker status

    // create shm for data buffer for all workers
    int instr_fd = shm_open(ws_instr_shm_name.c_str(), O_CREAT | O_RDWR, 0666);
    int cntr_fd = shm_open(ws_cntr_shm_name.c_str(), O_CREAT | O_RDWR, 0666);
    int efa_add_fd =
        shm_open(ws_efa_addr_shm_name.c_str(), O_CREAT | O_RDWR, 0666);
    int worker_status_fd =
        shm_open(ws_status_shm_name.c_str(), O_CREAT | O_RDWR, 0666);
    int data_buf_fd = shm_open(data_buf_name.c_str(), O_CREAT | O_RDWR, 0666);

    // truncate memory
    check_err((ftruncate(instr_fd, INSTR_SIZE * nw) < 0),
              "ftruncate instr_fd err\n");
    check_err((ftruncate(cntr_fd, CNTR_SIZE * nw) < 0),
              "ftruncate cntr_fd err\n");
    check_err((ftruncate(data_buf_fd, data_buf_size) < 0),
              "ftruncate data_buf_fd err\n");
    check_err((ftruncate(efa_add_fd, EFA_ADDR_SIZE * nw) < 0),
              "ftruncate efa_add_fd err\n");
    check_err((ftruncate(worker_status_fd, STATUS_SIZE * nw) < 0),
              "ftruncate worker_status_fd err\n");
    // map memory pointer
    ws_instr_ptr = mmap(0, INSTR_SIZE * nw, PROT_READ | PROT_WRITE, MAP_SHARED,
                        instr_fd, 0);
    ws_cntr_ptr =
        mmap(0, CNTR_SIZE * nw, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);
    ws_efa_add_ptr = mmap(0, EFA_ADDR_SIZE * nw, PROT_READ | PROT_WRITE,
                          MAP_SHARED, efa_add_fd, 0);
    ws_status_ptr = mmap(0, STATUS_SIZE * nw, PROT_READ | PROT_WRITE,
                         MAP_SHARED, worker_status_fd, 0);
    data_buf_ptr = mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        data_buf_fd, 0);
    // set memory to all zero
    std::fill_n((char*)ws_instr_ptr, INSTR_SIZE * nw, 0);
    std::cout << "created instruction memory size: " << INSTR_SIZE* nw << "\n";
    // ------------ workers shm part end

    // ------------ workers semaphore part
    for (int i = 0; i < nw; i++) {
      // name for semaphores
      std::string _instr =
          std::string("/" + name + SEM_SUFFIX_INSTR + std::to_string(i));
      std::string _cntr =
          std::string("/" + name + SEM_SUFFIX_CNTR + std::to_string(i));
      std::string _efa_addr =
          std::string("/" + name + SEM_SUFFIX_EFA_ADDR + std::to_string(i));
      std::string _w_status =
          std::string("/" + name + SEM_SUFFIX_W_STAT + std::to_string(i));
      std::string _data_buf =
          std::string("/" + name + SEM_SUFFIX_DATA_BUF + std::to_string(i));
      // open and create semaphores, init value to 1
      sem_t* _mtx_instr =
          sem_open(_instr.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
      mtxs_instr.push_back(_mtx_instr);

      sem_t* _mtx_cntr = sem_open(_cntr.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
      mtxs_cntr.push_back(_mtx_cntr);

      sem_t* _mtx_efa_addr =
          sem_open(_efa_addr.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
      mtxs_efa_addr.push_back(_mtx_efa_addr);

      sem_t* _mtx_w_status =
          sem_open(_w_status.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
      mtxs_w_status.push_back(_mtx_w_status);

      sem_t* _mtx_data_buf =
          sem_open(_data_buf.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
      mtxs_w_data_buf.push_back(_mtx_data_buf);
    }
    // ------------ workers semaphore end
  };

  /* create shared memory for upper level API calls */
  void create_self_shm_sem() {
    shm_comm_instr = std::string(name + "-comm-instr-mem");
    shm_comm_cntr = std::string(name + "-comm-cntr-mem");
    int comm_instr_fd =
        shm_open(shm_comm_instr.c_str(), O_CREAT | O_RDWR, 0666);
    int comm_cntr_fd = shm_open(shm_comm_cntr.c_str(), O_CREAT | O_RDWR, 0666);
    check_err((ftruncate(comm_instr_fd, INSTR_SIZE) < 0),
              "ftruncate instr_fd err\n");
    check_err((ftruncate(comm_cntr_fd, CNTR_SIZE) < 0),
              "ftruncate cntr_fd err\n");

    comm_instr_ptr = mmap(0, INSTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                          comm_instr_fd, 0);
    comm_cntr_ptr =
        mmap(0, CNTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, comm_cntr_fd, 0);

    // create semaphore
    std::string sem_comm_instr("/" + name + "-comm-instr-mtx");
    std::string sem_comm_cntr("/" + name + "-comm-cntr-mtx");
    mtx_comm_instr =
        sem_open(sem_comm_instr.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
    mtx_comm_cntr =
        sem_open(sem_comm_cntr.c_str(), O_CREAT, S_IRUSR | S_IWUSR, 1);
  }

  bool local_efa_addrs_ready() {
    bool flag = true;
    for (int i = 0; i < nw; i++) {
      shm_lock(mtxs_efa_addr[i], "lock efa addr while check ready\n");
      bool all_zero = true;
      char* _addr_s = (char*)ws_efa_add_ptr + i * EFA_ADDR_SIZE;
      for (int j = 0; j < EFA_ADDR_SIZE; j++) {
        if (*(_addr_s + j) != 0) {
          all_zero = false;
        }
      }
      if (all_zero) {
        flag = false;
      }
      shm_unlock(mtxs_efa_addr[i], "unlock efa addr while check ready\n");
    }
    return flag;
  }

  /* check ready status first */
  void get_local_efa_addrs(char* addrs_buf) {
    for (int i = 0; i < nw; i++) {
      shm_lock(mtxs_efa_addr[i], "lock efa addr while get addr\n");
      memcpy(addrs_buf + i * EFA_ADDR_SIZE,
             (char*)ws_efa_add_ptr + i * EFA_ADDR_SIZE, EFA_ADDR_SIZE);
      shm_unlock(mtxs_efa_addr[i], "unlock efa addr while get addr\n");
    }
  }

  /* the addrs_buf contains addresses for all workers */
  void set_local_peer_addrs(char* addrs_buf) {
    for (int i = 0; i < nw; ++i) {
      shm_lock(mtxs_instr[i], "lock instr, while setting peer addr\n");
      char* _instr_buf_s = (char*)ws_instr_ptr + i * INSTR_SIZE;
      // set operation code to set EFA ADDR
      *(int*)(_instr_buf_s + 8) = reverse_map(trans::shm::SET_EFA_ADDR);
      memcpy(_instr_buf_s + 12, addrs_buf + i * EFA_ADDR_SIZE, EFA_ADDR_SIZE);
      *(double*)_instr_buf_s = time_now();
      shm_unlock(mtxs_instr[i], "unlock instr, while setting peer addr\n");
    }
  }

  void get_workers_cntr(int* cntrs) {
    int _cntr = 0;
    for (int i = 0; i < nw; ++i) {
      shm_lock(mtxs_cntr[i], "lock while getting cntr, err\n");
      cntrs[i] = *(int*)((char*)ws_cntr_ptr + i * CNTR_SIZE);
      shm_unlock(mtxs_cntr[i], "unlock while getting cntr, err\n");
    }
  }

  int get_a_worker_cntr(int widx) {
    shm_lock(mtxs_cntr[widx], "lock err, while getting cntr");
    int w_cntr = *(int*)((char*)ws_cntr_ptr + widx * CNTR_SIZE);
    shm_unlock(mtxs_cntr[widx], "unlock err, while getting cntr");
    return w_cntr;
  }

  void plus_one_self_cntr() {
    shm_lock(mtx_comm_cntr, "lock err, while increase self counter by one");
    *(int*)comm_cntr_ptr += 1;
    shm_unlock(mtx_comm_cntr, "unlock err, after increasing comm self cntr");
  }

  /* offsets are relative to the data_buf_ptr */
  void send_recv_batch(Instruction* instr, bool round_robin = true) {
    // get all current cntr
    int _ws_cntrs[nw];
    this->get_workers_cntr(_ws_cntrs);
    // lock all
    for (int i = 0; i < nw; i++) {
      shm_lock(mtxs_instr[i], "lock, while batch send/recv");
    }
    int n_batches = *(int*)(instr->data);
    if (round_robin) {
      for (int i = 0; i < nw; ++i) {
        int w_t_c = 0;
        char* _w_instr_p = (char*)ws_instr_ptr + i * INSTR_SIZE;
        char* _w_instr_data_p = _w_instr_p + INSTR_OFFSET;
        // fill instruction data part
        for (int j = 0; j < n_batches; j++) {
          if (j % nw == i) {
            unsigned long long _offset =
                *(unsigned long long*)(instr->data + 4 + j * 16);
            size_t _size = *(size_t*)(instr->data + 4 + j * 16 + 8);
            *(unsigned long long*)(_w_instr_data_p + 4 + w_t_c * 16) = _offset;
            *(size_t*)(_w_instr_data_p + 4 + w_t_c * 16 + 8) = _size;
            std::cout << "offset: " << _offset << ", size: " << _size << "("
                      << i << ", " << j << ") \n";
            // worker task counter ++
            w_t_c++;
          }
        }
        // first 4 bytes for task count
        *(int*)_w_instr_data_p = w_t_c;
        // assign operation type
        if (instr->type == SEND_BATCH)
          *(int*)(_w_instr_p + 8) = reverse_map(SEND_BATCH);
        else
          *(int*)(_w_instr_p + 8) = reverse_map(RECV_BATCH);
        // assign timestamp
        *(double*)_w_instr_p = time_now();
      }

    } else {
      for (int widx = 0; widx < nw; widx++) {
        char* _w_instr_p = (char*)ws_instr_ptr + widx * INSTR_SIZE;
        char* _w_instr_data_p = _w_instr_p + INSTR_OFFSET;
        // first 4 bytes for task count
        *(int*)_w_instr_data_p = n_batches;
        // assign operation type
        if (instr->type == SEND_BATCH) {
          std::cout << "comm::worker " << widx << "send " << n_batches
                    << std::endl;
          *(int*)(_w_instr_p + 8) = reverse_map(SEND_BATCH);
        } else {
          std::cout << "comm::worker " << widx << "recv " << n_batches
                    << std::endl;
          *(int*)(_w_instr_p + 8) = reverse_map(RECV_BATCH);
        }
        // compute offsets and sizes
        for (int i = 0; i < n_batches; i++) {
          size_t _offset = *(unsigned long long*)(instr->data + 4 + i * 16);
          size_t _size = *(size_t*)(instr->data + 4 + i * 16 + 8);

          size_t _worker_size = _size / nw;
          size_t _worker_offset = _offset + widx * _worker_size;

          if (widx == nw - 1) {
            // last worker has more responsibility
            _worker_size += (_size - _worker_size * nw);
          }
          *(size_t*)(_w_instr_data_p + 4 + i * 16) = _worker_offset;
          *(size_t*)(_w_instr_data_p + 4 + i * 16 + 8) = _worker_size;
        }

        // assign timestamp
        *(double*)_w_instr_p = time_now();
      }
    }
    // unlock all
    for (int i = 0; i < nw; i++) {
      shm_unlock(mtxs_instr[i], "unlock, while batch send/recv");
    }

    // wait for job to complete
    if (round_robin) {
      for (int i = 0; i < n_batches; i++) {
        // force the order
        int w_idx = i % nw;
        while (this->get_a_worker_cntr(w_idx) <= _ws_cntrs[w_idx]) {
          // blocking
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        _ws_cntrs[w_idx] += 1;
        // increase self comminicator counter.
        this->plus_one_self_cntr();
      }
    } else {
      double s = time_now();
      // wait for splitting parameters case
      int _level = 0;
      while (_level < n_batches) {
        bool _inc = true;  // sync flag
        for (int wid = 0; wid < nw; wid++) {
          if (this->get_a_worker_cntr(wid) - _ws_cntrs[wid] <= _level) {
            _inc = false;
          }
        }

        if (_inc) {
          this->plus_one_self_cntr();
          _level++;
          double e = time_now();
          std::cout << name + " wait batch " << _level << " completion takes " << e - s
                    << " s\n";
          s = e;
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      }
    }
  }

  Instruction* _read_instr() {
    Instruction* i = NULL;
    shm_lock(mtx_comm_instr, "lock err, while reading instr\n");
    double ts = *(double*)comm_instr_ptr;
    if (ts != 0) {
      i = new Instruction();
      INSTR_T i_type = instr_map(*((int*)((char*)comm_instr_ptr + 8)));
      i->type = i_type;
      i->timestamp = ts;
      memcpy(i->data, (char*)comm_instr_ptr + INSTR_OFFSET,
             INSTR_SIZE - INSTR_OFFSET);
      // clear mem
      std::fill_n((char*)comm_instr_ptr, INSTR_SIZE, 0);
    }
    shm_unlock(mtx_comm_instr, "unlock err, after reading intrs\n");
    return i;
  }

  void run() {
    while (1) {
      Instruction* i = _read_instr();
      if (i) {
        std::cout << "received a job \n";
        switch (i->type) {
          case SEND_BATCH:
          case RECV_BATCH:
            this->send_recv_batch(i, false);
            break;
          default:
            std::cerr << "err instr encountered \n";
            break;
        }
        // i is newed
        delete i;
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }
  };
};

};  // namespace shm
};  // namespace trans

#endif