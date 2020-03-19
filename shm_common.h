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

void check_err(bool cond, std::string msg) {
  if (cond) {
    std::cerr << msg;
  }
};

namespace trans {
namespace shm {

enum INSTR_T {
  ERR_INSTR,
  SET_EFA_ADDR,
  RECV_INSTR,
  SEND_INSTR,
  RECV_PARAM,
  SEND_PARAM
};

class Instruction {
 public:
  INSTR_T type;
  double timestamp;
  char data[500] = {0};
  Instruction(){};
};

INSTR_T instr_map(int idx) {
  switch (idx) {
    case 1:
      return SET_EFA_ADDR;
    case 2:
      return RECV_INSTR;
    case 3:
      return SEND_INSTR;
    case 4:
      return RECV_PARAM;
    case 5:
      return SEND_PARAM;
    default:
      return ERR_INSTR;
  };
};

int reverse_map(INSTR_T t) {
  switch (t) {
    case SET_EFA_ADDR:
      return 1;
    case RECV_INSTR:
      return 2;
    case SEND_INSTR:
      return 3;
    case RECV_PARAM:
      return 4;
    case SEND_PARAM:
      return 5;
    default:
      return -1;
  };
};

class WorkerMemory {
 public:
  // 12 + 500 Bytes:
  // 8 bytes for timestamp;
  // 4 bytes for instr-code; the remaining bytes for instr data
  int instr_size = 512;
  int efa_addr_size = 64;
  int cntr_size = 4;                     // 4 Bytes
  int status_size = 4;                   // indicate the status of worker
  unsigned long long int data_buf_size;  // input from user
  // based on rank to move the pointer of instr, efa_addr, cntr, status
  int rank;

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
   */
  WorkerMemory(std::string prefix, int rank, unsigned long long int data_size) {
    data_buf_size = data_size;
    this->rank = rank;

    shm_instr = std::string(prefix + "-instr-mem");        // instruction
    shm_cntr = std::string(prefix + "-cntr-mem");          // counter
    shm_efa_addr = std::string(prefix + "-efa-addr-mem");  // local efa addr
    shm_w_status = std::string(prefix + "-worker-status-mem");  // worker status
    shm_data_buf = std::string(prefix + "-data-buf-mem");       // data buf

    sem_name_instr =
        std::string("/" + prefix + "-instr-mtx-" + std::to_string(rank));
    sem_name_cntr =
        std::string("/" + prefix + "-cntr-mtx-" + std::to_string(rank));
    sem_name_data =
        std::string("/" + prefix + "-data-buf-mtx-" + std::to_string(rank));
    sem_name_efa_addr =
        std::string("/" + prefix + "-efa-addr-mtx-" + std::to_string(rank));
    sem_name_w_status = std::string("/" + prefix + "-worker-status-mtx-" +
                                    std::to_string(rank));

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
    instr_ptr =
        mmap(0, instr_size, PROT_READ | PROT_WRITE, MAP_SHARED, instr_fd, 0);
    cntr_ptr =
        mmap(0, cntr_size, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);
    efa_add_ptr = mmap(0, efa_addr_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                       efa_add_fd, 0);
    status_ptr = mmap(0, status_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                      worker_status_fd, 0);
    data_buf_ptr = mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        data_buf_fd, 0);
    // move the pointer
    instr_ptr = (void*)((char*)instr_ptr + this->rank * instr_size);
    cntr_ptr = (void*)((char*)cntr_ptr + this->rank * cntr_size);
    efa_add_ptr = (void*)((char*)efa_add_ptr + this->rank * efa_addr_size);
    status_ptr = (void*)((char*)status_ptr + this->rank * status_size);

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

  void mem_lock(sem_t* s, std::string msg_if_err) {
    check_err(sem_wait(s) < 0, msg_if_err);
  };

  void mem_unlock(sem_t* s, std::string msg_if_err) {
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
  std::string name;
  EFAEndpoint* efa_ep;
  WorkerMemory* mem;

 public:
  SHMWorker(std::string name,
            int rank,
            unsigned long long int shared_data_size) {
    efa_ep = new EFAEndpoint(this->name + "-efa-ep");
    mem = new WorkerMemory(name, rank, shared_data_size);
    this->name = name;
    this->set_local_efa_addr(efa_ep);
    // init worker status
    *(int*)(mem->status_ptr) = 1;
  };

  Instruction* read_instr() {
    mem->mem_lock(mem->sem_instr, "read_instr: sem_wait err");
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
      memcpy(i->data, ((char*)mem->instr_ptr) + 12, 500);
      // wipe first 12 bytes to inidicate message already read
      std::fill_n((char*)mem->instr_ptr, 12, 0);
    }
    mem->mem_unlock(mem->sem_instr, "read_instr: sem_post err");
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

    mem->mem_lock(mem->sem_efa_addr, "set_local_efa_addr: sem_wait err");
    std::memcpy(mem->efa_add_ptr, local_ep_addrs, mem->efa_addr_size);
    mem->mem_unlock(mem->sem_efa_addr, "set_local_efa_addr: sem_post err");
    mem->print_sem_mutex_val(mem->sem_efa_addr);
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

  void efa_send_recv_instr(EFAEndpoint* efa, Instruction* i, bool is_send) {
    if (is_send) {
      fi_send(efa->ep, i->data, 64, NULL, efa->peer_addr, NULL);
      wait_cq(efa->txcq, 1);
      std::cout << name << ": efa send msg: " << i->data << "\n";
    } else {
      fi_recv(efa->ep, mem->data_buf_ptr, 64, NULL, FI_ADDR_UNSPEC, NULL);
      wait_cq(efa->rxcq, 1);
      char _msg[64] = {0};
      memcpy(_msg, mem->data_buf_ptr, 64);
      std::cout << name << ": efa recv msg: " << _msg << "\n";
    }

    mem->mem_lock(mem->sem_cntr,
                  "efa_send_recv_instr: increase cntr sem_wait err");
    (*(int*)mem->cntr_ptr) += 1;
    mem->mem_unlock(mem->sem_cntr,
                    "efa_send_recv_instr: increase cntr sem_post err");
  };

  /* assume the worker stores the partition of the parameters
    currently, simplest 5MB for each batch with 200MB total
  */
  void efa_send_recv_param(EFAEndpoint* efa, Instruction* i, bool is_send) {
    int total_size = 100 * 1024 * 1024;  // 100 MB
    int batch_p_size = 5 * 1024 * 1024;  // 5 MB
    double _st = time_now();
    if (is_send) {
      for (int i = 0; i < total_size / batch_p_size; ++i) {
        char* _buf_s = ((char*)mem->data_buf_ptr) + i * batch_p_size;
        fi_send(efa->ep, _buf_s, batch_p_size, NULL, efa->peer_addr, NULL);
      }
      // wait for transition queue
      wait_cq(efa->txcq, total_size / batch_p_size);
    } else {
      for (int i = 0; i < total_size / batch_p_size; ++i) {
        char* _buf_s = ((char*)mem->data_buf_ptr) + i * batch_p_size;
        fi_recv(efa->ep, _buf_s, batch_p_size, NULL, 0, NULL);
      }
      // wait for receive queue
      wait_cq(efa->rxcq, total_size / batch_p_size);
    }
    std::cout << "send/recv params cost: " << time_now() - _st << "\n";

    mem->mem_lock(mem->sem_cntr,
                  "efa_send_recv_param: increase cntr sem_wait err");
    // increase the counter; later can modify to progressively increase
    (*(int*)mem->cntr_ptr) += total_size / batch_p_size;
    mem->mem_unlock(mem->sem_cntr,
                    "efa_send_recv_param: increase cntr sem_post err");
  }

  // main function of the shm worker
  void run() {
    while (1) {
      Instruction* i = read_instr();
      if (i) {
        std::cout << "instr type " << i->type << "\n";
        mem->mem_lock(mem->sem_w_status, "set worker status: sem_wait err");
        // set worker status as working
        *(int*)mem->status_ptr = 2;
        mem->mem_unlock(mem->sem_w_status, "set worker status: sem_post err");
        switch (i->type) {
          case SET_EFA_ADDR:
            std::cout << "task for set efa addr\n";
            this->set_remote_efa_addr(efa_ep, i);
            break;
          case RECV_INSTR:
            std::cout << "task for receive efa instr\n";
            this->efa_send_recv_instr(efa_ep, i, false);
            break;
          case SEND_INSTR:
            std::cout << "task for send efa instr\n";
            this->efa_send_recv_instr(efa_ep, i, true);
            break;
          case RECV_PARAM:
            std::cout << "task RECV_PARAM\n";
            this->efa_send_recv_param(efa_ep, i, false);
            break;
          case SEND_PARAM:
            std::cout << "task SEND_PARAM\n";
            this->efa_send_recv_param(efa_ep, i, true);
            break;
          case ERR_INSTR:
            std::cerr << "err instr encountered \n";
            break;
        }
        mem->mem_lock(mem->sem_w_status, "set worker status: sem_wait err");
        // change worker status
        *(int*)mem->status_ptr = 1;  // back to idle
        mem->mem_unlock(mem->sem_w_status, "set worker status: sem_post err");
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

void get_worker_efa_addr(WorkerMemory* shm_w, char* addr_buf) {
  shm_w->mem_lock(shm_w->sem_efa_addr, "lock at get_worker_efa_addr, err");
  memcpy(addr_buf, shm_w->efa_add_ptr, shm_w->efa_addr_size);
  shm_w->mem_unlock(shm_w->sem_efa_addr, "unlock at get_worker_efa_addr, err");
};

void set_peer_addr(WorkerMemory* shm_w, char* addr_buf) {
  shm_w->mem_lock(shm_w->sem_instr, "insert remote efa addr lock err");
  *(int*)((char*)shm_w->instr_ptr + 8) =
      trans::shm::reverse_map(trans::shm::SET_EFA_ADDR);  // SET EFA ADDR
  memcpy((char*)shm_w->instr_ptr + 12, addr_buf, 64);
  // set timestamp at last,
  double timestamp = trans::time_now();
  *((double*)(shm_w->instr_ptr)) = timestamp;
  shm_w->mem_unlock(shm_w->sem_instr, "insert remote efa addr unlock err");
}

int get_worker_status(WorkerMemory* wm) {
  wm->mem_lock(wm->sem_w_status, "lock while getting worker status");
  int s = *(int*)(wm->status_ptr);
  wm->mem_unlock(wm->sem_w_status, "unlock while getting worker status");
  return s;
};

int get_worker_cntr(WorkerMemory* wm) {
  wm->mem_lock(wm->sem_cntr, "getting cntr, lock err");
  int c = *(int*)wm->cntr_ptr;
  wm->mem_unlock(wm->sem_cntr, "getting cntr, unlock err");
  return c;
};

};  // namespace shm
};  // namespace trans

#endif