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

enum INSTR_T { SET_EFA_ADDR, RECV_INSTR, SEND_INSTR, RECV_PARAM, SEND_PARAM };

class Instruction {
 public:
  INSTR_T type;
  double timestamp;
  char* data;
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
  };
};

class WorkerMemory {
 public:
  // 12 + 64 Bytes: 8 bytes for timestamp;
  // 4 bytes for instr-code; 64 bytes for instr data
  int instr_size = 76;
  int efa_addr_size = 64;
  int cntr_size = 4;                      // 4 Bytes
  int data_buf_size = 500 * 1024 * 1024;  // 500 MB
  int status_size = 4;                    // indicate the status of worker

  // shm identifiers
  std::string shm_instr;
  std::string shm_cntr;
  std::string shm_data_buf;
  std::string shm_efa_addr;
  std::string shm_w_status;
  std::string shm_mutex_name;

  void* instr_ptr;
  void* cntr_ptr;
  void* data_buf_ptr;
  void* efa_add_ptr;
  void* status_ptr;  // 1: idle; 2: working;
  sem_t* sem_mutex;

  WorkerMemory(std::string prefix, bool truncate) {
    shm_instr = std::string(prefix + "-instr-mem");
    shm_cntr = std::string(prefix + "-cntr-mem");
    shm_data_buf = std::string(prefix + "-data-buf-mem");
    shm_efa_addr = std::string(prefix + "-efa-addr-mem");
    shm_w_status = std::string(prefix + "-worker-status-mem");
    shm_mutex_name = std::string(prefix + "-mem-mutex");
    this->create_shm(truncate);
    // create lock
    sem_mutex = sem_open(shm_mutex_name.c_str(), O_CREAT);
  };

  void create_shm(bool truncate) {
    int instr_fd = shm_open(shm_instr.c_str(), O_CREAT | O_RDWR, 0666);
    int cntr_fd = shm_open(shm_cntr.c_str(), O_CREAT | O_RDWR, 0666);
    int data_buf_fd = shm_open(shm_data_buf.c_str(), O_CREAT | O_RDWR, 0666);
    int efa_add_fd = shm_open(shm_efa_addr.c_str(), O_CREAT | O_RDWR, 0666);
    int worker_status_fd =
        shm_open(shm_w_status.c_str(), O_CREAT | O_RDWR, 0666);

    // truncate memory size
    if (truncate) {
      check_err((ftruncate(instr_fd, instr_size) < 0),
                "ftruncate instr_fd err\n");
      check_err((ftruncate(cntr_fd, cntr_size) < 0), "ftruncate cntr_fd err\n");
      check_err((ftruncate(data_buf_fd, data_buf_size) < 0),
                "ftruncate data_buf_fd err\n");
      check_err((ftruncate(efa_add_fd, efa_addr_size) < 0),
                "ftruncate efa_add_fd err\n");
      check_err((ftruncate(worker_status_fd, status_size) < 0),
                "ftruncate worker_status_fd err\n");
    }
    instr_ptr =
        mmap(0, instr_size, PROT_READ | PROT_WRITE, MAP_SHARED, instr_fd, 0);
    cntr_ptr =
        mmap(0, cntr_size, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);
    data_buf_ptr = mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        data_buf_fd, 0);
    efa_add_ptr = mmap(0, efa_addr_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                       efa_add_fd, 0);
    status_ptr = mmap(0, status_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                      worker_status_fd, 0);
  };

  void mem_lock(std::string msg_if_err) {
    check_err(sem_wait(sem_mutex) < 0, msg_if_err);
  };

  void mem_unlock(std::string msg_if_err) {
    check_err(sem_post(sem_mutex) < 0, msg_if_err);
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
    // un link sem
    sem_unlink(shm_mutex_name.c_str());
  };
};

class SHMWorker {
  std::string name;
  EFAEndpoint* efa_ep;
  WorkerMemory* mem;

 public:
  SHMWorker(std::string name) {
    efa_ep = new EFAEndpoint((this->name + "-efa-ep"));
    mem = new WorkerMemory(name, true);
    this->name = name;
    this->set_local_efa_addr(efa_ep);
    // init worker status
    *(int*)(mem->status_ptr) = 1;
  };

  Instruction* read_instr() {
    mem->mem_lock("read_instr: sem_wait err");
    // read first 8 bytes for time stamp
    double ts = *((double*)mem->instr_ptr);
    if (ts == 0) {
      return NULL;
    } else {
      Instruction* i = new Instruction();
      i->timestamp = ts;
      // start from 9th byte to 12th stand for instr
      INSTR_T i_type = instr_map(*((int*)((char*)mem->instr_ptr + 8)));
      i->type = i_type;
      i->data = ((char*)mem->instr_ptr) + 12;

      // wipe first 12 bit to inidicate message already read
      std::fill_n(mem->instr_ptr, 12, 0);
      return i;
    }
    mem->mem_unlock("read_instr: sem_post err");
  };

  void set_local_efa_addr(EFAEndpoint* efa) {
    char local_ep_addrs[64] = {0};
    char readable[64] = {0};
    efa->get_name(local_ep_addrs, mem->efa_addr_size);
    size_t len = mem->efa_addr_size;
    fi_av_straddr(efa->av, local_ep_addrs, readable, &len);
    std::cout << "Local ep addresses: \n" << readable << "\n";

    mem->mem_lock("set_local_efa_addr: sem_wait err");
    std::memcpy(mem->efa_add_ptr, local_ep_addrs, mem->efa_addr_size);
    mem->mem_unlock("set_local_efa_addr: sem_post err");
  };

  /* insert remote efa addr into address vector */
  void set_remote_efa_addr(EFAEndpoint* efa, Instruction* i) {
    mem->mem_lock("set_remote_efa_addr: sem_wait err");
    fi_av_insert(efa->av, i->data, 1, &(efa->peer_addr), 0, NULL);
    mem->mem_unlock("set_remote_efa_addr: sem_post err");

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
      std::cout << name << ": efa recv msg: " << mem->data_buf_ptr << "\n";
    }

    mem->mem_lock("efa_send_recv_instr: increase cntr sem_wait err");
    (*(int*)mem->cntr_ptr) += 1;
    mem->mem_unlock("efa_send_recv_instr: increase cntr sem_post err");
  };

  /* assume the worker stores the partition of the parameters
    currently, simplest 5MB for each batch with 200MB total
  */
  void efa_send_recv_param(EFAEndpoint* efa, Instruction* i, bool is_send) {
    int total_size = 100 * 1024 * 1024;  // 100 MB
    int batch_p_size = 5 * 1024 * 1024;  // 5 MB

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
    mem->mem_lock("efa_send_recv_param: increase cntr sem_wait err");
    // increase the counter; later can modify to progressively increase
    (*(int*)mem->cntr_ptr) += total_size / batch_p_size;
    mem->mem_unlock("efa_send_recv_param: increase cntr sem_post err");
  }

  // main function of the shm worker
  void run() {
    while (1) {
      Instruction* i = read_instr();
      if (i) {
        mem->mem_lock("set worker status: sem_wait err");
        // set worker status as working
        *(int*)mem->status_ptr = 2;
        mem->mem_unlock("set worker status: sem_post err");
        switch (i->type) {
          case SET_EFA_ADDR:
            this->set_remote_efa_addr(efa_ep, i);
            break;
          case RECV_INSTR:
            this->efa_send_recv_instr(efa_ep, i, false);
            break;
          case SEND_INSTR:
            this->efa_send_recv_instr(efa_ep, i, true);
            break;
          case RECV_PARAM:
            this->efa_send_recv_param(efa_ep, i, false);
            break;
          case SEND_PARAM:
            this->efa_send_recv_param(efa_ep, i, true);
            break;
        }
        mem->mem_lock("set worker status: sem_wait err");
        // change worker status
        *(int*)mem->status_ptr = 1;  // back to idle
        mem->mem_unlock("set worker status: sem_post err");
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
};  // namespace trans

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "must specify worker name\n"
              << "Usage: ./worker <worker-name>\n";
  }
  std::string worker_name(argv[1]);
  trans::SHMWorker w(worker_name);
  w.run();

  return 0;
}