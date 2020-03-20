#include <iostream>

#include "shm_common.h"

void* data_buf_ptr;
void* comm_instr_ptr;
void* comm_cntr_ptr;

sem_t* mtx_comm_instr;
sem_t* mtx_comm_cntr;

void init_shm_sem(std::string& shm_prefix, size_t data_buf_size) {
  std::string shm_data_buf = shm_prefix + trans::shm::SHM_SUFFIX_DATA_BUF;
  std::string shm_comm_instr = shm_prefix + "-comm-instr-mem";
  std::string shm_comm_cntr = shm_prefix + "-comm-cntr-mem";
  // sem names
  std::string sem_comm_instr("/" + shm_prefix + "-comm-instr-mtx");
  std::string sem_comm_cntr("/" + shm_prefix + "-comm-cntr-mtx");

  // open shm and map it
  int data_buf_fd = shm_open(shm_data_buf.c_str(), O_RDWR, 0666);
  int instr_fd = shm_open(shm_comm_instr.c_str(), O_RDWR, 0666);
  int cntr_fd = shm_open(shm_comm_cntr.c_str(), O_RDWR, 0666);
  data_buf_ptr =
      mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED, data_buf_fd, 0);
  comm_instr_ptr = 
      mmap(0, trans::shm::INSTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, instr_fd, 0);
  comm_cntr_ptr = 
      mmap(0, trans::shm::CNTR_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, cntr_fd, 0);

  // open mutex for instr
  mtx_comm_instr =
        sem_open(sem_comm_instr.c_str(), 0);
  mtx_comm_cntr =
        sem_open(sem_comm_cntr.c_str(), 0);
}

int get_comm_cntr() {
  trans::shm::shm_lock(mtx_comm_cntr, "lock err");
  int _c = *(int*)comm_cntr_ptr;
  trans::shm::shm_unlock(mtx_comm_cntr, "unlock err");
  return _c;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "Usage: ./shm_demo_cli <shm-prefix> <data-buf-size>\n";
  }
  std::string shm_prefix(argv[1]);
  size_t data_size = std::stoull(argv[2]);

  init_shm_sem(shm_prefix, data_size);
  
  int cur_cntr = get_comm_cntr();
  // put data
  std::string msg = "<fake-request>";
  memcpy(data_buf_ptr, msg.c_str(), msg.length());
  // put send instr
  trans::shm::shm_lock(mtx_comm_instr, "lock err, while put instr");
  // put operation code
  *(int*)((char*)comm_instr_ptr + 8) = trans::shm::reverse_map(trans::shm::SEND_BATCH);
  // put instr data
  *(int*)((char*)comm_instr_ptr + 12) = 1; // n batch
  *(size_t*)((char*)comm_instr_ptr + 12 + 4) = 0; // offset
  *(int*)((char*)comm_instr_ptr + 12 + 4 + 8) = 64; // size
  // put timestamp
  *(double*)comm_instr_ptr = trans::time_now();
  trans::shm::shm_unlock(mtx_comm_instr, "unlock err, after putting the send request ");

  while (get_comm_cntr() != cur_cntr + 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  return 0;
}