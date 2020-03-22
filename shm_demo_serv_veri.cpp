#include <fstream>
#include <iostream>
#include <iomanip>

#include "shm_common.h"

void* data_buf_ptr;
void* comm_instr_ptr;
void* comm_cntr_ptr;

sem_t* mtx_comm_instr;
sem_t* mtx_comm_cntr;

std::string params_dir("./pbatches/");
std::vector<std::string> bins;
std::vector<size_t> p_sizes;
size_t p_offset = 10 * 1024 * 1024;  // 10 MB for other usage

size_t load_params();
// return the new offset after load the parameters
size_t _load_to(size_t offset, std::string& filename);

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
  data_buf_ptr = mmap(0, data_buf_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                      data_buf_fd, 0);
  comm_instr_ptr = mmap(0, trans::shm::INSTR_SIZE, PROT_READ | PROT_WRITE,
                        MAP_SHARED, instr_fd, 0);
  comm_cntr_ptr = mmap(0, trans::shm::CNTR_SIZE, PROT_READ | PROT_WRITE,
                       MAP_SHARED, cntr_fd, 0);

  // open mutex for instr
  mtx_comm_instr = sem_open(sem_comm_instr.c_str(), 0);
  mtx_comm_cntr = sem_open(sem_comm_cntr.c_str(), 0);
}

int get_comm_cntr() {
  trans::shm::shm_lock(mtx_comm_cntr, "lock err");
  int _c = *(int*)comm_cntr_ptr;
  trans::shm::shm_unlock(mtx_comm_cntr, "unlock err");
  return _c;
}

void serv_fake_trans(size_t total_p_size) {
  int cur_cntr = get_comm_cntr();
  // put send instr
  trans::shm::shm_lock(mtx_comm_instr, "lock err, while put instr");
  // put operation code
  *(int*)((char*)comm_instr_ptr + 8) =
      trans::shm::reverse_map(trans::shm::RECV_BATCH);
  // put instr data
  *(int*)((char*)comm_instr_ptr + 12) = 1;              // nbatch
  *(size_t*)((char*)comm_instr_ptr + 12 + 4) = 0;       // offset
  *(size_t*)((char*)comm_instr_ptr + 12 + 4 + 8) = 64;  // size
  // put timestamp
  *(double*)comm_instr_ptr = trans::time_now();
  trans::shm::shm_unlock(mtx_comm_instr,
                         "unlock err, after putting the send request ");

  while (get_comm_cntr() != cur_cntr + 1) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  // output data
  std::cout << "recv msg: " << (char*)data_buf_ptr << "\n";
  std::fill_n((char*)data_buf_ptr, 64, 0); // clear

  double s = trans::time_now();
  std::cout << "time now: " << s << "\n";
  cur_cntr = get_comm_cntr();

  // send batch parameters
  trans::shm::shm_lock(mtx_comm_instr, "lock err, while put instr");
  int n_batch = p_sizes.size();
  // put operation code
  *(int*)((char*)comm_instr_ptr + 8) =
      trans::shm::reverse_map(trans::shm::SEND_BATCH);
  *(int*)((char*)comm_instr_ptr + 12) = (int)n_batch;
  // each offset and size
  char* _offsets_sizes_s = (char*)comm_instr_ptr + 12 + 4;
  size_t _offset = p_offset;
  for (int i = 0; i < p_sizes.size(); i++) {
    *(size_t*)(_offsets_sizes_s + i * 16) = _offset;
    *(size_t*)(_offsets_sizes_s + i * 16 + 8) = p_sizes[i];
    // increase the offset
    _offset += p_sizes[i];
  }
  // put timestamp
  *(double*)comm_instr_ptr = trans::time_now();
  trans::shm::shm_unlock(mtx_comm_instr,
                         "unlock err, after putting the send request ");

  // wait completion
  while (get_comm_cntr() != cur_cntr + n_batch) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  double dur = trans::time_now() - s;
  double bw = ((total_p_size * 8) / dur) / 1e9;
  std::cout << "bw: " << bw << " Gbps; "
            << " dur: " << dur << "\n";
}

int main(int argc, char* argv[]) {
  std::cout.precision(9);
  std::cout << std::fixed;
  if (argc < 3) {
    std::cout << "Usage: ./shm_demo_cli <shm-prefix> <data-buf-size>\n";
  }
  std::string shm_prefix(argv[1]);
  size_t data_size = std::stoull(argv[2]);

  init_shm_sem(shm_prefix, data_size);

  // load data
  size_t total_p_size = load_params();
  std::cout << "loaded paramter to send size: " << total_p_size << "\n";

  int repeat_n = 10;
  for (int i = 0; i < repeat_n; i++) {
    serv_fake_trans(total_p_size);
  }
  return 0;
}

size_t _load_to(size_t offset, std::string& filename) {
  std::ifstream is(filename, std::ifstream::binary);
  if (is) {
    is.seekg (0, is.end);
    size_t length = is.tellg();
    is.seekg (0, is.beg);

    std::cout << "Read " << filename << "\n";

    is.read((char*)data_buf_ptr + offset, length);
    if (is)
      std::cout << "all characters read successfully.\n";
    else
      std::cout << "error: only " << is.gcount() << " could be read";
    is.close();
    
    // save the sizes to 
    p_sizes.push_back(length);
    return offset + length;
  }
};

size_t load_params() {
  size_t _offset = p_offset;
  bins.push_back("batch-0-8344576.bin");
  bins.push_back("batch-1-39426048.bin");
  bins.push_back("batch-2-44810240.bin");
  bins.push_back("batch-3-44810240.bin");
  bins.push_back("batch-4-77922304.bin");
  bins.push_back("batch-5-26071040.bin");
  for (auto pb:bins) {
    std::string _filepath = params_dir + pb;
    _offset = _load_to(_offset, _filepath);
  }
  return _offset - p_offset; // the size parameters loaded
};