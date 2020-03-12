#include "communicator.h"
#include "worker.h"
#include <iostream>
#include <string>
#include <unordered_map>

using namespace trans;


static const char integ_alphabet[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length =
    (sizeof(integ_alphabet) / sizeof(*integ_alphabet)) - 1;
void ft_fill_buf(void *buf, int size) {

  char *msg_buf;
  int msg_index;
  static unsigned int iter = 0;
  int i;

  msg_index = ((iter++) * 7) % integ_alphabet_length;
  msg_buf = (char *)buf;
  for (i = 0; i < size; i++) {
    msg_buf[i] = integ_alphabet[msg_index++];
    if (msg_index >= integ_alphabet_length)
      msg_index = 0;
  }
};

void send_parameters(Communicator *comm) {

  std::vector<Tasks*> ts;
  std::vector<char*> newbufs;
  // send out fake parameters
  int batch_p_size = 5 * 1024 * 1024; // 5MB
  int total_size = 200 * 1024 * 1024; // 200MB
  char *p_buf = new char[total_size];
  ft_fill_buf(p_buf, total_size);
  printf("p buf addr %p\n", p_buf);

  // do experiment once
  // receive request from client
  char *req_buf = new char[batch_p_size];
  Tasks *recv_once = new Tasks();
  recv_once->type = RECV;
  recv_once->bufs.push_back(req_buf);
  recv_once->sizes.push_back(batch_p_size);
  recv_once->numTask = 1;
  fid_cq *cq = comm->arecv(recv_once);
  comm->wait_cq(cq, 1);
  printf("Recv request msg: %s\n", req_buf);
  newbufs.push_back(req_buf);

  std::unordered_map<fid_cq *, int> umap;
  
  // construct send tasks for user
  auto s = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < comm->numThd; ++i) {
    Tasks *send_t = new Tasks();
    ts.push_back(send_t);
    send_t->type = SEND;
    int n_sub_tasks = (total_size / comm->numThd) / batch_p_size;
    send_t->numTask = n_sub_tasks;
    for (int j = 0; j < n_sub_tasks; j++) {
      char* _buf_s = p_buf + i * (total_size / comm->numThd) + j * batch_p_size;
      send_t->bufs.push_back(_buf_s);
      send_t->sizes.push_back(batch_p_size);
    }
    fid_cq *cq = comm->asend(send_t);
    if (umap.find(cq) != umap.end()) {
      umap[cq] += send_t->numTask;
    } else {
      umap[cq] = send_t->numTask;
    }
  }
  std::cout << "size of umap " << umap.size() << "\n";
  auto m = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> launch_cost = m - s;
  std::cout << "size of umap: " 
            << umap.size() 
            << " launch task cost " << launch_cost.count() << " ms\n";
  for (const auto &n : umap) {
    std::cout << "wait for " << n.second << " tasks \n";
    comm->wait_cq(n.first, n.second);
  }
  auto e = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> cost_t = e - s;
  float dur = cost_t.count();
  std::cout << "duration: " << dur << " ms\n";
  float bw = (total_size * 8 / (dur / 1000)) / 1e9;
  std::cout << "Send bw: " << bw << " Gbps\n";

  delete[] p_buf;
  // delete[] req_buf;
  // for (auto p:newbufs){
  //   delete[] p;
  // }
  // for (auto p:ts) {
  //   delete p;
  // }
};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage ./serv <listen-ip> <listen-port> <numThd>\n";
    return 1;
  }
  Communicator comm(true, std::stoi(argv[3]), std::string(argv[1]),
                    std::string(argv[2]));
  comm.server_listen();
  comm.enable();
  // warmup

  for (int i = 0; i < 10; ++i) {
    send_parameters(&comm);
    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  return 0;
}