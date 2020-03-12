#include "communicator.h"
#include "util.h"
#include <iostream>
#include <string>
#include <chrono>
#include <unordered_map>
#include <thread>

using namespace trans;

// void wait_cq(fid_cq *cq, int wait_for) {
//   struct fi_cq_err_entry entry;
//   int ret, completed = 0;

//   while (completed < wait_for) {
//     // ret = fi_cq_readfrom(cq, &entry, 1, &from);
//     // ret = fi_cq_sread(cq, &entry, 1, NULL, timeout);
//     ret = fi_cq_read(cq, &entry, 1);
//     if (ret == -FI_EAGAIN)
//       continue;

//     if (ret == -FI_EAVAIL) {
//       ret = fi_cq_readerr(cq, &entry, 1);
//       if (ret != 1)
//         std::cerr << "fi_cq_readerr\n";

//       printf("Completion with error: %d\n", entry.err);
//       // if (entry.err == FI_EADDRNOTAVAIL)
//       // 	get_peer_addr(entry.err_data);
//     }

//     if (ret < 0)
//       std::cerr << "fi_cq_read\n";
//     completed++;
//   }
// };

void recv_parameter(Communicator *comm) {

  std::vector<Tasks*> ts;
  std::vector<char*> newbufs;

  int batch_p_size = 5 * 1024 * 1024; // 5MB
  int total_size = 200 * 1024 * 1024; // 200MB
  char *p_buf = new char[total_size];
  printf("p_buf addr: %p\n", p_buf);

  // send a fake request
  Tasks *send_once = new Tasks();
  send_once->type = SEND;
  send_once->numTask  = 1;
  char *send_buf = new char[batch_p_size];
  std::string req_msg = "<fake-request-for-parameters>";
  memcpy(send_buf, req_msg.c_str(), req_msg.length());
  send_once->bufs.push_back(send_buf);
  send_once->sizes.push_back(batch_p_size);
  fid_cq *cq = comm->asend(send_once);
  wait_cq(cq, 1);
  ts.push_back(send_once);
  newbufs.push_back(send_buf);

  std::unordered_map<fid_cq*, int> umap;
  auto s = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < comm->numThd; ++i) {
    Tasks *send_t = new Tasks();
    ts.push_back(send_t);
    send_t->type = RECV;
    int n_sub_tasks = (total_size / comm->numThd) / batch_p_size;
    send_t->numTask = n_sub_tasks;
    for (int j = 0; j < n_sub_tasks; j++) {
      char* _buf_s = p_buf + i * (total_size / comm->numThd) + j * batch_p_size;
      send_t->bufs.push_back(_buf_s);
      send_t->sizes.push_back(batch_p_size);
    }
    fid_cq *cq = comm->arecv(send_t);
    if (umap.find(cq) != umap.end()) {
      umap[cq] += send_t->numTask;
    } else {
      umap[cq] = send_t->numTask;
    }
  }
  auto m = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> launch_cost = m - s;
  std::cout << "size of umap: " 
            << umap.size() 
            << " launch task cost " << launch_cost.count() << " ms\n";
  for (const auto& n: umap) {
    std::cout << "wait event completions " << n.second << "\n";
    comm->wait_cq(n.first, n.second);
  }
  auto e = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> cost_t = e - s;
  float dur = cost_t.count();
  std::cout << "duration: " << dur << " ms\n";
  float bw = (total_size * 8 / (dur / 1000)) / 1e9;
  std::cout << "Recv bw: " << bw << " Gbps\n";

  delete[] p_buf;
  // for (auto p:newbufs){
  //   delete[] p;
  // }
  // for (auto p:ts) {
  //   delete p;
  // }
};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage ./cli <dst-ip> <dst-port> <numThd>\n";
    return 1;
  }
  Communicator comm(false, std::stoi(argv[3]), std::string(argv[1]), std::string(argv[2]));
  comm.enable();
  
  for (int i = 0; i < 10; ++i) {
    recv_parameter(&comm);
    // std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  return 0;
}