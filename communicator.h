#ifndef COMMUNICATOR_H
#define COMMUNICATOR_H

#include "worker.h"
#include <arpa/inet.h>
#include <string>
#include <vector>

namespace trans {

class Communicator {
public:
  bool server_mode = false;
  bool enabled = false;
  int cur_worker = 0; // round robin task assignment
  int server_fd = 0;
  struct sockaddr_in serv_addr;
  int client_sock = 0;
  std::string ip, port;
  size_t addr_size = 64;
  int numThd;

  char *remote_ep_addrs;
  char *local_ep_addrs;
  std::vector<Worker *> workers;
  std::vector<std::queue<Tasks*> *> worker_tasks;
  std::vector<std::mutex *> tasks_mutex;
  std::vector<std::queue<complete_t> *> worker_signals;
  std::vector<std::mutex *> signals_mutex;
  // following two queues are aggregated results
  // from all workers
  std::queue<complete_t> txcq;
  std::mutex _mtx_txcq;
  std::queue<complete_t> rxcq;
  std::mutex _mtx_rxcq;
  std::thread *thd_m_txcq, *thd_m_rxcq;

  void init_addr_mem();

  void init_socket();

  void init_server_socket();
  void init_client_socket();

  void init_workers();

  // get EFA addresses of each workers
  void get_local_ep_addrs();

  // send to addrs to remote and get back the remote ep addrs
  void exchange_ep_addrs();

  // insert addrs from remote into local workers
  void insert_to_local_av();

  // stringfy ep addrs
  std::string str_ep_addr(char *addr);

  Communicator();
  Communicator(bool server_mode, int numThd, std::string ip, std::string port);

  /* only in server mode for listening*/
  void server_listen();
  /* need to enable communicator to start workers;
    start threads for monite job completion
   */
  void enable();

  /* block operation to wait for certain completion */
  void wait_cq(fid_cq *cq, int count);

  // sync api
  // void ssend(char *buf, int len);
  // void srecv(char *buf, int len);

  // async api
  fid_cq *asend(Tasks *t);
  fid_cq *arecv(Tasks *t);
  fid_cq *_atask(Tasks *t);
  ~Communicator();
};

}; // namespace trans

#endif /* COMMUNICATOR_H */
