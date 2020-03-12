#include "communicator.h"

#include <iostream>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <sys/socket.h>
#include <unistd.h>
#include "util.h"


namespace trans {

Communicator::Communicator(bool server_mode, int numThd, std::string ip,
                           std::string port) {
  this->server_mode = server_mode;
  this->ip = ip;
  this->port = port;
  this->numThd = numThd;

  // initialize memory
  this->init_addr_mem();
  this->init_workers();
  this->get_local_ep_addrs();

  this->init_socket();
  if (!server_mode) {
    // client mode
    this->exchange_ep_addrs();
  }
};

void _addtask(Worker *w, Tasks *t) {
  std::lock_guard<std::mutex> _lock(*(w->task_mutex));
  w->task_q->emplace(t);
};

fid_cq *Communicator::_atask(Tasks *t) {

  Worker *w = workers[cur_worker];

  _addtask(w, t);

  // next worker
  cur_worker += 1;
  cur_worker %= numThd;

  fid_cq *cq = t->type == SEND ? w->ep->cq : w->ep->cq;
  return cq;
};


fid_cq *Communicator::asend(Tasks *t) {
  if (t->type == SEND) {
    return _atask(t);
  }
  else {
    return NULL;
  }
  
};

fid_cq *Communicator::arecv(Tasks *t) {
  if (t->type == RECV) {
    return _atask(t);
  }
  else {
    return NULL;
  }
};


void Communicator::init_addr_mem() {
  // init for local
  local_ep_addrs = new char[addr_size * numThd];
  // init for remote
  remote_ep_addrs = new char[addr_size * numThd];
};

void Communicator::init_socket() {
  if (this->server_mode)
    this->init_server_socket();
  else
    this->init_client_socket();
};

void Communicator::init_server_socket() {

  int opt = 1;
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(std::stoi(port));

  // bind to port
  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }
  // listen on port, initialization done
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }
};

void Communicator::init_client_socket() {
  struct sockaddr_in serv_addr;
  if ((client_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    return;
  }
  // set up server addr
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::stoi(port));
  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
    printf("\nInvalid address/ Address not supported \n");
    return;
  }
  // connect to server
  if (connect(client_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) <
      0) {
    printf("\nConnection Failed \n");
    return;
  }
};

void Communicator::exchange_ep_addrs() {
  size_t msglen = numThd * addr_size;
  // send local addrs to server
  send(client_sock, local_ep_addrs, msglen, 0);
  // read remote addrs from server
  int valread = read(client_sock, remote_ep_addrs, msglen);
  // std::cout << "Get from remote bytes \n";
  // for (int i = 0; i < numThd * addr_size; ++i) {
  //   std::cout << int(remote_ep_addrs[i]) << ", ";
  // }
  // std::cout << "\n";
  std::cout << "Get remote EFA address \n";
  std::cout << this->str_ep_addr(remote_ep_addrs);
  // insert remote addrs to address vector of local endpoints
  this->insert_to_local_av();
};

void Communicator::init_workers() {
  for (int i = 0; i < numThd; ++i) {
    std::queue<Tasks*> *tq = new std::queue<Tasks*>();
    std::mutex *tm = new std::mutex();
    std::queue<complete_t> *sq = new std::queue<complete_t>();
    std::mutex *sm = new std::mutex();

    std::string workername = "worker-" + std::to_string(i);
    Worker *w = new Worker(workername, tq, tm, sq, sm);
    w->run();

    // record values
    this->workers.push_back(w);
    this->worker_tasks.push_back(tq);
    this->tasks_mutex.push_back(tm);
    this->worker_signals.push_back(sq);
    this->signals_mutex.push_back(sm);
  }
};

void Communicator::server_listen() {
  int addrlen = sizeof(serv_addr);
  if (!server_mode) {
    std::cerr << "must in server mode\n";
    return;
  }
  // accept connection from client
  int new_socket;
  if ((new_socket = accept(server_fd, (struct sockaddr *)&serv_addr,
                           (socklen_t *)&addrlen)) < 0) {
    perror("accept");
    exit(EXIT_FAILURE);
  }
  // read remote ep address from c0lient
  int buf_size = addr_size * numThd;
  int valread = read(new_socket, remote_ep_addrs, buf_size);
  std::cout << "Recv remote addrs from client: \n";
  std::cout << this->str_ep_addr(remote_ep_addrs);

  // send back to client
  send(new_socket, (void *)local_ep_addrs, buf_size, 0);

  // std::cout << "Send local ep addrs bytes \n";
  // for (int i = 0; i < numThd * addr_size; ++i) {
  //   std::cout << int(local_ep_addrs[i]) << ", ";
  // }
  // std::cout << "\n";
  // insert remote ep addrs into address vector
  this->insert_to_local_av();

  // wait for other requests through EFA
};

void Communicator::get_local_ep_addrs() {
  if (this->workers.size() == 0) {
    std::cerr << "workers are not initialized\n";
  }

  for (int i = 0; i < workers.size(); ++i) {
    char name_buf[addr_size];
    std::fill_n(name_buf, addr_size, 0);
    Worker *w = workers[i];
    w->ep->get_name(name_buf, addr_size);
    memcpy(local_ep_addrs + (i * addr_size), name_buf, addr_size);
  }
  std::cout << "Local ep addresses: \n";
  std::cout << this->str_ep_addr(local_ep_addrs);
};

void Communicator::insert_to_local_av() {
  for (int i = 0; i < numThd; ++i) {
    char addr_to_insert[64] = {0};
    memcpy(addr_to_insert, remote_ep_addrs + i * addr_size, addr_size);
    int err = fi_av_insert(workers[i]->ep->av, addr_to_insert, 1,
                           &(workers[i]->ep->peer_addr), 0, NULL);
    if (err != 1)
      std::cerr << "fi_av_insert err " << err;
  }

  std::cout << "Verify inserted address\n";
  // verify address inserted correct
  for (int i = 0; i < numThd; ++i) {
    char name_buf[64] = {0};
    size_t len = addr_size;
    fi_av_lookup(workers[i]->ep->av, workers[i]->ep->peer_addr, name_buf, &len);
    char printable[64] = {0};
    len = addr_size;
    fi_av_straddr(workers[i]->ep->av, name_buf, printable, &len);
    std::cout << "Worker-" << i << ": " << printable << "\n";
  }
};

std::string Communicator::str_ep_addr(char *addr) {
  std::string ret = "";
  for (int i = 0; i < numThd; ++i) {
    char readable[addr_size];
    size_t len = addr_size;
    Worker *w = workers[i];

    fi_av_straddr(w->ep->av, addr + (i * addr_size), readable, &len);
    ret += std::string(readable);
    ret += "\n";
  }
  return ret;
};

void Communicator::wait_cq(fid_cq *cq, int count) {
  struct fi_cq_err_entry entry;
  int ret, completed = 0;
  printf("wait_cq cq addr %p\n", cq);
  while (completed < count) {
    // ret = fi_cq_readfrom(cq, &entry, 1, &from);
    // ret = fi_cq_sread(cq, &entry, 1, NULL, timeout);
    ret = fi_cq_read(cq, &entry, 1);
    if (ret == -FI_EAGAIN)
      continue;

    if (ret == -FI_EAVAIL) {
      ret = fi_cq_readerr(cq, &entry, 1);
      CHK_ERR("fi_cq_readerr", (ret != 1), ret);

      printf("Completion with error: %d\n", entry.err);
      // if (entry.err == FI_EADDRNOTAVAIL)
      // 	get_peer_addr(entry.err_data);
    }

    CHK_ERR("fi_cq_read", (ret < 0), ret);
    completed++;
  }
};

void Communicator::enable() {
  enabled = true;
  // worker start start worker immediately after creation
  // for (int i = 0; i < numThd; ++i) {
  //   workers[i]->run();
  // }
  // std::cout << "Workers started \n";
  // event completion monitors start
};

Communicator::~Communicator() {
  for (int i = 0; i < numThd; ++i) {
    delete workers[i];
    delete worker_tasks[i];
    delete tasks_mutex[i];
    delete worker_signals[i];
    delete signals_mutex[i];
  }

  delete remote_ep_addrs;
  delete local_ep_addrs;
  // stop monitor threads is missing
  if (enabled) {
    delete thd_m_txcq;
    delete thd_m_rxcq;
  }
};

}; // namespace trans
