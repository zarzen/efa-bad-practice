#ifndef EFA_EP_H
#define EFA_EP_H

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

#include <atomic>
#include <iostream>

#include "spdlog/spdlog.h"

namespace trans {

#define ERR_CHK(cond, msg, ...)        \
  do {                                 \
    if (cond) {                        \
      spdlog::error(msg, __VA_ARGS__); \
      exit(1);                         \
    }                                  \
  } while (0)

class EFAEndpoint {
  char addr[64];
  std::atomic_size_t sendCntr{0};
  std::atomic_size_t recvCntr{0};
  int waitCQ(fid_cq* cq, int count);

  std::string nickname;
  bool selfReady = false;
  bool peerReady = false;

  struct fi_info* fi;
  struct fi_info* hints;
  struct fid_fabric* fabric;
  struct fid_domain* domain;
  struct fid_av* av;
  struct fid_ep* ep;
  struct fid_cq* txcq;
  struct fid_cq* rxcq;
  fi_addr_t peer_addr;

 public:
  const static int addrSize{64};
  EFAEndpoint(std::string nickname);

  int initialize();

  void getAddr(char* name_buf, int size);
  void printableAddr(char* buf, int size);
  void printablePeerAddr(char* buf, int size);

  void insertPeerAddr(char* addr);

  int send(const void* buf, size_t len, uint64_t tag);
  int isend(const void* buf, size_t len, uint64_t tag);
  int syncSend();

  int recv(void* buf, size_t len, uint64_t tag);
  int irecv(void* buf, size_t len, uint64_t tag);
  int syncRecv();

  std::string getName() { return this->nickname; }

  ~EFAEndpoint();
};

};     // namespace trans
#endif /* EFA_EP_H */