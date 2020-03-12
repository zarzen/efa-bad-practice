#ifndef EFA_EP_H
#define EFA_EP_H

#include <iostream>

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

namespace trans {

class EFAEndpoint {
public:
  std::string nickname;
  bool ep_ready = false;
  bool av_ready = false;

  struct fi_info *fi;
  struct fid_fabric *fabric;
  struct fid_domain *domain;
  struct fid_av *av;
  struct fid_ep *ep;
  struct fid_cq *txcq, *rxcq;
  fi_addr_t peer_addr;

  size_t max_size = 64;

  EFAEndpoint(std::string nickname);

  int init_res();

  void get_name(char *name_buf, int size);

  void insert_peer_address(char *addr);

  ~EFAEndpoint();
};

};     // namespace trans
#endif /* EFA_EP_H */