#include "efa_ep.h"

#include <rdma/fi_tagged.h>

#include <cstring>

namespace trans {

void HandleCQError(struct fid_cq* cq) {
  struct fi_cq_err_entry err_entry;
  spdlog::error("Error while checking completion");
  int ret = fi_cq_readerr(cq, &err_entry, 1);
  char _err_buf[128];
  fi_cq_strerror(cq, err_entry.prov_errno, err_entry.err_data, _err_buf, 128);
  ERR_CHK(ret < 0,
          "Error while calling fi_cq_readerr, err code {:d}, err msg {:s}", ret,
          _err_buf);
}

EFAEndpoint::EFAEndpoint(std::string nickname) : fi(nullptr) {
  this->nickname = nickname;
  initialize();
};

void EFAEndpoint::printableAddr(char* buf, int size) {
  if (size < addrSize) {
    throw "require larger buffer for";
  }
  size_t len = size;
  fi_av_straddr(this->av, this->addr, buf, &len);
}

void EFAEndpoint::printablePeerAddr(char* buf, int size) {
  if (size != this->addrSize) {
    throw "size not match";
  }
  char addrBuf[this->addrSize];
  size_t len = 64;
  fi_av_lookup(this->av, this->peer_addr, addrBuf, &len);
  len = 64;
  std::fill_n(buf, size, '\0');
  fi_av_straddr(this->av, addrBuf, buf, &len);
}

int EFAEndpoint::initialize() {
  struct fi_cq_attr txcq_attr, rxcq_attr;
  struct fi_av_attr av_attr;
  int err;
  std::string provider = "efa";

  hints = fi_allocinfo();
  ERR_CHK(!hints, "fi_allocinfo err {}", -ENOMEM);

  // clear all buffers
  memset(&txcq_attr, 0, sizeof(txcq_attr));
  memset(&rxcq_attr, 0, sizeof(rxcq_attr));
  memset(&av_attr, 0, sizeof(av_attr));

  // get provider
  hints->ep_attr->type = FI_EP_RDM;
  hints->fabric_attr->prov_name = strdup(provider.c_str());
  hints->domain_attr->control_progress = FI_PROGRESS_MANUAL;
  hints->domain_attr->data_progress = FI_PROGRESS_MANUAL;
  // SAS
  hints->rx_attr->msg_order = FI_ORDER_SAS;
  hints->tx_attr->msg_order = FI_ORDER_SAS;
  err = fi_getinfo(FI_VERSION(1, 9), nullptr, 0, 0, hints, &fi);
  ERR_CHK(err < 0, "fi_getinfo err {} ", err);

  // fi_freeinfo(hints);
  spdlog::debug("Using OFI device: {:s}", fi->fabric_attr->name);

  // init fabric, domain, address-vector,
  err = fi_fabric(fi->fabric_attr, &fabric, NULL);
  ERR_CHK(err < 0, "fi_fabric err {} ", err);

  err = fi_domain(fabric, fi, &domain, NULL);
  ERR_CHK(err < 0, "fi_domain err {} ", err);

  av_attr.type = fi->domain_attr->av_type;
  av_attr.count = 1;
  err = fi_av_open(domain, &av_attr, &av, NULL);
  ERR_CHK(err < 0, "fi_av_open err {} ", err);

  // open complete queue
  txcq_attr.format = FI_CQ_FORMAT_TAGGED;
  txcq_attr.size = fi->tx_attr->size;
  rxcq_attr.format = FI_CQ_FORMAT_TAGGED;
  rxcq_attr.size = fi->rx_attr->size;
  err = fi_cq_open(domain, &txcq_attr, &txcq, NULL);
  ERR_CHK(err < 0, "fi_txcq_open err {} ", err);

  err = fi_cq_open(domain, &rxcq_attr, &rxcq, NULL);
  ERR_CHK(err < 0, "fi_rxcq_open err {} ", err);

  // open endpoint
  err = fi_endpoint(domain, fi, &ep, NULL);
  ERR_CHK(err < 0, "fi_endpoint err {} ", err);

  // bind complete queue, address vector to endpoint
  err = fi_ep_bind(ep, (fid_t)txcq, FI_SEND);
  ERR_CHK(err < 0, "fi_ep_bind txcq err {} ", err);

  err = fi_ep_bind(ep, (fid_t)rxcq, FI_RECV);
  ERR_CHK(err < 0, "fi_ep_bind rxcq err {} ", err);

  err = fi_ep_bind(ep, (fid_t)av, 0);
  ERR_CHK(err < 0, "fi_ep_bind av err {} ", err);

  // enable endpoint
  err = fi_enable(ep);
  ERR_CHK(err < 0, "fi_enable err {} ", err);

  // get self addr
  size_t len = 64;
  err = fi_getname((fid_t)ep, this->addr, &len);
  ERR_CHK(err < 0, "fi_getname err {} ", err);

  selfReady = true;
  return err;
};

void EFAEndpoint::getAddr(char* name_buf, int size) {
  if (size != this->addrSize) {
    throw "wrong size";
  }
  memcpy(name_buf, this->addr, size);
};

void EFAEndpoint::insertPeerAddr(char* addr) {
  int ret = 0;
  ret = fi_av_insert(av, addr, 1, &peer_addr, 0, NULL);
  ERR_CHK(ret != 1, "fi_av_insert {}", ret);
  this->peerReady = true;
};

int EFAEndpoint::waitCQ(fid_cq* cq, int count) {
  struct fi_cq_err_entry entry;
  int ret, completed = 0;
  while (completed < count) {
    ret = fi_cq_read(cq, &entry, 1);
    if (ret == -FI_EAGAIN) {
      continue;
    }

    if (ret == -FI_EAVAIL) {
      HandleCQError(cq);
    }
    ERR_CHK(ret < 0, "{:s} fi_cq_read err", this->nickname);
    completed++;
  }
  return completed;
}

int EFAEndpoint::send(const void* buf, size_t len, uint64_t tag) {
  this->isend(buf, len, tag);
  return this->syncSend();
};

int EFAEndpoint::isend(const void* buf, size_t len, uint64_t tag) {
  ERR_CHK(!(selfReady && peerReady), "isend error, selfReady {}, peerReady {}",
          selfReady, peerReady);
  this->sendCntr++;
  return fi_tsend(this->ep, buf, len, NULL, this->peer_addr, tag, NULL);
}

int EFAEndpoint::syncSend() {
  int completed = this->waitCQ(this->txcq, this->sendCntr);
  this->sendCntr -= completed;
  return completed;
}

int EFAEndpoint::recv(void* buf, size_t len, uint64_t tag) {
  this->irecv(buf, len, tag);
  this->syncRecv();
}

int EFAEndpoint::irecv(void* buf, size_t len, uint64_t tag) {
  ERR_CHK(!(selfReady && peerReady), "irecv error, selfReady {}, peerReady {}",
          selfReady, peerReady);
  this->recvCntr++;
  return fi_trecv(this->ep, buf, len, NULL, this->peer_addr, tag, 0, NULL);
}

int EFAEndpoint::syncRecv() {
  int completed = this->waitCQ(this->rxcq, this->recvCntr);
  this->recvCntr -= completed;
  return completed;
}

EFAEndpoint::~EFAEndpoint() {
  fi_close((fid_t)ep);
  fi_close((fid_t)txcq);
  fi_close((fid_t)rxcq);
  fi_close((fid_t)av);
  fi_close((fid_t)domain);
  fi_close((fid_t)fabric);
  fi_freeinfo(fi);
  fi_freeinfo(hints);
};

};  // namespace trans