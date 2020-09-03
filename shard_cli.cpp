#include <chrono>
#include <thread>
#include "shard_util.hpp"
#include "tcp.h"
#include "thd_comm.hpp"

using namespace trans;
using std::pair;
using std::shared_ptr;
using std::string;
using std::vector;

shared_ptr<TcpClient> initCtl(char* ip, char* port) {
  string ipStr(ip);
  int portInt = std::stoi(port);
  return shared_ptr<TcpClient>(new TcpClient(ipStr, portInt));
}

void setupCommunicator(vector<string>& serverIPs,
                       vector<shared_ptr<TcpClient>>& tcpConns,
                       vector<shared_ptr<ThdCommunicator>>& comms) {
  for (int i = 0; i < tcpConns.size(); ++i) {
    int EFAListenPort = comms[i]->getListenPort();
    // send port
    tcpConns[i]->tcpSend((char*)&EFAListenPort, sizeof(int));
    // receiv port
    char buf[4];
    tcpConns[i]->tcpRecv(buf, sizeof(int));
    int dstPort = *(int*)buf;
    comms[i]->setPeer(serverIPs[i], dstPort);
  }
}

void initServerComm(char* argv[],
                    vector<shared_ptr<ThdCommunicator>>& comm2Servers,
                    vector<string>& serverIPs,
                    vector<shared_ptr<TcpClient>>& socket2Servers) {
  int numServers = std::atoi(argv[3]);
  spdlog::info("num server to connect {}", numServers);

  for (int i = 0; i < numServers; i++) {
    string servAddr(argv[4 + i * 2]);
    serverIPs.push_back(servAddr);
    int servPort = std::atoi(argv[5 + i * 2]);
    shared_ptr<TcpClient> tcp(new TcpClient(servAddr, servPort));
    socket2Servers.push_back(tcp);
    shared_ptr<ThdCommunicator> comm(new ThdCommunicator(COMM_NW));
    comm->setSliceSize(COMM_SLICE);
    comm2Servers.push_back(comm);
  }
  setupCommunicator(serverIPs, socket2Servers, comm2Servers);
}

void perAppExp(char* buf,
               std::shared_ptr<TcpClient> toCtl,
               vector<shared_ptr<TcpClient>>& socket2Servers,
               vector<shared_ptr<ThdCommunicator>>& comm2Servers) {
  double startTime = trans::time_now();
  char signalBuf[8];
  *(int*)signalBuf = -1;       // request full model
  *(int*)(signalBuf + 4) = 0;  // no split
  // send pull signal to servers
  socket2Servers[0]->tcpSend(signalBuf, 8);  // only pull from server 0
  // prepare recvTo buffers
  vector<pair<char*, size_t>> recvTo;
  size_t modelSize = totalSize(MODEL_BATCHES, MODEL_BATCH_N);
  recvTo.push_back(std::make_pair(buf, modelSize));

  comm2Servers[0]->arecvBatch(recvTo);
  comm2Servers[0]->sync();
  double dur = trans::time_now() - startTime;
  double bw = ((modelSize * 8) / dur) / 1e9;
  spdlog::info("per app recv bw : {:f}, dur: {:f} ms", bw, dur * 1000);

  // send result to ctl
  toCtl->tcpSend((char*)&dur, sizeof(double));
}

void verticalExp(char* buf,
                 std::shared_ptr<TcpClient> toCtl,
                 vector<shared_ptr<TcpClient>>& socket2Servers,
                 vector<shared_ptr<ThdCommunicator>>& comm2Servers) {
  double startTime = trans::time_now();
  char signalBuf[8];
  *(int*)signalBuf = -1;                           // request full model
  *(int*)(signalBuf + 4) = socket2Servers.size();  // split evently
  // send pull signal to servers
  for (auto sock : socket2Servers) {
    sock->tcpSend(signalBuf, 8);
  }

  // prepare recvTo buffers
  vector<pair<char*, size_t>> recvTo;
  size_t modelSize = totalSize(MODEL_BATCHES, MODEL_BATCH_N);
  // recv to same location
  size_t partSize = modelSize / comm2Servers.size();
  recvTo.push_back(std::make_pair(buf, partSize));

  // launch async recv
  for (auto comm : comm2Servers) {
    comm->arecvBatch(recvTo);
  }
  // sync
  for (auto comm : comm2Servers) {
    comm->sync();
  }

  double dur = trans::time_now() - startTime;
  double bw = ((modelSize * 8) / dur) / 1e9;
  spdlog::info("vertical recv bw : {:f}, dur: {:f} ms", bw, dur * 1000);

  // send result to ctl
  toCtl->tcpSend((char*)&dur, sizeof(double));
}

void perLayerExp(char* buf,
                 std::shared_ptr<TcpClient> toCtl,
                 vector<shared_ptr<TcpClient>>& socket2Servers,
                 vector<shared_ptr<ThdCommunicator>>& comm2Servers) {
  size_t numServer = socket2Servers.size();
  size_t* preCntrs = new size_t[numServer];
  for (int i = 0; i < comm2Servers.size(); ++i) {
    preCntrs[i] = comm2Servers[i]->getCntr();
  }

  double startTime = trans::time_now();

  // launch tasks
  char signalBuf[8];
  for (int i = 0; i < MODEL_BATCH_N; i++) {
    int sidx = i % numServer;
    *(int*)signalBuf = i;        // request batch i
    *(int*)(signalBuf + 4) = 0;  // no split
    socket2Servers[sidx]->tcpSend(signalBuf, 8);

    size_t batchSize = MODEL_BATCHES[i];
    vector<pair<char*, size_t>> recvTo;
    recvTo.push_back(std::make_pair(buf, batchSize));
    comm2Servers[sidx]->arecvBatch(recvTo);
  }

  // sync each batch
  // because EFA send-after-send option, it guarantees order
  double* completionCost = new double[MODEL_BATCH_N];
  for (int i = 0; i < MODEL_BATCH_N; ++i) {
    int sidx = i % numServer;
    while (comm2Servers[sidx]->getCntr() <= preCntrs[sidx]) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    preCntrs[sidx] += 1;
    // completed batch i
    completionCost[i] = trans::time_now() - startTime;
  }

  // send to ctl
  toCtl->tcpSend((char*)completionCost, sizeof(double) * MODEL_BATCH_N);

  delete[] completionCost;
  delete[] preCntrs;
}

void hybridExp(char* buf,
                 std::shared_ptr<TcpClient> toCtl,
                 vector<shared_ptr<TcpClient>>& socket2Servers,
                 vector<shared_ptr<ThdCommunicator>>& comm2Servers) {

  size_t numServer = socket2Servers.size();
  size_t* preCntrs = new size_t[numServer];
  for (int i = 0; i < comm2Servers.size(); ++i) {
    preCntrs[i] = comm2Servers[i]->getCntr();
  }

  double startTime = trans::time_now();

  // launch tasks
  char signalBuf[8];
  for (int i = 0; i < MODEL_BATCH_N; i++) {
    *(int*)signalBuf = i;        // request batch i
    *(int*)(signalBuf + 4) = numServer;  // no split

    // for each server signal part of the batch
    for (auto tcpConn : socket2Servers) {
      tcpConn->tcpSend(signalBuf, 8);
    }
    size_t batchSize = MODEL_BATCHES[i];
    size_t partSize = batchSize / numServer;
    vector<pair<char*, size_t>> recvTo; // FIXME quick dirty same location
    recvTo.push_back(std::make_pair(buf, partSize));
    for (auto comm : comm2Servers) {
      comm->arecvBatch(recvTo);
    }
  }

  // sync each batch
  // make sure all parts of a batch received
  double* completionCost = new double[MODEL_BATCH_N];
  for (int i = 0; i < MODEL_BATCH_N; ++i) {
    for (int sidx=0; sidx < numServer; ++ sidx) {
      while (comm2Servers[sidx]->getCntr() <= preCntrs[sidx]) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
      preCntrs[sidx] += 1;
    }
    // completed batch i
    completionCost[i] = trans::time_now() - startTime;
  }

  // send to ctl
  toCtl->tcpSend((char*)completionCost, sizeof(double) * MODEL_BATCH_N);

  delete[] completionCost;
  delete[] preCntrs;
}


int main(int argc, char* argv[]) {
  if (const char* env_p = std::getenv("DEBUG_THIS")) {
    spdlog::set_level(spdlog::level::debug);
  }
  if (argc < 4) {
    spdlog::error("require <ctl-ip> <ctl-port> <num-of-servers>");
  }

  shared_ptr<TcpClient> toCtl = initCtl(argv[1], argv[2]);
  spdlog::info("connected to controller {}:{}", argv[1], argv[2]);

  vector<shared_ptr<ThdCommunicator>> comm2Servers;
  vector<string> serverIPs;
  vector<shared_ptr<TcpClient>> socket2Servers;
  initServerComm(argv, comm2Servers, serverIPs, socket2Servers);
  spdlog::info("initialized communicator to servers");

  char* recvBuf = new char[10UL * 1024UL * 1024UL * 1024UL];
  char ctlInstrBuf[4] = {'\0'};

  // main loop
  bool exit = false;
  while (!exit) {
    // receive instruction from ctl
    toCtl->tcpRecv(ctlInstrBuf, 4);
    int instrCode = *(int*)ctlInstrBuf;
    switch (instrCode) {
      case 0:
        // per app
        perAppExp(recvBuf, toCtl, socket2Servers, comm2Servers);
        break;
      case 1:
        // vertical
        verticalExp(recvBuf, toCtl, socket2Servers, comm2Servers);
        break;
      case 2:
        // per layer
        perLayerExp(recvBuf, toCtl, socket2Servers, comm2Servers);
        break;
      case 3:
        // hybrid
        hybridExp(recvBuf, toCtl, socket2Servers, comm2Servers);
        break;
      default:
        exit = true;
        spdlog::info("exiting");
        break;
    }
  }

  // signal servers to exit threads
  for (auto conn: socket2Servers) {
    char buf[8] = {'\0'};
    *(int*)buf = MODEL_BATCH_N + 1;
    conn->tcpSend(buf, 8);
  }
}