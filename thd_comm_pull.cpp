#include "thd_comm.hpp"
#include "tcp.h"
#include <string>
#include <cstdlib>
#include <sys/mman.h>

int nw = 8;
size_t blockSize =  32 * 1024 * 1024;
int nBlock = 8;

void cliRecvThd(std::string efaPort, trans::ThdCommunicator* comm, char* recvBuff){

  // start receiving
  std::vector<std::pair<char*, size_t>> recvTo;
  for (int i = 0; i < nBlock; i++) {
    char* dataBuff = recvBuff + i * blockSize;
    recvTo.push_back(std::make_pair(dataBuff, blockSize));
  }
  while (true) {
    double startTime = trans::time_now();
    comm->arecvBatch(recvTo);
    comm->sync();

    // compute bw and output
    double dur = trans::time_now() - startTime;

    size_t totalSize = nBlock * blockSize;
    double bw = ((totalSize * 8) / dur) / 1e9;
    spdlog::info("client [{:s}] recv bw : {:f}, dur: {:f}s, total size {}", efaPort, bw, dur, totalSize);
  }
}

void runAsCli(std::vector<std::pair<std::string, int>>& servers){

  char* recvBuff = new char[10 * 1024 * 1024 * 1024UL];
  // mlock(recvBuff, 10 * 1024 * 1024 * 1024UL);
  size_t chunkSize = 500 * 1024 * 1024UL;

  std::vector<TcpClient> sockToServ;
  std::vector<std::thread> recvThds;
  std::vector<trans::ThdCommunicator*> comms;
  int serverCntr = 0;
  for (auto sp : servers) {

    trans::ThdCommunicator* _c = new trans::ThdCommunicator(nw);
    int EFAListen = _c->getListenPort();
    TcpClient toServer(sp.first, sp.second);
    spdlog::debug("sending local EFA listen port {:d}", EFAListen);
    toServer.tcpSend((char*)&EFAListen, sizeof(int));
    char buff[4] = {'\0'};
    toServer.tcpRecv(buff, sizeof(int));
    int dstEFAPort = *(int*)buff; 
    spdlog::debug("received dst EFA port {:d}", dstEFAPort);

    char* memPtr = recvBuff + serverCntr * chunkSize;
    
    _c->setPeer(sp.first, dstEFAPort);

    std::thread recv(cliRecvThd, std::to_string(EFAListen), _c, memPtr);
    recvThds.push_back(std::move(recv));
    comms.push_back(_c);

    serverCntr ++;
  }

  for (size_t i = 0; i < recvThds.size(); i++) {
    recvThds[i].join();
  }

}

void serverSendThd(std::shared_ptr<TcpAgent> cli, char* memBuff, size_t offset) {
  trans::ThdCommunicator comm(nw);
  int EFAListen = comm.getListenPort();

  char buf[4] = {'\0'};
  // exchange port for EFA address fetch
  cli->tcpRecv(buf, sizeof(int));
  int dstPort = *(int*)buf;
  spdlog::debug("received port for EFA connection {:d}", dstPort);
  cli->tcpSend((char*)&EFAListen, sizeof(int));

  std::string dstIP = cli->getIP();
  spdlog::info("client ip addr {:s}", dstIP);
  comm.setPeer(dstIP, dstPort);

  std::vector<std::pair<char*, size_t>> sendFrom;
  for (int i = 0; i < nBlock; i++) {
    char* dataBuff = memBuff + offset + i * blockSize;
    sendFrom.push_back(std::make_pair(dataBuff, blockSize));
  }

  // repeatly sending
  while (true) {
    double startTime = trans::time_now();
    comm.asendBatch(sendFrom);

    comm.sync();
    // compute bw and output
    double dur = trans::time_now() - startTime;

    size_t totalSize = nBlock * blockSize;
    double bw = ((totalSize * 8) / dur) / 1e9;
    spdlog::info("server [{:d}] send bw : {:f}, dur: {:f}", EFAListen, bw, dur);
  }

}

void runAsServer(int sockPort) {
  int listenPort = sockPort;
  char* memBuff = new char[10 * 1024UL * 1024UL * 1024UL];
  // mlock(memBuff, 10 * 1024UL * 1024UL * 1024UL);
  size_t offset = 0;
  size_t step = 500UL * 1024UL * 1024UL;

  TcpServer server("0.0.0.0", listenPort);
  spdlog::info("server started");
  std::vector<std::thread> cliThds;
  while (true) {
    std::shared_ptr<TcpAgent> cli = server.tcpAccept();
    spdlog::info("accepted one client");
    std::thread handleThd(serverSendThd, cli, memBuff, offset);
    cliThds.push_back(std::move(handleThd));

    offset += step;
  }
}

int main(int argc, char* argv[]){
  if(const char* env_p = std::getenv("DEBUG_THIS")) {
    spdlog::set_level(spdlog::level::debug);
  }

  if (argc < 2) {
    spdlog::error("running mode required, [cli/serv]");
    return -1;
  }
  std::string mode(argv[1]);
  if (mode == "cli") {
    if (argc < 3) {
      spdlog::error("running in cli mode requires: <num-servers>");
      return -1;
    }
    int numServers = std::atoi(argv[2]);
    if (argc < numServers * 2 + 3) {
      spdlog::error("require {} pairs of <addr> <port>", numServers);
      return -1;
    }
    std::vector<std::pair<std::string, int>> servers;
    for (int i = 0; i < numServers; i++) {
      std::string servAddr(argv[3+i*2]);
      int servPort = std::atoi(argv[4+i*2]);
      servers.push_back(std::make_pair(servAddr, servPort));
    }
    runAsCli(servers);
  }else if (mode == "serv") {
    if (argc < 3) {
      spdlog::error("running in serv mode requires: localSocketPort");
      return -1;
    }
    int sockPort = std::atoi(argv[2]);
    runAsServer(sockPort);
  } else {
    spdlog::error("unkown runing mode");
    return -1;
  }
}
