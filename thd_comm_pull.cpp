#include "thd_comm.hpp"
#include "tcp.h"
#include <string>

void runAsCli(int EFAListen, std::string dstSockAddr, int dstSockPort){
  char* recvBuff = new char[1024 * 1024 * 1024UL];

  TcpClient toServer(dstSockAddr, dstSockPort);
  std::string efaPortStr = std::to_string(EFAListen);
  toServer.tcpSend(efaPortStr.c_str(), sizeof(int));
  char buff[4] = {'\0'};
  toServer.tcpRecv(buff, sizeof(int));
  pipeps::ThdCommunicator comm(efaPortStr, dstSockAddr, std::string(buff, sizeof(int)), 4);

  // start receiving
  size_t blockSize =  32 * 1024 * 1024;
  int nBlock = 8;
  std::vector<std::pair<char*, size_t>> recvTo;
  for (int i = 0; i < nBlock; i++) {
    char* dataBuff = recvBuff + i * blockSize;
    recvTo.push_back(std::make_pair(dataBuff, blockSize));
  }
  while (true) {
    double startTime = trans::time_now();
    size_t target_cntr = comm.cntr + recvTo.size();
    comm.arecvBatch(recvTo);

    // wait for completion
    while (comm.cntr != target_cntr) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    // compute bw and output
    double dur = trans::time_now() - startTime;

    size_t totalSize = nBlock * blockSize;
    double bw = ((totalSize * 8) / dur) / 1e9;
    spdlog::info("client [{:d}] send bw : {:f}, dur: {:f}", EFAListen, bw, dur);
  }
}

void serverSendThd(std::shared_ptr<TcpAgent> cli, int EFAListen, char* memBuff, size_t offset) {
  
  char buf[4] = {'\0'};
  // exchange port for EFA address fetch
  cli->tcpRecv(buf, sizeof(int));
  int dstPort = std::atoi(buf);
  std::string efaPortStr = std::to_string(EFAListen);
  cli->tcpSend(efaPortStr.c_str(), sizeof(int));

  std::string dstIP = cli->getIP();
  pipeps::ThdCommunicator comm(efaPortStr, dstIP, std::to_string(dstPort), 4);

  size_t blockSize =  32 * 1024 * 1024;
  int nBlock = 8;
  std::vector<std::pair<char*, size_t>> sendFrom;
  for (int i = 0; i < nBlock; i++) {
    char* dataBuff = memBuff + i * blockSize;
    sendFrom.push_back(std::make_pair(dataBuff, blockSize));
  }

  // repeatly sending
  while (true) {
    double startTime = trans::time_now();
    size_t target_cntr = comm.cntr + sendFrom.size();
    comm.asendBatch(sendFrom);

    while (comm.cntr != target_cntr) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    // compute bw and output
    double dur = trans::time_now() - startTime;

    size_t totalSize = nBlock * blockSize;
    double bw = ((totalSize * 8) / dur) / 1e9;
    spdlog::info("server [{:d}] send bw : {:f}, dur: {:f}", EFAListen, bw, dur);
  }

}

void runAsServer(int sockPort) {
  int listenPort = sockPort;
  int EFAListenPort = 10000;
  char* memBuff = new char[1024UL * 1024UL * 1024UL];
  size_t offset = 0;
  size_t step = 250UL * 1024UL * 1024UL;

  TcpServer server("0.0.0.0", listenPort);

  while (true) {
    std::shared_ptr<TcpAgent> cli = server.tcpAccept();
    std::thread handleThd(serverSendThd, cli, EFAListenPort, memBuff, offset);

    EFAListenPort++;
    offset += step;
  }
}

int main(int argc, char* argv[]){
  if (argc < 2) {
    spdlog::error("running mode required, [cli/serv]");
    return -1;
  }
  std::string mode(argv[1]);
  if (mode == "cli") {
    if (argc < 5) {
      spdlog::error("running in cli mode requires: localEFAPort, dstAddr, dstPort");
      return -1;
    }
    int localEFAListen = std::atoi(argv[2]);
    std::string dstAddr(argv[3]);
    int dstPort = std::atoi(argv[4]);
    runAsCli(localEFAListen, dstAddr, dstPort);
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