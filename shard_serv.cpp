#include "shard_util.hpp"
#include "spdlog/spdlog.h"
#include "tcp.h"
#include "thd_comm.hpp"

void initCommunicator(std::shared_ptr<TcpAgent> cli,
                      trans::ThdCommunicator& comm) {
  comm.setSliceSize(COMM_SLICE);
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
}

void cliHandlerThd(std::shared_ptr<TcpAgent> fromCli, char* buf) {
  trans::ThdCommunicator comm(COMM_NW);
  initCommunicator(fromCli, comm);

  // main loop
  while (true) {
    // recv instruction
    char buf[8];  // two ints
    fromCli->tcpRecv(buf, 8);
    int bidx = *(int*)buf;
    int splitN = *(int*)(buf + 4);

    std::vector<std::pair<char*, size_t>> sendFrom;
    if (bidx < 0) {
      // request full model
      size_t size = totalSize(MODEL_BATCHES, MODEL_BATCH_N);
      if (splitN > 0) {  // vertical split
        size /= splitN;
      }
      sendFrom.push_back(std::make_pair(buf, size));

    } else if (bidx < MODEL_BATCH_N) {
      size_t size = MODEL_BATCHES[bidx];
      if (splitN > 0) {  // vertical split
        size /= splitN;
      }
      sendFrom.push_back(std::make_pair(buf, size));
    } else {
      spdlog::error("unknown instruction");
      throw "error unknown instruction";
    }

    comm.asendBatch(sendFrom);
  }
}

void runServer(int port) {
  TcpServer server("0.0.0.0", port);
  spdlog::info("server started");
  std::vector<std::thread> cliThds;
  char* memBuff = new char[10 * 1024UL * 1024UL * 1024UL];

  while (true) {
    std::shared_ptr<TcpAgent> cli = server.tcpAccept();
    spdlog::info("accepted one client");
    std::thread handleThd(cliHandlerThd, cli, memBuff);
    cliThds.push_back(std::move(handleThd));
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    spdlog::error("require a port for listen");
    return -1;
  }
  int serverPort = std::stoi(argv[1]);
  runServer(serverPort);
}