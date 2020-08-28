#include "thd_comm.hpp"
#include "tcp.h"
#include <string>

void runAsCli(){

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


}

void runAsServer() {
  int listenPort = 9999;
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
  
}