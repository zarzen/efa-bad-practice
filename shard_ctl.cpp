#include <sstream>
#include <vector>
#include "shard_util.hpp"
#include "spdlog/spdlog.h"
#include "tcp.h"

void waitClients(TcpServer& server,
                 int size,
                 std::vector<std::shared_ptr<TcpAgent>>& clientPtrs) {
  for (int i = 0; i < size; i++) {
    std::shared_ptr<TcpAgent> c = server.tcpAccept();
    clientPtrs.push_back(c);
    spdlog::info("accepted one client");
  }
}

void fullModelExp(int expID,
                  std::string logPrefix,
                  std::vector<std::shared_ptr<TcpAgent>>& clients,
                  int repeat) {
  for (int r = 0; r < repeat; r++) {
    // send instr
    for (int i = 0; i < clients.size(); i++) {
      clients[i]->tcpSend((char*)&expID, sizeof(int));
    }

    // wait response
    double totalTime = 0;
    for (int i = 0; i < clients.size(); i++) {
      char recvBuf[8];
      clients[i]->tcpRecv(recvBuf, 8);
      totalTime += *(double*)recvBuf;
    }
    double avgTime = totalTime / clients.size();
    spdlog::info(logPrefix, avgTime*1000);
  }
}

void layerwiseExp(int expID,
                  std::string logPrefix,
                  std::vector<std::shared_ptr<TcpAgent>>& clients,
                  int repeat) {
  for (int r = 0; r < repeat; r++) {
    // send
    for (int i = 0; i < clients.size(); i++) {
      clients[i]->tcpSend((char*)&expID, sizeof(int));
    }

    // wait for completion times
    double** layerCompletion = new double*[clients.size()];
    for (int i = 0; i < clients.size(); ++i) {
      layerCompletion[i] = new double[MODEL_BATCH_N];
    }
    size_t bufSize = MODEL_BATCH_N * sizeof(double);
    // char* recvBuf = new char[bufSize];
    for (int i = 0; i < clients.size(); ++i) {
      clients[i]->tcpRecv((char*)layerCompletion[i], bufSize);
      // clients[i]->tcpRecv(recvBuf, bufSize);
      // for (int j = 0; j < MODEL_BATCH_N; j++) {
      //   layerCompletion[i][j] = *(double*)(recvBuf + j * sizeof(double));
      // }
    }

    // compute avg layer completion time
    std::stringstream ss;
    for (int j = 0; j < MODEL_BATCH_N; j++) {
      double t = 0;
      for (int i = 0; i < clients.size(); ++i) {
        t += layerCompletion[i][j];
      }
      ss << t * 1000 / clients.size();
      if (j != MODEL_BATCH_N - 1)
        ss << ", ";
    }
    spdlog::info(logPrefix, ss.str());
  }
}

void perApp(std::vector<std::shared_ptr<TcpAgent>>& clients, int repeat) {
  fullModelExp(0, "per app (ms): {}", clients, repeat);
}

void verticalApp(std::vector<std::shared_ptr<TcpAgent>>& clients, int repeat) {
  fullModelExp(1, "vertical (ms): {}", clients, repeat);
}

void perLayer(std::vector<std::shared_ptr<TcpAgent>>& clients, int repeat) {
  layerwiseExp(2, "perLayer shard (ms): {}", clients, repeat);
}

void hybrid(std::vector<std::shared_ptr<TcpAgent>>& clients, int repeat) {
  layerwiseExp(3, "hybrid (ms): {}", clients, repeat);
}

int main(int argc, char* argv[]) {
  if (const char* env_p = std::getenv("DEBUG_THIS")) {
    spdlog::set_level(spdlog::level::debug);
  }

  if (argc < 3) {
    spdlog::error("require port to listen and client num");
    return -1;
  }
  int port = std::stoi(argv[1]);
  int cliN = std::stoi(argv[2]);

  TcpServer sockServ("0.0.0.0", port);
  std::vector<std::shared_ptr<TcpAgent>> clientPtrs;
  spdlog::info("waiting for {} clients", cliN);
  waitClients(sockServ, cliN, clientPtrs);

  spdlog::info("========per app exp========");
  perApp(clientPtrs, 2);
  spdlog::info("========vertical exp========");
  verticalApp(clientPtrs, 2);
  spdlog::info("========per layer exp========");
  perLayer(clientPtrs, 2);
  spdlog::info("========hybrid exp========");
  hybrid(clientPtrs, 2);

  // signal clients to exit
  int exitS = 4;
  for (auto conn: clientPtrs) {
    conn->tcpSend((char*)&exitS, 4);
  }
}