#include <fstream>
#include <thread>
#include "sock_cli_serv.h"
#include "thd_comm.hpp"

int repeatN = 10;

size_t load_params(char* buf, std::vector<std::pair<char*, size_t>>& dataLoc);

void genReceiverPtrs(char* recvBuf,
                     std::vector<std::pair<char*, size_t>>& paramLoc,
                     std::vector<std::pair<char*, size_t>>& recvLoc);

void _client() {
  std::string commDstIP("127.0.0.1");
  std::string commDstPort("8111");
  std::string commEFAPort("8222");

  std::string sockDstIP("127.0.0.1");
  std::string sockDstPort("8333");

  int nw = 4;
  pipeps::ThdCommunicator comm(commEFAPort, commDstIP, commDstPort, nw);
  spdlog::info("_client Created ThdCommunicator");

  size_t bufSize = 1 * 1024UL * 1024UL * 1024UL;
  char* paramBuf = new char[bufSize];
  char* recvBuf = new char[bufSize];
  std::vector<std::pair<char*, size_t>> paramLoc;
  std::vector<std::pair<char*, size_t>> recvLoc;
  size_t paramSize = load_params(paramBuf, paramLoc);
  genReceiverPtrs(recvBuf, paramLoc, recvLoc);
  spdlog::info("data and buffer preparement DONE");

  trans::SockCli scli(sockDstIP, sockDstPort);
  char syncBuf[4];
  std::string sync("sync");
  scli._send(sync.c_str(), 4);
  scli._recv(syncBuf, 4);
  spdlog::info("_client tcp sync DONE");

  for (int i = 0; i < repeatN; i++) {
    double ts = trans::time_now();
    size_t curC = comm.cntr;
    size_t target = curC + recvLoc.size();
    comm.arecvBatch(recvLoc);
    while (comm.cntr != target) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    double dur = trans::time_now() - ts;
    double bw = ((paramSize * 8) / dur) / 1e9;
    spdlog::info("_client bw : {:lf}, dur: {:lf}", bw, dur);
  }
  delete[] paramBuf;
  delete[] recvBuf;
};

void _server() {
  std::string commDstIP("127.0.0.1");
  std::string commDstPort("8222");
  std::string commEFAPort("8111");
  std::string sockPort("8333");

  int nw = 4;
  pipeps::ThdCommunicator comm(commEFAPort, commDstIP, commDstPort, nw);
  spdlog::info("_server Created ThdCommunicator");

  size_t bufSize = 1 * 1024UL * 1024UL * 1024UL;
  char* paramBuf = new char[bufSize];
  std::vector<std::pair<char*, size_t>> paramLoc;
  size_t paramSize = load_params(paramBuf, paramLoc);
  spdlog::info("data and buffer preparement DONE");

  trans::SockServ sServ(sockPort);
  sServ._listen();
  char syncBuf[4];
  sServ._recv(syncBuf, 4);
  std::string sync("sync");
  sServ._send(sync.c_str(), 4);
  spdlog::info("_server tcp sync DONE");

  for (int i = 0; i < repeatN; i++) {
    double ts = trans::time_now();
    size_t curC = comm.cntr;
    size_t target = curC + paramLoc.size();
    comm.asendBatch(paramLoc);
    while (comm.cntr != target) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    double dur = trans::time_now() - ts;
    double bw = ((paramSize * 8) / dur) / 1e9;
    spdlog::info("_server bw : {:lf}, dur: {:lf}", bw, dur);
  }

  delete[] paramBuf;
};

int main(int argc, char* argv[]) {
  // Set global log level to debug
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern("[%H:%M:%S.%f] [%^%l%$] [thread %t] %v");
  if (argc < 2) {
    spdlog::error("Usage: ./thd_ctest <client/server>");
    return -1;
  }
  std::string mode(argv[1]);
  if (mode == "client") {
    _client();
  } else {
    _server();
  }

  return 0;
}

size_t _load_to(std::string& filename, char* data_buf) {
  std::ifstream is(filename, std::ifstream::binary);
  if (is) {
    is.seekg(0, is.end);
    size_t length = is.tellg();
    is.seekg(0, is.beg);

    std::cout << "Read " << filename << "\n";

    is.read(data_buf, length);
    if (is)
      std::cout << "all characters read successfully.\n";
    else
      std::cout << "error: only " << is.gcount() << " could be read";
    is.close();

    return length;
  } else {
    return -1;
  }
};

size_t load_params(char* buf, std::vector<std::pair<char*, size_t>>& dataLoc) {
  std::string params_dir("./pbatches/");
  std::vector<std::string> bins;
  size_t _offset = 0;
  bins.push_back("batch-0-8344576.bin");
  bins.push_back("batch-1-39426048.bin");
  bins.push_back("batch-2-44810240.bin");
  bins.push_back("batch-3-44810240.bin");
  bins.push_back("batch-4-77922304.bin");
  bins.push_back("batch-5-26071040.bin");
  for (auto pb : bins) {
    std::string _filepath = params_dir + pb;
    char* param_buf = buf + _offset;
    size_t len = _load_to(_filepath, param_buf);
    _offset += len;
    dataLoc.push_back(std::make_pair(param_buf, len));
  }
  return _offset;
};

void genReceiverPtrs(char* recvBuf,
                     std::vector<std::pair<char*, size_t>>& paramLoc,
                     std::vector<std::pair<char*, size_t>>& recvLoc) {
  size_t _offset = 0;
  for (auto p : paramLoc) {
    char* recv_p = recvBuf + _offset;
    _offset += p.second;
    recvLoc.push_back(std::make_pair(recv_p, p.second));
  }
}
