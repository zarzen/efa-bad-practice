#include <fstream>

#include "storage.hpp"
#include "util.h"

using namespace pipeps::store;

void* openSHM(std::string& shmName, size_t& shmSize);

size_t loadParamToSHM(void* memPtr,
                      std::vector<std::pair<size_t, size_t>>& dataLoc);

void getRecvLoc(size_t& curOffset,
                std::vector<std::pair<size_t, size_t>>& sendFrom,
                std::vector<std::pair<size_t, size_t>>& recvTo);

bool verifyData(char* memPtr,
                std::vector<std::pair<size_t, size_t>>& sendFrom,
                std::vector<std::pair<size_t, size_t>>& recvTo);

void clearData(void* memPtr, std::vector<std::pair<size_t, size_t>>& d);

int main(int argc, char const* argv[]) {
  if (argc < 5) {
    std::cout
        << "Usage: ./store_cli <dstIp> <dstPort> <cacheName> <cacheSize>\n";
    return -1;
  }
  std::string ip(argv[1]);
  std::string port(argv[2]);
  std::string cacheName(argv[3]);
  size_t cacheSize = std::stoull(argv[4]);
  std::string cname("test-store-cli");

  std::string modelKey = "resnet152-test";

  std::unique_ptr<StoreCli> sCli(
      new StoreCli(cname, ip, port, cacheName, cacheSize, 4));

  void* dataPtr = openSHM(cacheName, cacheSize);
  std::vector<std::pair<size_t, size_t>> sendDataLoc;
  std::vector<std::pair<size_t, size_t>> recvDataLoc;
  size_t cacheOffset = loadParamToSHM(dataPtr, sendDataLoc);
  if (cacheOffset == 0) {
    std::cerr << "error happend while loading parameters\n";
    return -1;
  }
  size_t paramSize = cacheOffset;
  getRecvLoc(cacheOffset, sendDataLoc, recvDataLoc);
  std::cout << "params memory size: " + std::to_string(paramSize) + "\n";
  int curCntr = sCli->getCommCntr();
  int target = curCntr + sendDataLoc.size();
  double s = trans::time_now();
  sCli->push(modelKey, sendDataLoc);
  while (sCli->getCommCntr() != target) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  std::cout << "push parameters completed\n";

  // receive multiple times
  for (int i = 0; i < 10; i++) {
    double s = trans::time_now();
    curCntr = sCli->getCommCntr();
    target = curCntr + recvDataLoc.size();
    sCli->pull(modelKey, recvDataLoc);
    while (sCli->getCommCntr() != target) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    double e = trans::time_now();
    bool v = verifyData((char*)dataPtr, sendDataLoc, recvDataLoc);
    if (v)
      std::cout << "data correct\n";
    else
      std::cout << "received data wrong\n";

    double dur = e - s;
    double bw = ((paramSize * 8) / dur) / 1e9;
    std::cout << "bw: " << bw << " Gbps; "
              << " dur: " << dur << "\n";
    // clean received;
    clearData(dataPtr, recvDataLoc);
  }

  return 0;
}

void* openSHM(std::string& shmName, size_t& shmSize) {
  int data_fd = shm_open(shmName.c_str(), O_RDWR, 0666);
  void* data_buf_ptr =
      mmap(0, shmSize, PROT_READ | PROT_WRITE, MAP_SHARED, data_fd, 0);
  return data_buf_ptr;
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

size_t loadParamToSHM(void* memPtr,
                      std::vector<std::pair<size_t, size_t>>& dataLoc) {
  size_t _offset = 0;
  std::string dataDir("./pbatches/");
  std::string bins[6] = {"batch-0-8344576.bin",
                         "batch-1-39426048.bin",
                         "batch-2-44810240.bin",
                         "batch-3-44810240.bin",
                         "batch-4-77922304.bin",
                         "batch-5-26071040.bin"};
  for (auto b : bins) {
    char* buf = (char*)memPtr + _offset;
    std::string datafile = dataDir + b;
    size_t len = _load_to(datafile, buf);
    if (len < 0) {
      std::cerr << "err while loadParamToSHM\n";
      exit(-1);
    }
    std::cout << "load data size: " + std::to_string(len) << "\n";
    dataLoc.push_back(std::make_pair(_offset, len));
    _offset += len;
  }
  return _offset;
}

void getRecvLoc(size_t& curOffset,
                std::vector<std::pair<size_t, size_t>>& sendFrom,
                std::vector<std::pair<size_t, size_t>>& recvTo) {
  for (auto p : sendFrom) {
    recvTo.push_back(std::make_pair(p.first + curOffset, p.second));
  }
}

bool verifyData(char* memPtr,
                std::vector<std::pair<size_t, size_t>>& sendFrom,
                std::vector<std::pair<size_t, size_t>>& recvTo) {
  bool same = true;
  for (int i = 0; i < sendFrom.size(); i++) {
    auto _s = sendFrom[i];
    auto _r = recvTo[i];
    for (int j = 0; j < _s.second; j++) {
      if (*(memPtr + _s.first + j) != *(memPtr + _r.first + j)) {
        same = false;
        return same;
      } else {
        if (j < 10) {
          std::cout << *((char*)memPtr + _r.first + j);
        }
      }
    }
  }
  std::cout << "\n";
  return same;
}

void clearData(void* memPtr, std::vector<std::pair<size_t, size_t>>& d) {
  for (auto b : d) {
    std::fill_n((char*)memPtr + b.first, b.second, 0);
  }
}