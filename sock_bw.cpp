#include "sock_cli_serv.h"
#include <iostream>
#include <string>
#include <string.h>
#include <chrono>

int batch_p_size = 5 * 1024 * 1024; // 5MB
int total_size = 200 * 1024 * 1024; // 200MB

void sock_cli(std::string ip, std::string port) {
  trans::SockCli cli(ip, port);
  char req_buf[64] = {0};
  memcpy(req_buf, "<fake-request>", 64);
  cli._send(req_buf, 64);
  char *p_buf = new char[total_size];

  for (int i = 0; i < total_size / batch_p_size; i++ ) {
    auto s = std::chrono::high_resolution_clock::now();
    char *_buf_s = p_buf + i * batch_p_size;
    int got_size = cli._recv(_buf_s, batch_p_size);
    if (got_size != batch_p_size) 
      std::cerr << "err while receiving\n";
    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> cost_t = e - s;
    float bw = ((batch_p_size * 8) / (cost_t.count() / 1e3) ) / 1e9;
    std::cout << i << ": cost time " << cost_t.count() << " ms\n"
              << "bw: " << bw << "Gbps\n";
  }

};

void sock_serv(std::string ip, std::string port) {
  trans::SockServ serv(port);
  char req_buf[64] = {0};
  serv._listen();
  serv._recv(req_buf, 64);
  std::cout << "recv msg: " << req_buf << "\n";

  // sending out parameters
  char *p_buf = new char[total_size];
  for (int i = 0; i < total_size / batch_p_size; ++i) {
    auto s = std::chrono::high_resolution_clock::now();
    char *_buf_s = p_buf + i * batch_p_size;
    int send_size = serv._send(_buf_s, batch_p_size);
    if (send_size != batch_p_size) 
      std::cerr << "err while sending\n";

    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> cost_t = e - s;
    float bw = ((batch_p_size * 8) / (cost_t.count() / 1e3) ) / 1e9;
    std::cout << i << ": cost time" << cost_t.count() << " ms\n"
              << "bw: " << bw << "Gbps\n";
  }
};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage: ./sock_bw <mode> <ip> <port>" 
              << "\n (<mode> = s/c, s: server; c: client)\n";
    return 1;
  }
  std::string mode(argv[1]);
  std::string ip(argv[2]);
  std::string port(argv[3]);
  if (mode == "c") {
    sock_cli(ip, port);
  }
  else if (mode == "s") {
    sock_serv(ip, port);
  } 
  else {
    std::cerr << "wrong running mode\n";
  }
};