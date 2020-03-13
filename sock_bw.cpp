#include "sock_cli_serv.h"
#include <iostream>
#include <string>
#include <string.h>
#include <chrono>

int batch_p_size = 2 * 1024 * 1024; // 5MB
int total_size = 200 * 1024 * 1024; // 200MB

static const char integ_alphabet[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length =
    (sizeof(integ_alphabet) / sizeof(*integ_alphabet)) - 1;
void ft_fill_buf(void *buf, int size) {

  char *msg_buf;
  int msg_index;
  static unsigned int iter = 0;
  int i;

  msg_index = ((iter++) * 7) % integ_alphabet_length;
  msg_buf = (char *)buf;
  for (i = 0; i < size; i++) {
    msg_buf[i] = integ_alphabet[msg_index++];
    if (msg_index >= integ_alphabet_length)
      msg_index = 0;
  }
};

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
  ft_fill_buf(p_buf, total_size);
  for (int i = 0; i < total_size / batch_p_size; i++) {
    auto s = std::chrono::high_resolution_clock::now();
    char *_buf_s = p_buf + i * batch_p_size;
    int send_size;
    do {
      send_size = serv._send(_buf_s, batch_p_size);
      if (send_size != batch_p_size) {
        std::cout << "err while sending \n"
                  << "send out " << send_size << "\n";
      }
    } while (send_size != batch_p_size);

    auto e = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> cost_t = e - s;
    float bw = ((batch_p_size * 8) / (cost_t.count() / 1e3) ) / 1e9;
    std::cout << i << ": cost time" << cost_t.count() << " ms\n"
              << "bw: " << bw << "Gbps\n";
  }
  std::cout << "End exp\n";
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