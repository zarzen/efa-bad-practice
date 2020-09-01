
#include "helper_socket.h"
#include "spdlog/spdlog.h"

namespace trans {

SockCli::SockCli(std::string ip, std::string port) {
  struct sockaddr_in serv_addr;
  if ((client_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    throw "Socket creation error";
  }

  int yes = 1;
  if (setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes))) {
    perror("util/common/TCPServer SetSocketOption");
    exit(EXIT_FAILURE);
  }

  // set up server addr
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::stoi(port));
  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
    printf("\nInvalid address/ Address not supported \n");
    throw "Invalid address/ Address not supported";
  }
  // connect to server
  if (connect(client_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) <
      0) {
    spdlog::error("Connection Failed");
    throw "Connection Failed";
  }
};

SockCli::SockCli() {}

SockCli::SockCli(const SockCli& s) {
  this->client_sock = s.client_sock;
}

int SockCli::_send(const char* buf, int len) {
  return send(client_sock, buf, len, 0);
};
int SockCli::_recv(char* buf, int len) {
  return read(client_sock, buf, len);
};

void SockServ::initSocket(std::string& port) {
  int opt = 1;
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(1);
  }
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(1);
  }
  int yes = 1;
  if (setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes))) {
    perror("util/common/TCPServer SetSocketOption");
    exit(EXIT_FAILURE);
  }
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(std::stoi(port));

  // bind to port
  if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind failed");
    exit(1);
  }
  // listen on port, initialization done
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(1);
  }
}

SockServ::SockServ(std::string port) {
  this->initSocket(port);
};

SockServ::SockServ() {
  std::string fakePort = "0";
  this->initSocket(fakePort);
}

std::string SockServ::getListenPort() {
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);
  if (getsockname(server_fd, (struct sockaddr *)&sin, &len) == -1)
      perror("getsockname");
  return std::to_string(ntohs(sin.sin_port));
}

int SockServ::acceptCli() {
  int addrlen = sizeof(serv_addr);
  if ((new_cli = accept(server_fd, (struct sockaddr*)&serv_addr,
                        (socklen_t*)&addrlen)) < 0) {
    perror("acceptCli");
    exit(1);
  }
  return new_cli;
};

int SockServ::_send(const char* buf, int len) {
  return send(new_cli, buf, len, 0);
};

int SockServ::_recv(char* buf, int len) {
  return read(new_cli, buf, len);
};

};  // namespace trans
