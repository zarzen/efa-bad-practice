
#include "sock_cli_serv.h"

namespace trans {

SockCli::SockCli(std::string ip, std::string port) {
  struct sockaddr_in serv_addr;
  if ((client_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    return;
  }
  // set up server addr
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::stoi(port));
  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
    printf("\nInvalid address/ Address not supported \n");
    return;
  }
  // connect to server
  if (connect(client_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) <
      0) {
    printf("\nConnection Failed \n");
    return;
  }
};

SockCli::SockCli(){}

SockCli::SockCli(const SockCli& s){
  this->client_sock = s.client_sock;
}

int SockCli::_send(char *buf, int len) {
  return send(client_sock, buf, len, 0);
};
int SockCli::_recv(char *buf, int len) { return read(client_sock, buf, len); };

SockServ::SockServ(std::string port) {
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
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(std::stoi(port));

  // bind to port
  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind failed");
    exit(1);
  }
  // listen on port, initialization done
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(1);
  }
};

int SockServ::_listen() {

  int addrlen = sizeof(serv_addr);
  if ((new_cli = accept(server_fd, (struct sockaddr *)&serv_addr,
                        (socklen_t *)&addrlen)) < 0) {
    perror("accept");
    exit(1);
  }
  return new_cli;
};

int SockServ::_send(char *buf, int len) { return send(new_cli, buf, len, 0); };

int SockServ::_recv(char *buf, int len) { return read(new_cli, buf, len); };

}; // namespace trans
