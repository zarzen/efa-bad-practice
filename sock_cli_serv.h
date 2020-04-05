#ifndef SOCK_CLI_SERV
#define SOCK_CLI_SERV
#include <arpa/inet.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace trans {
class SockCli {
public:
  int client_sock;

  SockCli();
  SockCli(std::string ip, std::string port);
  SockCli(const SockCli& s);
  int _send(char *buf, int len);
  int _recv(char *buf, int len);
};

class SockServ {
public:
  int server_fd = 0;
  struct sockaddr_in serv_addr;
  int new_cli;

  SockServ(std::string port);

  int _listen();

  int _send(char *buf, int len);

  int _recv(char *buf, int len);
};

}; // namespace trans

#endif /* SOCK_CLI_SERV */