#ifndef SOCK_CLI_SERV
#define SOCK_CLI_SERV
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>

namespace trans {
class SockCli {
 public:
  int client_sock;

  SockCli();
  SockCli(std::string ip, int port);
  SockCli(const SockCli& s);
  int _send(const char* buf, int len);
  int _recv(char* buf, int len);
};

class SockServ {
  void initSocket(int port);
  int server_fd = 0;
  struct sockaddr_in serv_addr;
  int new_cli;

 public:
  SockServ(int port);
  SockServ();

  int getListenPort();
  int acceptCli();

  int _send(const char* buf, int len);

  int _recv(char* buf, int len);
};

};  // namespace trans

#endif /* SOCK_CLI_SERV */