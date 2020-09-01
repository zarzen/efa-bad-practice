#ifndef BALANCE_TCP_H
#define BALANCE_TCP_H

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <memory>
#include <string>

class TcpServer;
class TcpAgent;
class TcpClient;

class TcpServer {
 public:
  TcpServer(std::string address, int port, int listen_num = 4);
  ~TcpServer();
  std::shared_ptr<TcpAgent> tcpAccept();

 private:
  std::string _address;
  int _port;
  int _server_fd;
};

class TcpAgent {
 public:
  TcpAgent(int conn_fd);
  TcpAgent(int conn_fd, sockaddr_in addr);
  ~TcpAgent();

  int tcpSend(const char* data, size_t size);
  int tcpRecv(char* data, size_t size);
  int tcpSendWithLength(const char* data, size_t size);
  int tcpSendWithLength(const std::shared_ptr<char> data, size_t size);
  int tcpRecvWithLength(std::shared_ptr<char>& data, size_t& size);
  int tcpSendString(const std::string data);
  int tcpRecvString(std::string& data);

  std::string getIP();

 protected:
  int _conn_fd;
  struct sockaddr_in _addr;
};

class TcpClient : public TcpAgent {
 public:
  TcpClient(std::string address, int port);
  ~TcpClient();

 private:
  std::string _address;
  int _port;
};

#endif