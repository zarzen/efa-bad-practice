#include "helper_socket.h"
#include "spdlog/spdlog.h"

int main() {
  trans::SockServ serv;
  spdlog::info("serv port {}", serv.getListenPort());
}