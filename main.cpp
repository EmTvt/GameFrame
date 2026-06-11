// 单客户端 TCP 回显服务器（阻塞 I/O 版本，未使用 epoll_wait）
//
// 目的：先把服务端的基本骨架跑通：socket -> bind -> listen -> accept -> read/write -> close
// 后续会在此基础上引入 epoll_create1 / epoll_ctl / epoll_wait，扩展为多客户端事件驱动版本。
//
// 测试方式：
//   1. 编译并启动本程序（监听 127.0.0.1:8888）
//   2. 另开一个终端：telnet 127.0.0.1 8888
//   3. 输入任意文字回车，服务端会原样回显

#include <arpa/inet.h>   // inet_ntop, htons
#include <netinet/in.h>  // sockaddr_in
#include <sys/socket.h>  // socket, bind, listen, accept
#include <unistd.h>      // read, write, close

#include <cerrno>
#include <cstring>       // strerror
#include <iostream>

namespace {

constexpr uint16_t kServerPort = 8888;
constexpr int kBacklog = 16;          // listen 队列长度
constexpr size_t kBufferSize = 1024;  // 单次读取缓冲区大小

// 简单的错误退出辅助函数
void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

}  // namespace

int main() {
    // 1) 创建监听 socket：IPv4 + TCP
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        die("socket");
    }

    // 允许地址重用：避免程序重启时 bind 报 "Address already in use"
    int opt = 1;
    if (::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("setsockopt(SO_REUSEADDR)");
    }

    // 2) 绑定本机地址 + 端口
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // 监听本机所有网卡
    server_addr.sin_port = htons(kServerPort);

    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        die("bind");
    }

    // 3) 开始监听
    if (::listen(listen_fd, kBacklog) < 0) {
        die("listen");
    }

    std::cout << "[server] listening on 0.0.0.0:" << kServerPort << " ..." << std::endl;

    // 4) 阻塞等待一个客户端到来
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int conn_fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (conn_fd < 0) {
        die("accept");
    }

    // 打印客户端地址信息
    char client_ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
    std::cout << "[server] client connected: "
              << client_ip << ":" << ntohs(client_addr.sin_port) << std::endl;

    // 5) 循环读取客户端数据并原样回写（echo）
    char buffer[kBufferSize];
    while (true) {
        ssize_t n = ::read(conn_fd, buffer, sizeof(buffer));
        if (n > 0) {
            // 输出到本地终端，方便观察
            std::cout << "[server] recv " << n << " bytes: ";
            std::cout.write(buffer, n);
            std::cout.flush();

            // 原样写回给客户端
            ssize_t total_written = 0;
            while (total_written < n) {
                ssize_t w = ::write(conn_fd, buffer + total_written, n - total_written);
                if (w < 0) {
                    if (errno == EINTR) continue;  // 被信号打断，重试
                    std::cerr << "[server] write failed: " << std::strerror(errno) << std::endl;
                    break;
                }
                total_written += w;
            }
        } else if (n == 0) {
            // 对端正常关闭连接
            std::cout << "[server] client closed the connection." << std::endl;
            break;
        } else {
            // n < 0
            if (errno == EINTR) continue;  // 被信号打断，重试
            std::cerr << "[server] read failed: " << std::strerror(errno) << std::endl;
            break;
        }
    }

    // 6) 清理资源
    ::close(conn_fd);
    ::close(listen_fd);
    std::cout << "[server] bye." << std::endl;
    return 0;
}
