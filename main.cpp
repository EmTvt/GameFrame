// 基于 epoll_wait 的多客户端 TCP 回显服务器
//
// 架构：单线程事件驱动
//   socket -> bind -> listen -> epoll_create1 -> epoll_ctl(listen_fd)
//   主循环：epoll_wait -> 分发事件
//     - listen_fd 可读  => accept 新连接（循环 accept 直到 EAGAIN）
//     - conn_fd 可读    => read 数据 -> 原样写回（echo）
//                          n == 0 表示对端关闭，从 epoll 移除并 close
//
// 关键点：
//   1) listen_fd 与所有 conn_fd 都设为非阻塞（O_NONBLOCK）
//   2) 使用边沿触发（EPOLLET）：必须把当前可读数据一次性读干净，否则后续不再通知
//   3) 多客户端：listen_fd 一个，conn_fd 多个，全部挂在同一个 epoll 实例上
//
// 测试方式（开多个终端模拟多个客户端）：
//   终端1：./build/epoll_proj
//   终端2：telnet 127.0.0.1 8888    # 客户端 A
//   终端3：telnet 127.0.0.1 8888    # 客户端 B
//   两个客户端各自输入文字，服务端各自回显，互不干扰

#include <arpa/inet.h>   // inet_ntop, htons
#include <fcntl.h>       // fcntl, O_NONBLOCK
#include <netinet/in.h>  // sockaddr_in
#include <sys/epoll.h>   // epoll_create1, epoll_ctl, epoll_wait
#include <sys/socket.h>  // socket, bind, listen, accept4
#include <unistd.h>      // read, write, close

#include <cerrno>
#include <cstring>  // strerror
#include <iostream>
#include <unordered_map>

namespace {

constexpr uint16_t kServerPort = 8888;
constexpr int kBacklog = 128;          // listen 队列长度
constexpr int kMaxEvents = 64;         // 单次 epoll_wait 最多取出的事件数
constexpr size_t kBufferSize = 4096;   // 单次读取缓冲区大小

// 简单的错误退出辅助函数
void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

// 把 fd 设为非阻塞模式
bool set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// 把 fd 注册到 epoll 实例上（默认边沿触发 + 监听可读）
bool epoll_add(int epfd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return ::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == 0;
}

// 创建并初始化监听套接字（socket + setsockopt + bind + listen + 非阻塞）
int create_listen_socket(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");

    int opt = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        die("bind");
    }

    if (::listen(fd, kBacklog) < 0) {
        die("listen");
    }

    if (!set_nonblocking(fd)) {
        die("fcntl(listen_fd, O_NONBLOCK)");
    }
    return fd;
}

// 处理 listen_fd 上的可读事件：循环 accept 出所有已就绪连接
// 因为是 ET 模式 + 非阻塞，必须一次取干净，直到 EAGAIN/EWOULDBLOCK
void handle_accept(int listen_fd, int epfd,
                   std::unordered_map<int, std::string>& clients) {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = ::accept(listen_fd,
                               reinterpret_cast<sockaddr*>(&client_addr),
                               &client_len);
        if (conn_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 已就绪的连接全部接收完了
                break;
            }
            if (errno == EINTR) continue;
            std::cerr << "[server] accept failed: " << std::strerror(errno) << std::endl;
            break;
        }

        if (!set_nonblocking(conn_fd)) {
            std::cerr << "[server] set_nonblocking failed for fd=" << conn_fd << std::endl;
            ::close(conn_fd);
            continue;
        }

        // 新连接挂上 epoll，监听可读 + 边沿触发
        if (!epoll_add(epfd, conn_fd, EPOLLIN | EPOLLET)) {
            std::cerr << "[server] epoll_ctl ADD failed for fd=" << conn_fd
                      << ": " << std::strerror(errno) << std::endl;
            ::close(conn_fd);
            continue;
        }

        // 记录客户端信息
        char ip[INET_ADDRSTRLEN] = {0};
        ::inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        std::string peer = std::string(ip) + ":" + std::to_string(ntohs(client_addr.sin_port));
        clients[conn_fd] = peer;
        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << peer
                  << ", total=" << clients.size() << std::endl;
    }
}

// 关闭并清理一个连接
void close_client(int epfd, int conn_fd,
                  std::unordered_map<int, std::string>& clients) {
    // 从 epoll 中移除（close 后内核也会自动清理，但显式更直观）
    ::epoll_ctl(epfd, EPOLL_CTL_DEL, conn_fd, nullptr);
    ::close(conn_fd);
    auto it = clients.find(conn_fd);
    if (it != clients.end()) {
        std::cout << "[server] client closed fd=" << conn_fd
                  << " (" << it->second << "), remaining=" << (clients.size() - 1)
                  << std::endl;
        clients.erase(it);
    }
}

// 处理 conn_fd 上的可读事件：读干净 + 原样回写
// ET 模式要求循环 read 直到 EAGAIN
void handle_read(int epfd, int conn_fd,
                 std::unordered_map<int, std::string>& clients) {
    char buffer[kBufferSize];
    while (true) {
        ssize_t n = ::read(conn_fd, buffer, sizeof(buffer));
        if (n > 0) {
            std::cout << "[server] fd=" << conn_fd
                      << " (" << clients[conn_fd] << ") recv " << n << " bytes: ";
            std::cout.write(buffer, n);
            std::cout.flush();

            // 原样写回：本示例假设发送缓冲区不会阻塞（小数据量回显）
            // 严谨做法：write 返回 EAGAIN 时把剩余数据缓存起来，注册 EPOLLOUT 等可写再继续写
            ssize_t total = 0;
            while (total < n) {
                ssize_t w = ::write(conn_fd, buffer + total, n - total);
                if (w < 0) {
                    if (errno == EINTR) continue;
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // 简化处理：发送缓冲区满则直接丢弃剩余（仅 demo）
                        std::cerr << "[server] write would block, dropping "
                                  << (n - total) << " bytes (demo simplification)"
                                  << std::endl;
                        break;
                    }
                    std::cerr << "[server] write failed: " << std::strerror(errno) << std::endl;
                    close_client(epfd, conn_fd, clients);
                    return;
                }
                total += w;
            }
        } else if (n == 0) {
            // 对端正常关闭
            close_client(epfd, conn_fd, clients);
            return;
        } else {
            // n < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 数据读干净了，正常返回等下一次事件
                return;
            }
            if (errno == EINTR) continue;
            std::cerr << "[server] read failed: " << std::strerror(errno) << std::endl;
            close_client(epfd, conn_fd, clients);
            return;
        }
    }
}

}  // namespace

int main() {
    // 1) 创建监听套接字（已设为非阻塞）
    int listen_fd = create_listen_socket(kServerPort);

    // 2) 创建 epoll 实例
    int epfd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) die("epoll_create1");

    // 3) 把 listen_fd 注册到 epoll：监听"可读"事件（有新连接到来时触发）
    //    listen_fd 用边沿触发，配合循环 accept 直到 EAGAIN
    if (!epoll_add(epfd, listen_fd, EPOLLIN | EPOLLET)) {
        die("epoll_ctl(listen_fd)");
    }

    std::cout << "[server] epoll server listening on 0.0.0.0:" << kServerPort
              << " (max_events=" << kMaxEvents << ")" << std::endl;

    // 4) fd -> 客户端地址描述 的映射，便于日志展示
    std::unordered_map<int, std::string> clients;

    // 5) 主事件循环
    epoll_event events[kMaxEvents];
    while (true) {
        int nready = ::epoll_wait(epfd, events, kMaxEvents, -1 /* 永久阻塞 */);
        if (nready < 0) {
            if (errno == EINTR) continue;  // 被信号打断，重试
            die("epoll_wait");
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            // 异常事件：错误 / 挂起，统一按关闭处理
            if (ev & (EPOLLERR | EPOLLHUP)) {
                if (fd == listen_fd) {
                    std::cerr << "[server] listen_fd error/hup, exiting." << std::endl;
                    std::exit(EXIT_FAILURE);
                }
                close_client(epfd, fd, clients);
                continue;
            }

            if (fd == listen_fd) {
                // 新连接到来
                handle_accept(listen_fd, epfd, clients);
            } else if (ev & EPOLLIN) {
                // 已连接客户端可读
                handle_read(epfd, fd, clients);
            }
        }
    }

    // 正常情况下不会走到这里（上面是死循环）
    ::close(listen_fd);
    ::close(epfd);
    return 0;
}
