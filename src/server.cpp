#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace epoll_proj {

namespace {

constexpr int kBacklog = 128;
constexpr int kMaxEvents = 64;

[[noreturn]] void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

bool set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool epoll_add(int epfd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return ::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == 0;
}

}  // namespace

TcpServer::TcpServer(uint16_t port) : port_(port) {
    create_listen_socket();

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");

    // listen_fd: 监听可读 + 边沿触发，与循环 accept 直到 EAGAIN 配合
    if (!epoll_add(epfd_, listen_fd_, EPOLLIN | EPOLLET)) {
        die("epoll_ctl(listen_fd)");
    }
}

TcpServer::~TcpServer() {
    // unique_ptr 会自动析构所有 Connection（其析构里 close fd）
    connections_.clear();
    if (listen_fd_ >= 0) ::close(listen_fd_);
    if (epfd_ >= 0) ::close(epfd_);
}

void TcpServer::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");

    int opt = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port_);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        die("bind");
    }
    if (::listen(fd, kBacklog) < 0) die("listen");
    if (!set_nonblocking(fd)) die("fcntl(listen_fd, O_NONBLOCK)");

    listen_fd_ = fd;
}

void TcpServer::handle_accept() {
    // ET 模式：必须循环 accept 直到 EAGAIN
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = ::accept(listen_fd_,
                               reinterpret_cast<sockaddr*>(&client_addr),
                               &client_len);
        if (conn_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            std::cerr << "[server] accept failed: " << std::strerror(errno) << std::endl;
            break;
        }

        if (!set_nonblocking(conn_fd)) {
            std::cerr << "[server] set_nonblocking failed for fd=" << conn_fd << std::endl;
            ::close(conn_fd);
            continue;
        }

        if (!epoll_add(epfd_, conn_fd, EPOLLIN | EPOLLET)) {
            std::cerr << "[server] epoll_ctl ADD failed for fd=" << conn_fd
                      << ": " << std::strerror(errno) << std::endl;
            ::close(conn_fd);
            continue;
        }

        // 创建 Connection 并注入回调
        auto conn = std::make_unique<Connection>(conn_fd, client_addr);
        if (message_cb_) conn->set_message_callback(message_cb_);
        conn->set_close_callback([this](Connection& c) { remove_connection(c); });

        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << conn->peer()
                  << ", total=" << (connections_.size() + 1) << std::endl;

        connections_.emplace(conn_fd, std::move(conn));
    }
}

void TcpServer::remove_connection(Connection& conn) {
    int fd = conn.fd();  // 注意：close() 内部已经把 fd_ 置 -1，此处不能再用
    // 由于 Connection::close 已经把 fd_ 置成 -1，先在 close 之前我们记不到真实 fd。
    // 解决：在 close_callback 里通过 connections_ 反查也可以，但更简单的做法是
    // 直接遍历找到这个 Connection 的迭代器。这里我们改用按对象地址查找：
    for (auto it = connections_.begin(); it != connections_.end(); ++it) {
        if (it->second.get() == &conn) {
            int real_fd = it->first;
            ::epoll_ctl(epfd_, EPOLL_CTL_DEL, real_fd, nullptr);
            std::cout << "[server] client closed fd=" << real_fd
                      << " (" << conn.peer() << "), remaining="
                      << (connections_.size() - 1) << std::endl;
            connections_.erase(it);
            return;
        }
    }
    (void)fd;  // 避免未使用警告
}

void TcpServer::run() {
    std::cout << "[server] epoll server listening on 0.0.0.0:" << port_
              << " (max_events=" << kMaxEvents << ")" << std::endl;

    epoll_event events[kMaxEvents];
    while (true) {
        int nready = ::epoll_wait(epfd_, events, kMaxEvents, -1);
        if (nready < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == listen_fd_) {
                if (ev & (EPOLLERR | EPOLLHUP)) {
                    std::cerr << "[server] listen_fd error/hup, exiting." << std::endl;
                    std::exit(EXIT_FAILURE);
                }
                handle_accept();
                continue;
            }

            // 已连接客户端事件
            auto it = connections_.find(fd);
            if (it == connections_.end()) {
                // 可能上一轮已经被移除（理论上不该到这里）
                ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                continue;
            }
            Connection& conn = *it->second;

            if (ev & (EPOLLERR | EPOLLHUP)) {
                conn.close();   // 会回调 remove_connection
                continue;
            }
            if (ev & EPOLLIN) {
                conn.handle_read();
            }
        }
    }
}

}  // namespace epoll_proj
