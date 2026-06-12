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

bool epoll_mod(int epfd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    return ::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0;
}

}  // namespace

TcpServer::TcpServer(uint16_t port) : port_(port) {
    create_listen_socket();

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");

    if (!epoll_add(epfd_, listen_fd_, EPOLLIN | EPOLLET)) {
        die("epoll_ctl(listen_fd)");
    }
}

TcpServer::~TcpServer() {
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

        auto conn = std::make_unique<Connection>(conn_fd, client_addr);
        if (message_cb_) conn->set_message_callback(message_cb_);
        // close 回调：Connection::close() 调用时 fd_ 仍然保留（不会被置为 -1），
        // 所以这里直接用 c.fd() 即可，语义比之前"捕获 conn_fd"更直观。
        conn->set_close_callback([this](Connection& c) {
            remove_connection(c.fd());
        });
        // 事件更新回调：Connection 想开/关 EPOLLOUT 时通过它通知 Server
        conn->set_update_events_callback([this](Connection& c, uint32_t events) {
            update_events(c.fd(), events);
        });

        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << conn->peer()
                  << ", total=" << (connections_.size() + 1) << std::endl;

        connections_.emplace(conn_fd, std::move(conn));
    }
}

void TcpServer::remove_connection(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    std::cout << "[server] client closed fd=" << fd
              << " (" << it->second->peer() << "), remaining="
              << (connections_.size() - 1) << std::endl;
    connections_.erase(it);
}

void TcpServer::update_events(int fd, uint32_t events) {
    if (!epoll_mod(epfd_, fd, events)) {
        std::cerr << "[server] epoll_ctl MOD failed for fd=" << fd
                  << ": " << std::strerror(errno) << std::endl;
    }
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

            auto it = connections_.find(fd);
            if (it == connections_.end()) {
                ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
                continue;
            }
            Connection& conn = *it->second;

            // 错误优先：直接关闭
            if (ev & (EPOLLERR | EPOLLHUP)) {
                conn.close();
                continue;
            }
            // 可读
            if (ev & EPOLLIN) {
                conn.handle_read();
                // 业务回调中可能已经 close()，再次确认
                if (!connections_.count(fd)) continue;
            }
            // 可写
            if (ev & EPOLLOUT) {
                conn.handle_write();
            }
        }
    }
}

}  // namespace epoll_proj
