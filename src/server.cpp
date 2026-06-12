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

[[noreturn]] void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

bool set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

TcpServer::TcpServer(uint16_t port) : port_(port) {
    create_listen_socket();

    // 把 listen_fd 挂到 EventLoop 上：来事件就调 handle_accept
    // 事件位掩码这里其实用不上（listen socket 只关心 EPOLLIN），但保留参数让回调签名统一
    loop_.add_fd(listen_fd_, EPOLLIN | EPOLLET, [this](uint32_t ev) {
        if (ev & (EPOLLERR | EPOLLHUP)) {
            std::cerr << "[server] listen_fd error/hup, exiting." << std::endl;
            std::exit(EXIT_FAILURE);
        }
        handle_accept();
    });
}

TcpServer::~TcpServer() {
    // 先清空 connections_，Connection 析构会 ::close(fd)；
    // 此时 loop_ 还活着、loop_.del_fd 也能正常工作，但我们不在 Connection 析构里调它 ——
    //   Connection 不感知 EventLoop 的存在（保持对外接口干净）。
    // 由于 epfd 即将被 loop_ 析构关掉，残留的 fd 关注关系会随 epfd 一起释放，没有泄漏。
    connections_.clear();
    if (listen_fd_ >= 0) ::close(listen_fd_);
    // epfd_ 由 loop_ 自己负责，不需要这里关
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

        auto conn = std::make_unique<Connection>(conn_fd, client_addr);
        if (message_cb_) conn->set_message_callback(message_cb_);
        // close 回调：Connection::close() 调用时 fd_ 仍然保留（不会被置为 -1），
        // 所以这里直接用 c.fd() 即可。
        conn->set_close_callback([this](Connection& c) {
            remove_connection(c.fd());
        });
        // 事件更新回调：Connection 想开/关 EPOLLOUT 时通过它通知 Server
        conn->set_update_events_callback([this](Connection& c, uint32_t events) {
            update_events(c.fd(), events);
        });

        // 注册到事件循环
        loop_.add_fd(conn_fd, EPOLLIN | EPOLLET, [this, conn_fd](uint32_t ev) {
            handle_conn_event(conn_fd, ev);
        });

        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << conn->peer()
                  << ", total=" << (connections_.size() + 1) << std::endl;

        connections_.emplace(conn_fd, std::move(conn));
    }
}

void TcpServer::handle_conn_event(int fd, uint32_t ev) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;  // 防御：连接可能已被关闭
    Connection& conn = *it->second;

    // 错误优先：直接关闭
    if (ev & (EPOLLERR | EPOLLHUP)) {
        conn.close();
        return;
    }
    // 可读
    if (ev & EPOLLIN) {
        conn.handle_read();
        // resource-safety.md 第 4 节：业务回调可能已经 close()，再次确认
        // 注意：本检查必须保留 —— 否则下面 handle_write 是 UAF
        if (!connections_.count(fd)) return;
    }
    // 可写
    if (ev & EPOLLOUT) {
        conn.handle_write();
    }
}

void TcpServer::remove_connection(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    // 先从 epoll 注销，再 erase（unique_ptr 析构会 close fd）
    loop_.del_fd(fd);
    std::cout << "[server] client closed fd=" << fd
              << " (" << it->second->peer() << "), remaining="
              << (connections_.size() - 1) << std::endl;
    connections_.erase(it);
}

void TcpServer::update_events(int fd, uint32_t events) {
    loop_.mod_fd(fd, events);
}

void TcpServer::run() {
    std::cout << "[server] epoll server listening on 0.0.0.0:" << port_ << std::endl;
    loop_.loop();
}

}  // namespace epoll_proj
