#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "channel.h"

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

    // listen_fd 走 Channel 体系：accept 时只关心可读
    // 错误/挂断在这里直接退出（教学项目简化处理）
    accept_channel_ = std::make_unique<Channel>(&loop_, listen_fd_);
    accept_channel_->set_read_cb([this]() { handle_accept(); });
    accept_channel_->set_error_cb([]() {
        std::cerr << "[server] listen_fd EPOLLERR, exiting." << std::endl;
        std::exit(EXIT_FAILURE);
    });
    accept_channel_->set_close_cb([]() {
        std::cerr << "[server] listen_fd EPOLLHUP, exiting." << std::endl;
        std::exit(EXIT_FAILURE);
    });
    accept_channel_->enable_reading();
}

TcpServer::~TcpServer() {
    // 顺序：先清空 connections_（Connection 析构会摘 channel + close fd），
    // 再摘 accept_channel_，最后由 loop_ 析构关 epfd / wakeup_fd。
    connections_.clear();

    if (accept_channel_) {
        accept_channel_->disable_all();
        accept_channel_->remove();
        accept_channel_.reset();
    }
    if (listen_fd_ >= 0) ::close(listen_fd_);
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

        auto conn = std::make_unique<Connection>(&loop_, conn_fd, client_addr);
        if (message_cb_) conn->set_message_callback(message_cb_);
        // close 回调：Connection::close() 调用时 fd_ 仍然保留（不会被置为 -1），
        // 所以这里直接用 c.fd() 即可。
        conn->set_close_callback([this](Connection& c) {
            remove_connection(c.fd());
        });

        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << conn->peer()
                  << ", total=" << (connections_.size() + 1) << std::endl;

        // 先放进表，再 start() —— 否则万一 EPOLLIN 立刻触发、close_cb 回调时找不到 self
        Connection* raw = conn.get();
        connections_.emplace(conn_fd, std::move(conn));
        raw->start();  // 把 channel enable_reading，正式开始收事件
    }
}

void TcpServer::remove_connection(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    // Channel 的 epoll 注销 + fd 的 ::close 都已经在 Connection::close() 里完成
    // 这里只负责把表项移除（unique_ptr 析构走\"已 close\"分支，不会重复 close）
    std::cout << "[server] client closed fd=" << fd
              << " (" << it->second->peer() << "), remaining="
              << (connections_.size() - 1) << std::endl;
    connections_.erase(it);
}

void TcpServer::run() {
    std::cout << "[server] epoll server listening on 0.0.0.0:" << port_ << std::endl;
    loop_.loop();
}

}  // namespace epoll_proj
