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
#include "event_loop_thread_pool.h"

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

TcpServer::TcpServer(uint16_t port, size_t num_threads)
    : port_(port),
      thread_pool_(std::make_unique<EventLoopThreadPool>(&loop_, num_threads)) {
    create_listen_socket();

    // listen_fd 走 Channel 体系：accept 时只关心可读
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
    // 析构顺序很关键：
    // 1) 先把所有 Connection 显式关掉 —— 此时各 subLoop 还活着，channel->remove() 能正常走
    //    （连接的 I/O 操作都在 subLoop 线程，所以这里要 run_in_loop 派过去）
    for (auto& [fd, conn] : connections_) {
        EventLoop* sub_loop = conn->loop();
        Connection* raw = conn.get();
        sub_loop->run_in_loop([raw]() { raw->close(); });
    }
    // 2) 销毁 thread_pool_ —— EventLoopThread 析构会让各 subLoop quit + join
    //    Connection 持有的 Channel 也在 subLoop 退出前安全摘干净
    thread_pool_.reset();
    // 3) 现在所有 subLoop 已经退出，connections_ 可以安全析构
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
    // 本函数永远在 mainLoop 线程
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

        // round-robin 选 subLoop（num_threads=0 时回退到 mainLoop，行为不变）
        EventLoop* sub_loop = thread_pool_->next_loop();

        auto conn = std::make_unique<Connection>(sub_loop, conn_fd, client_addr);
        if (message_cb_) conn->set_message_callback(message_cb_);

        // close_cb 关键点：Connection::close() 在 subLoop 线程里被触发，
        //   但 connections_ 这张表只允许 mainLoop 访问 —— 必须派回主线程
        // 注意 fd 复用陷阱：旧连接 fd 关闭后，新连接可能立刻拿到同一个 fd。
        //   所以这里同时捕获 Connection*，remove 时按地址校验是不是同一个对象，
        //   防止把新连接误删。教学项目里这是替代 shared_ptr<Connection> 的简化手段。
        EventLoop* main_loop = &loop_;
        Connection* self = conn.get();
        conn->set_close_callback([main_loop, this, self](Connection& c) {
            int fd = c.fd();
            main_loop->run_in_loop([this, fd, self]() {
                remove_connection_in_loop(fd, self);
            });
        });

        std::cout << "[server] new client fd=" << conn_fd
                  << " from " << conn->peer()
                  << " → loop@" << sub_loop
                  << ", total=" << (connections_.size() + 1) << std::endl;

        // fd 复用兜底：旧连接关闭路径（close_cb → run_in_loop → remove_in_loop）
        //   可能还在 mainLoop 的 pending_functors_ 里没执行；这时 fd 已经被内核回收，
        //   新 accept 又拿到同一个 fd → map 里 key 撞车。
        // 此时旧 conn 一定已经 kDisconnected（subLoop 那边的 close() 已经把状态改了），
        //   直接覆盖（unique_ptr 析构走"已 close"分支，不再操作 channel/fd）是安全的。
        // 但 close_cb 已经按"旧 self 指针"匹配，所以旧的那次 remove_in_loop 进来时
        //   会发现 self != map[fd].get()，自动跳过 erase，不会误删新连接。
        Connection* raw = conn.get();
        connections_[conn_fd] = std::move(conn);

        sub_loop->run_in_loop([raw]() {
            // 在 subLoop 线程里调 enable_reading —— Channel::update 内部 assert_in_loop_thread
            raw->start();
        });
    }
}

void TcpServer::remove_connection_in_loop(int fd, Connection* self) {
    // 永远在 mainLoop 线程执行（由 close_cb 通过 run_in_loop 派过来）
    loop_.assert_in_loop_thread();

    auto it = connections_.find(fd);
    if (it == connections_.end()) return;

    // 关键校验：fd 复用场景下，map[fd] 可能已经是新连接了，不能误删
    if (it->second.get() != self) {
        std::cout << "[server] stale close_cb for fd=" << fd
                  << " (already replaced by a new connection), skip erase." << std::endl;
        return;
    }

    std::cout << "[server] client closed fd=" << fd
              << " (" << it->second->peer() << "), remaining="
              << (connections_.size() - 1) << std::endl;

    // erase 触发 unique_ptr<Connection> 析构
    // 注意：此时 Connection::close() 已经在 subLoop 线程里跑完了
    //   - Channel 已 remove、fd 已 close、state_ == kDisconnected
    //   - 析构函数走"已 close"分支，不会重复操作
    connections_.erase(it);
}

void TcpServer::run() {
    thread_pool_->start();   // 先把 subLoop 线程都启起来
    std::cout << "[server] epoll server listening on 0.0.0.0:" << port_
              << " with " << thread_pool_->num_threads() << " IO thread(s)"
              << std::endl;
    loop_.loop();   // mainLoop 在主线程跑
}

}  // namespace epoll_proj
