#include "tcp_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <utility>

#include "channel.h"
#include "event_loop.h"

namespace epoll_proj {

bool TcpClient::set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

TcpClient::TcpClient(EventLoop* loop, std::string host, uint16_t port)
    : loop_(loop), host_(std::move(host)), port_(port) {
    // 一次性解析地址，后续重连复用。失败直接抛 / 退出有些重，这里只是把 sin_addr 填 0；
    // start_connect 时 inet_pton 已经检查过格式，构造时就不再校验，避免在构造里抛异常。
    std::memset(&server_addr_, 0, sizeof(server_addr_));
    server_addr_.sin_family = AF_INET;
    server_addr_.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &server_addr_.sin_addr) != 1) {
        // 不抛：把 sin_addr 留 0，start_connect 时 connect(2) 会失败 → 走重试路径。
        // 用户能从 stderr 看到错误，但进程不会因为传错地址直接 crash。
        std::cerr << "[tcp_client] invalid host: " << host_ << std::endl;
    }
}

TcpClient::~TcpClient() {
    // 析构必须在 loop 线程，否则 disconnect_in_loop 里碰 Connection 会跨线程
    loop_->assert_in_loop_thread();

    // 防御性兜底：用户没显式 disconnect 就析构时，本地资源能关多少关多少。
    //   - connect_channel_：unique_ptr 自动析构会 remove epoll 注册
    //   - connecting_fd_：connect 中途的 fd，自己 close
    //   - connection_：调它的 close()，让 close_cb 走完正常清理流程
    retry_enabled_ = false;
    if (connection_) {
        // 注意：connection_->close() 会回调到 on_connection_closed，
        //   里面会再次清 connection_、调 schedule_retry。
        //   schedule_retry 看到 retry_enabled_=false 会回到 kDisconnected，不挂定时器。
        connection_->close();
        connection_.reset();
    }
    if (connect_channel_) {
        connect_channel_->disable_all();
        connect_channel_->remove();
        connect_channel_.reset();
    }
    if (connecting_fd_ >= 0) {
        ::close(connecting_fd_);
        connecting_fd_ = -1;
    }
}

void TcpClient::connect() {
    loop_->run_in_loop([this]() { connect_in_loop(); });
}

void TcpClient::disconnect() {
    loop_->run_in_loop([this]() { disconnect_in_loop(); });
}

void TcpClient::connect_in_loop() {
    loop_->assert_in_loop_thread();

    // 幂等：已经在连或已连上时直接忽略
    if (state_ == State::kConnecting || state_ == State::kConnected) return;

    retry_enabled_ = true;
    backoff_ms_    = kInitialBackoffMs;
    start_connect();
}

void TcpClient::disconnect_in_loop() {
    loop_->assert_in_loop_thread();

    retry_enabled_ = false;

    if (state_ == State::kConnected && connection_) {
        // 走 Connection 自己的 close → 触发 close_cb → on_connection_closed
        // → 看到 retry_enabled_=false → state 转 kDisconnected
        connection_->close();
        return;
    }

    if (state_ == State::kConnecting) {
        // 握手中：拆掉临时 channel 和 fd，状态回到 kDisconnected
        if (connect_channel_) {
            connect_channel_->disable_all();
            connect_channel_->remove();
            connect_channel_.reset();
        }
        if (connecting_fd_ >= 0) {
            ::close(connecting_fd_);
            connecting_fd_ = -1;
        }
        state_ = State::kDisconnected;
        return;
    }

    // kRetrying：定时器还会触发一次 start_connect()，但那时会看到 retry_enabled_=false 直接退出
    // 这种"留个空 callback 让它自然过期"换简洁，不主动 cancel_timer。
    if (state_ == State::kRetrying) {
        state_ = State::kDisconnected;
    }
}

void TcpClient::start_connect() {
    loop_->assert_in_loop_thread();

    if (!retry_enabled_) {
        // disconnect 在重试定时器期间被调用了，到点也不要再拨号
        state_ = State::kDisconnected;
        return;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "[tcp_client] socket failed: " << std::strerror(errno) << std::endl;
        schedule_retry();
        return;
    }
    if (!set_nonblocking(fd)) {
        std::cerr << "[tcp_client] set_nonblocking failed" << std::endl;
        ::close(fd);
        schedule_retry();
        return;
    }

    int ret = ::connect(fd, reinterpret_cast<const sockaddr*>(&server_addr_),
                        sizeof(server_addr_));
    if (ret == 0) {
        // 立刻成功（同机 loopback 偶发）。直接走完整握手成功流程。
        connecting_fd_ = fd;
        state_ = State::kConnecting;
        // 复用 handle_connect_writable 的成功分支：getsockopt 也会回 0
        // 不直接 inline 是想让"握手成功"的路径只出现一份
        handle_connect_writable();
        return;
    }

    if (errno != EINPROGRESS) {
        // 立刻失败：ECONNREFUSED（server 没起）、ENETUNREACH 等
        std::cerr << "[tcp_client] connect to " << host_ << ":" << port_
                  << " failed immediately: " << std::strerror(errno) << std::endl;
        ::close(fd);
        schedule_retry();
        return;
    }

    // EINPROGRESS：进入异步握手。挂个临时 channel 等 EPOLLOUT
    connecting_fd_ = fd;
    state_ = State::kConnecting;

    connect_channel_ = std::make_unique<Channel>(loop_, fd);
    // 注意：握手期间只关注 EPOLLOUT。
    //   - 成功：内核报告可写
    //   - 失败：同样报告可写（用 getsockopt(SO_ERROR) 取真正错误码）
    // EPOLLERR / EPOLLHUP 也可能来，我们都当"握手出结果"处理；
    // 这两个事件靠 epoll 自动上报，不用显式注册。
    connect_channel_->set_write_cb([this]() { handle_connect_writable(); });
    connect_channel_->set_error_cb([this]() { handle_connect_writable(); });
    connect_channel_->set_close_cb([this]() { handle_connect_writable(); });
    connect_channel_->enable_writing();
}

void TcpClient::handle_connect_writable() {
    loop_->assert_in_loop_thread();

    // 取 fd 之后立刻把临时 channel 拆掉。无论成功失败，channel 都不能继续挂着，
    // 否则后面 Connection 会用同一个 fd 再注册一个 channel → 双重注册到 epoll
    int fd = connecting_fd_;
    connecting_fd_ = -1;

    if (connect_channel_) {
        connect_channel_->disable_all();
        connect_channel_->remove();
        connect_channel_.reset();
    }

    if (fd < 0) {
        // 异常路径兜底：理论上不可能
        schedule_retry();
        return;
    }

    // getsockopt 取真实结果。失败时 err != 0
    int       err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        err = errno;
    }
    if (err != 0) {
        std::cerr << "[tcp_client] connect to " << host_ << ":" << port_
                  << " failed: " << std::strerror(err) << std::endl;
        ::close(fd);
        schedule_retry();
        return;
    }

    // 握手成功 —— 构造 Connection 接管 fd
    sockaddr_in peer = server_addr_;   // 业务里 peer 就是我们要连的对端
    auto conn = std::make_shared<Connection>(loop_, fd, peer);

    // 挂回调：与 TcpServer 侧保持一致
    if (message_cb_) conn->set_message_callback(message_cb_);
    // close_cb：Connection 死的时候通知我们，统一走 on_connection_closed
    //   - 不跨线程派任务：TcpClient 和 Connection 在同一个 loop 上，直接同步即可
    //   - 捕获 this 安全：TcpClient 寿命覆盖 Connection（析构里 close 了 connection_）
    conn->set_close_callback([this](const ConnectionPtr& c) {
        on_connection_closed(c);
    });

    state_ = State::kConnected;
    backoff_ms_  = kInitialBackoffMs;    // 复位退避
    connection_  = conn;

    // 先通知业务"连上了"（这时业务可以挂 idle timer / 初始化 context），
    // 再 start() —— 注册到 epoll、关注读事件。
    // 顺序原因和 TcpServer::handle_accept 一致：start() 之后才允许第一次 EPOLLIN
    if (connection_cb_) connection_cb_(conn);
    conn->start();
}

void TcpClient::on_connection_closed(const ConnectionPtr& conn) {
    loop_->assert_in_loop_thread();

    // 通知业务"断了"。此时 conn->state() == kDisconnected
    if (connection_cb_) connection_cb_(conn);

    // 清掉我们持有的 ConnectionPtr。
    // 一旦清掉，Connection 会被业务持有的 functor / 上下文延迟到他们也释放后再析构
    //   —— 即"安全析构"语义，与 TcpServer 侧 connections_.erase 等价
    connection_.reset();

    schedule_retry();
}

void TcpClient::schedule_retry() {
    loop_->assert_in_loop_thread();

    if (!retry_enabled_) {
        state_ = State::kDisconnected;
        return;
    }

    if (backoff_ms_ == 0) backoff_ms_ = kInitialBackoffMs;

    state_ = State::kRetrying;
    const int64_t this_wait = backoff_ms_;

    std::cerr << "[tcp_client] will retry " << host_ << ":" << port_
              << " in " << this_wait << " ms" << std::endl;

    loop_->run_after(this_wait, [this]() {
        // 到点 → 重试：start_connect 内部会再检查 retry_enabled_
        // 不在 lambda 里捕获 weak_this 是因为 TcpClient 析构会把 retry_enabled_ 关掉、
        //   并把 connection_ / connect_channel_ 拆干净 —— 但定时器到点时如果 TcpClient
        //   已经析构了，this 是悬空的。教学项目约定：TcpClient 生命周期必须长于 loop，
        //   或在析构前先 disconnect 并保证不再 run_after。
        if (state_ != State::kRetrying) return;   // 期间被 disconnect 改了状态
        start_connect();
    });

    // 翻倍到上限
    backoff_ms_ = std::min(backoff_ms_ * 2, kMaxBackoffMs);
}

}  // namespace epoll_proj
