#include "connection.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>

#include "channel.h"
#include "event_loop.h"

namespace epoll_proj {

namespace {

std::string format_peer(const sockaddr_in& addr) {
    char ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}

}  // namespace

Connection::Connection(EventLoop* loop, int fd, const sockaddr_in& peer_addr)
    : loop_(loop),
      fd_(fd),
      peer_(format_peer(peer_addr)),
      state_(State::kConnected),
      channel_(std::make_unique<Channel>(loop, fd)) {
    // 把事件分类回调挂到 channel 上 —— 原先在 Server::handle_conn_event 里的事件分支
    // 现在归位到 Connection 自己
    channel_->set_read_cb ([this]() { handle_read(); });
    channel_->set_write_cb([this]() { handle_write(); });
    channel_->set_close_cb([this]() { close(); });
    channel_->set_error_cb([this]() {
        std::cerr << "[conn " << peer_ << "] EPOLLERR" << std::endl;
        close();
    });
}

Connection::~Connection() {
    // 兜底：仅当从未调用过 close() 时，才在析构里关闭 fd（防泄漏）。
    // 已调用 close() 的情况：state_ == kDisconnected，channel 已 remove、fd 已关闭，跳过。
    if (state_ == State::kConnected && fd_ >= 0) {
        // 在 close fd 前先把 channel 从 epoll 摘掉，否则内核虽然会自动清理，
        // 但 Channel 内部 state_ 还停在 kAdded，未来若 Channel 被复用会出错
        if (channel_) {
            channel_->disable_all();
            channel_->remove();
        }
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::start() {
    // 把本连接挂上 epoll：关注读事件
    // 拆分出 start() 是为了让"Connection 构造好 + Server 把它放进 connections_ 表" 之后，
    // 再触发可能的事件 —— 避免 EPOLLIN 来得太快，close_cb 找不到自己。
    loop_->assert_in_loop_thread();
    channel_->enable_reading();
}

// ---------- 读 ----------
void Connection::handle_read() {
    while (true) {
        input_buffer_.ensure_writable(4096);
        ssize_t n = ::read(fd_,
                           input_buffer_.begin_write(),
                           input_buffer_.writable_bytes());
        if (n > 0) {
            input_buffer_.has_written(static_cast<size_t>(n));
        } else if (n == 0) {
            // 对端关闭
            close();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 读干净
            if (errno == EINTR) continue;
            std::cerr << "[conn " << peer_ << "] read failed: "
                      << std::strerror(errno) << std::endl;
            close();
            return;
        }
    }

    if (message_cb_ && input_buffer_.readable_bytes() > 0) {
        message_cb_(*this, input_buffer_);
    }
}

// ---------- 写 ----------
void Connection::handle_write() {
    if (output_buffer_.empty()) {
        // 理论上不会走到这里：只有 buffer 有数据时才关注 EPOLLOUT
        if (channel_->is_writing()) channel_->disable_writing();
        return;
    }

    while (!output_buffer_.empty()) {
        ssize_t n = ::write(fd_, output_buffer_.peek(), output_buffer_.readable_bytes());
        if (n > 0) {
            output_buffer_.retrieve(static_cast<size_t>(n));
        } else if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核发送缓冲又满了，等下次 EPOLLOUT
                return;
            }
            std::cerr << "[conn " << peer_ << "] write failed: "
                      << std::strerror(errno) << std::endl;
            close();
            return;
        }
    }

    // 全部写完了，关掉 EPOLLOUT 避免空转
    if (channel_->is_writing()) channel_->disable_writing();
}

void Connection::send(std::string_view data) {
    if (state_ != State::kConnected || data.empty()) return;

    size_t remaining = data.size();
    const char* ptr = data.data();

    // 快路径：没有积压数据时，直接 write 一把
    if (output_buffer_.empty()) {
        while (remaining > 0) {
            ssize_t n = ::write(fd_, ptr, remaining);
            if (n > 0) {
                ptr += n;
                remaining -= static_cast<size_t>(n);
            } else if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;  // 内核发送缓冲满了，剩余进 buffer
                }
                std::cerr << "[conn " << peer_ << "] send failed: "
                          << std::strerror(errno) << std::endl;
                close();
                return;
            }
        }
    }

    // 还有剩余 → 追加到 output_buffer_，开启 EPOLLOUT
    if (remaining > 0) {
        output_buffer_.append(ptr, remaining);
        if (!channel_->is_writing()) {
            channel_->enable_writing();
        }
    }
}

// ---------- 关闭 ----------
void Connection::close() {
    if (state_ == State::kDisconnected) return;
    state_ = State::kDisconnected;

    // 先把 channel 从 epoll 摘掉，再 close fd —— 避免 fd 关掉后 epoll 还可能投递事件
    // （内核会自动清理，但显式 remove 让 Channel 状态机和 epoll 状态保持一致）
    if (channel_) {
        channel_->disable_all();
        channel_->remove();
    }

    if (fd_ >= 0) {
        ::close(fd_);
        // 保留 fd_ 值：业务回调（close_cb_）需要通过 c.fd() 找到自己在 map 里的 key
    }

    if (close_cb_) {
        close_cb_(*this);
    }
}

}  // namespace epoll_proj
