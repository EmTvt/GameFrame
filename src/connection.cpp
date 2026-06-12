#include "connection.h"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/uio.h>      // readv
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>

namespace epoll_proj {

namespace {

std::string format_peer(const sockaddr_in& addr) {
    char ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}

}  // namespace

Connection::Connection(int fd, const sockaddr_in& peer_addr)
    : fd_(fd),
      peer_(format_peer(peer_addr)),
      state_(State::kConnected),
      events_(EPOLLIN | EPOLLET) {}

Connection::~Connection() {
    // 兜底：仅当从未调用过 close() 时，才在析构里关闭 fd（防泄漏）。
    // 已调用 close() 的情况下，state_ == kDisconnected，fd 已被关闭，这里跳过。
    if (state_ == State::kConnected && fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ---------- 读 ----------
void Connection::handle_read() {
    // 使用 readv 配合栈上额外缓冲，提高一次性读取量、减少系统调用次数
    // 普通做法：用 input_buffer_.begin_write() 直接读，简单清晰，这里采用这种
    while (true) {
        // 至少留 4KB 写入空间
        input_buffer_.ensure_writable(4096);
        ssize_t n = ::read(fd_,
                           input_buffer_.begin_write(),
                           input_buffer_.writable_bytes());
        if (n > 0) {
            input_buffer_.has_written(static_cast<size_t>(n));
            // 不在循环里频繁回调，等读完一轮再统一通知业务
        } else if (n == 0) {
            // 对端关闭：如果 output_buffer 还有未发数据，简化处理为直接放弃
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

    // 通知业务：现在 input_buffer_ 里可能有完整或半完整的消息，让业务自取
    if (message_cb_ && input_buffer_.readable_bytes() > 0) {
        message_cb_(*this, input_buffer_);
    }
}

// ---------- 写 ----------
void Connection::handle_write() {
    // 仅在 output_buffer_ 有数据时才会被关注 EPOLLOUT，所以这里通常 readable > 0
    if (output_buffer_.empty()) {
        disable_write_events();
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
    disable_write_events();
}

void Connection::send(std::string_view data) {
    if (state_ != State::kConnected || data.empty()) return;

    size_t remaining = data.size();
    const char* ptr = data.data();

    // 1) 若没有积压数据，先尝试直接 write 一把（快路径，避免每次都过 buffer）
    if (output_buffer_.empty()) {
        while (remaining > 0) {
            ssize_t n = ::write(fd_, ptr, remaining);
            if (n > 0) {
                ptr += n;
                remaining -= static_cast<size_t>(n);
            } else if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // 内核发送缓冲满了，剩余进 buffer
                    break;
                }
                std::cerr << "[conn " << peer_ << "] send failed: "
                          << std::strerror(errno) << std::endl;
                close();
                return;
            }
        }
    }

    // 2) 还有剩余 → 追加到 output_buffer_，开启 EPOLLOUT 让事件循环帮忙发
    if (remaining > 0) {
        output_buffer_.append(ptr, remaining);
        enable_write_events();
    }
}

// ---------- 关闭 ----------
void Connection::close() {
    if (state_ == State::kDisconnected) return;
    state_ = State::kDisconnected;  // 仅用 state_ 表示 "已关闭"，不动 fd_

    if (fd_ >= 0) {
        ::close(fd_);
        // 注意：这里【保留】fd_ 的值，不置 -1。
        //   1) 让 close_cb_ 里的 Server 能通过 c.fd() 找到自己在 map 里的 key
        //   2) 析构函数根据 state_ 判断要不要再 close，避免重复关闭
        // 风险防护：state_ 已是 kDisconnected，本类不会再对该 fd 做任何 I/O
    }

    if (close_cb_) {
        close_cb_(*this);
    }
}

// ---------- EPOLLOUT 控制 ----------
void Connection::enable_write_events() {
    if (events_ & EPOLLOUT) return;
    events_ |= EPOLLOUT;
    if (update_events_cb_) update_events_cb_(*this, events_);
}

void Connection::disable_write_events() {
    if (!(events_ & EPOLLOUT)) return;
    events_ &= ~EPOLLOUT;
    if (update_events_cb_) update_events_cb_(*this, events_);
}

}  // namespace epoll_proj
