#include "connection.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>

namespace epoll_proj {

namespace {
constexpr size_t kReadBufferSize = 4096;

std::string format_peer(const sockaddr_in& addr) {
    char ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}
}  // namespace

Connection::Connection(int fd, const sockaddr_in& peer_addr)
    : fd_(fd),
      peer_(format_peer(peer_addr)),
      state_(State::kConnected) {}

Connection::~Connection() {
    // 兜底关闭：如果调用方忘了 close()，析构时清理掉 fd
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::handle_read() {
    // ET 模式：必须一次性把内核缓冲区读干净
    char buf[kReadBufferSize];
    while (true) {
        ssize_t n = ::read(fd_, buf, sizeof(buf));
        if (n > 0) {
            // 把这一段数据抛给业务回调处理（业务可能在回调里调用 send 或 close）
            if (message_cb_) {
                message_cb_(*this, std::string_view(buf, static_cast<size_t>(n)));
            }
            // 业务回调中可能已经 close 了，及时退出读循环
            if (state_ == State::kDisconnected) {
                return;
            }
        } else if (n == 0) {
            // 对端正常关闭
            close();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核缓冲区已读干净，正常返回等下次事件
                return;
            }
            if (errno == EINTR) continue;  // 信号打断，重试
            std::cerr << "[conn " << peer_ << "] read failed: "
                      << std::strerror(errno) << std::endl;
            close();
            return;
        }
    }
}

ssize_t Connection::send(std::string_view data) {
    if (state_ != State::kConnected) return 0;

    size_t total = 0;
    while (total < data.size()) {
        ssize_t w = ::write(fd_, data.data() + total, data.size() - total);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // TODO（下一步）：把剩余数据存入写缓冲区 + 注册 EPOLLOUT
                std::cerr << "[conn " << peer_ << "] send would block, dropped "
                          << (data.size() - total) << " bytes (will be fixed in next step)"
                          << std::endl;
                break;
            }
            std::cerr << "[conn " << peer_ << "] send failed: "
                      << std::strerror(errno) << std::endl;
            close();
            break;
        }
        total += static_cast<size_t>(w);
    }
    return static_cast<ssize_t>(total);
}

void Connection::close() {
    if (state_ == State::kDisconnected) return;
    state_ = State::kDisconnected;

    if (fd_ >= 0) {
        ::close(fd_);
        // 注意：这里只置 -1，是因为析构里会再次检查；epoll 的移除由 Server 负责
        fd_ = -1;
    }

    // 通知上层 Server 把自己从连接表 / epoll 里摘除
    if (close_cb_) {
        close_cb_(*this);
    }
}

}  // namespace epoll_proj
