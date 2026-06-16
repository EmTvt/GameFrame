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

    channel_->tie(shared_from_this());
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
        // 拷贝一份 callback 再调用：handler 内部可能调 set_message_callback 改自己
        //   （协议切换、握手完成切到正式协议等），直接通过成员调用会析构正在运行的 lambda → UB
        auto cb = message_cb_;
        cb(shared_from_this(), input_buffer_);
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

    // 通知"buffer 已排空"。注意进到这里就一定意味着 readable_bytes() == 0：
    //   - 入口若 buffer 为空已经 return 了
    //   - while 循环只有把 buffer 写干净才会自然退出（出错/EAGAIN 都在内部 return 掉了）
    // 因此 write_complete 是这条函数自然终点的事件，无需再次判断空
    // —— 同样走 queue_in_loop，让业务回调里能安全地再 send / close
    if (write_complete_cb_) {
        loop_->queue_in_loop(
            [self = shared_from_this(), cb = write_complete_cb_]() { cb(self); });
    }
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
        // 在 append 前后取一次 readable_bytes，做"跨高水位"边沿检测：
        //   old_len < mark && new_len >= mark  → 这一次 send 把 buffer 推过了红线
        // 只在跨越的瞬间触发一次回调；后续 send 哪怕 buffer 持续高位也不会重复打扰业务
        // —— 业务自己要做的事就是"过载时该停就停"，水位降回 0 时由 write_complete 通知恢复
        const size_t old_len = output_buffer_.readable_bytes();
        output_buffer_.append(ptr, remaining);
        const size_t new_len = output_buffer_.readable_bytes();

        if (high_water_mark_ > 0 && high_water_cb_ &&
            old_len < high_water_mark_ && new_len >= high_water_mark_) {
            // 异步派发到 loop：避免业务回调里再调 send/close 造成状态机递归
            // 用 shared_from_this 钉住生命周期，回调真正执行时 Connection 一定还在
            loop_->queue_in_loop(
                [self = shared_from_this(), cb = high_water_cb_]() { cb(self); });
        }

        if (!channel_->is_writing()) {
            channel_->enable_writing();
        }
    }
}

// ---------- 关闭 ----------
void Connection::close() {
    if (state_ == State::kDisconnected) return;
    state_ = State::kDisconnected;

    // 关键 trick：先抓一份 shared_from_this，把"自己"的生命周期钉住到本函数结束。
    //   close_cb_ 一旦触发，TcpServer 会派任务把 connections_[fd] erase 掉，
    //   而 map 持有的就是最后一份 shared_ptr —— 如果没有 guard，erase 后我们就在自己的
    //   析构里继续跑下面的代码，自我 UAF。
    //   有了 guard，连接在 close() 退出之前不会被析构（即使 map 里那份已经没了）。
    auto guard = shared_from_this();

    // 先把 channel 从 epoll 摘掉，再 close fd —— 避免 fd 关掉后 epoll 还可能投递事件
    // （内核会自动清理，但显式 remove 让 Channel 状态机和 epoll 状态保持一致）
    if (channel_) {
        channel_->disable_all();
        channel_->remove();
    }

    if (fd_ >= 0) {
        ::close(fd_);
        // 保留 fd_ 值：业务回调（close_cb_）需要通过 c->fd() 找到自己在 map 里的 key
    }

    if (close_cb_) {
        close_cb_(guard);
    }
}

}  // namespace epoll_proj
