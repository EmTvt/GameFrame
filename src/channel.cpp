#include "channel.h"

#include "event_loop.h"

namespace epoll_proj {

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd) {}

Channel::~Channel() {
    // 析构期不主动调 update/remove —— 析构往往发生在 fd 已 close 之后，
    // 此时再调 epoll_ctl DEL 是无效操作（内核已经清理）。
    // 调用方应在 close fd 之前调 disable_all() + remove()，让状态显式。
}

void Channel::handle_event(uint32_t revents) {
    // 关闭事件优先：EPOLLHUP 且没有 EPOLLIN 通常意味着对端纯关闭
    // 注：EPOLLRDHUP 算"对端写端关闭"，按读路径处理（让 handle_read 自己 read 到 0）
    if ((revents & EPOLLHUP) && !(revents & EPOLLIN)) {
        if (close_cb_) close_cb_();
        return;
    }

    if (revents & EPOLLERR) {
        if (error_cb_) error_cb_();
        // 不 return：错误后通常上层会在 error_cb 里 close，但若没有，
        // 继续走 read/write 也无所谓——它们会拿到错误码自行关闭。
    }

    if (revents & (EPOLLIN | EPOLLRDHUP)) {
        if (read_cb_) read_cb_();
    }

    if (revents & EPOLLOUT) {
        if (write_cb_) write_cb_();
    }
}

void Channel::update() {
    loop_->update_channel(this);
}

void Channel::remove() {
    loop_->remove_channel(this);
}

}  // namespace epoll_proj
