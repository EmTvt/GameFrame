#include "channel.h"

#include "event_loop.h"

namespace epoll_proj {

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd) {}

Channel::~Channel() {
    // 析构期不主动调 update/remove —— 析构往往发生在 fd 已 close 之后，
    // 此时再调 epoll_ctl DEL 是无效操作（内核已经清理）。
    // 调用方应在 close fd 之前调 disable_all() + remove()，让状态显式。
}

void Channel::tie(const std::shared_ptr<void>& obj) {
    tie_  = obj;
    tied_ = true;
}

void Channel::handle_event(uint32_t revents) {
    if (tied_) {
        // 升级 weak_ptr → shared_ptr：拿到表示对象还活着，本次派发期间它不会被析构
        //   （除了我们手里这份 guard，map / 其他 shared_ptr 持有者爱怎样怎样）
        // 没拿到说明对象已经析构（理论上不该发生：Channel 一般跟着 Connection 一起活，
        //   但极端时序下、或将来 Channel 寿命被解耦时，这一层就是兜底）
        if (auto guard = tie_.lock()) {
            handle_event_with_guard(revents);
        }
        // guard 在这里析构，正好是事件派发完毕之后 —— 如果中途有人 close + erase，
        // 真正的对象析构发生在这一刻，栈安全
        return;
    }
    // 没 tie 的 Channel（如 Acceptor、wakeup eventfd）走老路径，零开销
    handle_event_with_guard(revents);
}

void Channel::handle_event_with_guard(uint32_t revents) {
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
