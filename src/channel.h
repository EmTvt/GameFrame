// Channel: 一个 fd 在 epoll 视角下的"代言人"
//
// 设计动机：
//   原先 EventLoop 维护 unordered_map<fd, EventCallback>，每次事件查表 + 派发；
//   且事件回调签名是 void(uint32_t events)，由上层（TcpServer / Connection）
//   自己解析事件位，分支逻辑漂在 Server 的 handle_conn_event 里 —— 职责错位。
//
// Channel 把"一个 fd 的关注事件 + 分类回调"打包成对象，带来：
//   1) epoll data.ptr 直接存 Channel*，事件派发零查表（loop 里 handlers_ map 消失）
//   2) 事件回调按 read / write / close / error 分开，分支逻辑回到所属对象
//   3) 关注事件的修改变成对象自己的方法（enable_reading / enable_writing 等），
//      不再需要 Connection → Server → EventLoop 的回调注入链
//   4) 持有 EventLoop*，为未来"Channel 知道自己属于哪个 loop"做铺垫（多 Reactor）
//
// 所有权约定：
//   Channel 不拥有 fd。fd 由 Channel 的持有者（Connection / Acceptor 等）负责 close。
//   Channel 析构时只确保已经从 EventLoop 移除注册，不动 fd。

#pragma once

#include <sys/epoll.h>

#include <cstdint>
#include <functional>

namespace epoll_proj {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    // ---------- 回调注册 ----------
    void set_read_cb (EventCallback cb) { read_cb_  = std::move(cb); }
    void set_write_cb(EventCallback cb) { write_cb_ = std::move(cb); }
    void set_close_cb(EventCallback cb) { close_cb_ = std::move(cb); }
    void set_error_cb(EventCallback cb) { error_cb_ = std::move(cb); }

    // ---------- 关注事件控制 ----------
    // kReadEvent = EPOLLIN | EPOLLRDHUP，识别"对端半关闭"用得上
    void enable_reading()  { events_ |=  kReadEvent;  update(); }
    void disable_reading() { events_ &= ~kReadEvent;  update(); }
    void enable_writing()  { events_ |=  EPOLLOUT;    update(); }
    void disable_writing() { events_ &= ~EPOLLOUT;    update(); }
    void disable_all()     { events_ = 0;             update(); }

    bool is_reading() const { return events_ & kReadEvent; }
    bool is_writing() const { return events_ & EPOLLOUT; }

    // 从 EventLoop 中彻底注销本 Channel（DEL）
    // 调用方语义：本 channel 即将销毁、或对应 fd 即将 close 时调用一次
    void remove();

    // ---------- 给 EventLoop 用的 ----------
    int fd() const { return fd_; }
    uint32_t events() const { return events_; }

    // epoll_wait 拿到 revents 后调本函数 —— 按事件位分发到 read/write/close/error
    // 注意：close 优先级最高（对端关闭后再 read 才有意义；EPOLLHUP 单独到来时 read 也应该结束）
    void handle_event(uint32_t revents);

    // epoll 注册状态机：避免对同一 fd EPOLL_CTL_ADD 两次（内核会 EEXIST）
    // 由 EventLoop::update_channel 维护
    enum State { kNew, kAdded, kDeleted };
    State state() const { return state_; }
    void  set_state(State s) { state_ = s; }

private:
    void update();   // 转发到 loop_->update_channel(this)

    static constexpr uint32_t kReadEvent = EPOLLIN | EPOLLRDHUP;

    EventLoop* loop_;
    int        fd_;
    uint32_t   events_{0};
    State      state_{kNew};

    EventCallback read_cb_;
    EventCallback write_cb_;
    EventCallback close_cb_;
    EventCallback error_cb_;
};

}  // namespace epoll_proj
