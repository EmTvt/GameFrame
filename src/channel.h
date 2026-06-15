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
#include <memory>

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

    // ---------- 生命周期绑定 ----------
    // 把 Channel 的事件派发期生命周期"挂钩"到一个外部 shared_ptr 上。
    //
    // 解决的问题：
    //   引入"事件循环之外的主动 close 路径"（定时器、跨线程业务调度等）后，
    //   handle_event 派发链上某个 cb 可能触发 conn->close() → map.erase →
    //   Connection 析构。问题是 handle_event 自己还在 Connection 的成员函数
    //   栈上运行（read_cb_ 等是 lambda 捕获 this 调成员函数）—— 后续分支
    //   （比如 close 后又轮到 write_cb_）就在已死对象上跑了。
    //
    // 解法：让 Channel 持 weak_ptr，事件派发前升级成 shared_ptr，整个派发
    //   过程中对象至少存活到 handle_event 退出。即使中途有人 close + erase，
    //   map 那份 shared_ptr 没了，但栈上 guard 还在，析构推迟到 handle_event
    //   返回之后。
    //
    // 用 weak_ptr<void> 而不是 weak_ptr<Connection>：
    //   Channel 不依赖 Connection 类型（Acceptor 也用 Channel，但不需要 tie，
    //   保持 tied_=false 即可走老路径）。
    void tie(const std::shared_ptr<void>& obj);

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
    // tied_ == true 时由 handle_event 锁住 tie_ 后调用，
    // tied_ == false 时直接调用，行为等价
    void handle_event_with_guard(uint32_t revents);

    static constexpr uint32_t kReadEvent = EPOLLIN | EPOLLRDHUP;

    EventLoop* loop_;
    int        fd_;
    uint32_t   events_{0};
    State      state_{kNew};

    // 生命周期绑定：tied_ 是显式开关，避免每次都去 lock 一个空 weak_ptr
    // （也方便区分"没绑过"和"绑过但对象已死"两种语义）
    std::weak_ptr<void> tie_;
    bool tied_{false};

    EventCallback read_cb_;
    EventCallback write_cb_;
    EventCallback close_cb_;
    EventCallback error_cb_;
};

}  // namespace epoll_proj
