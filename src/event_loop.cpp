#include "event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "channel.h"
#include "timer_queue.h"

namespace epoll_proj {

namespace {

constexpr int kMaxEvents = 64;

[[noreturn]] void die(const char* msg) {
    std::cerr << msg << " failed: " << std::strerror(errno) << std::endl;
    std::exit(EXIT_FAILURE);
}

}  // namespace

EventLoop::EventLoop() {
    // 构造即绑定线程：从这一刻起，本对象只能被当前线程操作
    owner_tid_ = std::this_thread::get_id();

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");

    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) die("eventfd");

    // wakeup_fd_ 也走 Channel 体系，统一对待
    wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
    wakeup_channel_->set_read_cb([this]() { handle_wakeup(); });
    wakeup_channel_->enable_reading();  // → 内部调 update_channel → epoll_ctl ADD

    // TimerQueue 必须在 EventLoop 主体（epfd_ / wakeup_channel_）就绪后再构造，
    // 因为它内部要 new Channel，并立刻 enable_reading 把 timerfd 挂上 epoll
    timer_queue_ = std::make_unique<TimerQueue>(this);
}

EventLoop::~EventLoop() {
    // 析构顺序：先 timer_queue_，再 wakeup_channel_，最后关 fd
    //   TimerQueue 析构会 remove 它的 Channel —— 必须趁 epfd_ 还活着
    timer_queue_.reset();

    // 先把 wakeup_channel_ 从 epoll 注销（在 epfd_ 还活着的时候）
    if (wakeup_channel_) {
        wakeup_channel_->disable_all();
        wakeup_channel_->remove();
        wakeup_channel_.reset();
    }
    if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    if (epfd_ >= 0) ::close(epfd_);
}

// ---------- Channel 注册 ----------

void EventLoop::update_channel(Channel* ch) {
    assert_in_loop_thread();

    epoll_event ev{};
    // 所有 channel 默认 ET 模式：和现有 Connection 行为一致
    // 注意：wakeup_fd_ 也会被加上 ET，但其 read_cb 已经 read 到 EAGAIN（cnt 读完后下次 read 会 EAGAIN）
    //       —— 实际上单次写 1、单次读 8 字节，ET/LT 行为没差别
    ev.events = ch->events() | EPOLLET;
    ev.data.ptr = ch;
    int fd = ch->fd();

    Channel::State st = ch->state();
    if (st == Channel::kNew || st == Channel::kDeleted) {
        // ADD：之前没注册过，或之前被 remove 过现在重新加回来
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::cerr << "[loop] epoll_ctl ADD fd=" << fd << " failed: "
                      << std::strerror(errno) << std::endl;
            return;
        }
        ch->set_state(Channel::kAdded);
    } else {
        // MOD：已经在 epoll 里，改关注事件即可
        if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            std::cerr << "[loop] epoll_ctl MOD fd=" << fd << " failed: "
                      << std::strerror(errno) << std::endl;
        }
    }
}

void EventLoop::remove_channel(Channel* ch) {
    assert_in_loop_thread();

    if (ch->state() != Channel::kAdded) {
        // 没注册过 / 已经 remove 过 —— 幂等
        ch->set_state(Channel::kDeleted);
        return;
    }

    // DEL 失败通常是 fd 已被 ::close（内核会自动从 epoll 移除），不视为错误
    if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, ch->fd(), nullptr) < 0
        && errno != EBADF && errno != ENOENT) {
        std::cerr << "[loop] epoll_ctl DEL fd=" << ch->fd() << " failed: "
                  << std::strerror(errno) << std::endl;
    }
    ch->set_state(Channel::kDeleted);
}

// ---------- 主循环 ----------

void EventLoop::loop() {
    assert_in_loop_thread();

    epoll_event events[kMaxEvents];
    while (!quit_.load()) {
        int nready = ::epoll_wait(epfd_, events, kMaxEvents, -1);
        if (nready < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        // 派发：epoll 直接告诉我们 Channel*，O(1) 拿到回调对象
        // 安全性：本批次开始时这些 Channel 都是有效的；如果前序回调里 remove + delete 了
        //   后序某个 Channel，那个指针就悬空 —— 这是 muduo 也存在的"事件批内 use-after-free"风险。
        //   现实场景里：单线程模型下 Connection 析构（释放 Channel）总是发生在
        //   它自己的 close_cb 之后；同一批 epoll_wait 不会同时收到不同连接的事件后又互相 delete。
        //   等引入 shared_ptr<Connection> + tied 机制后才能彻底闭环。当前阶段先记下这条隐患。
        for (int i = 0; i < nready; ++i) {
            auto* ch = static_cast<Channel*>(events[i].data.ptr);
            ch->handle_event(events[i].events);
        }

        // 处理跨线程投递的任务（放在事件分发之后）
        do_pending_functors();
    }
}

void EventLoop::quit() {
    quit_.store(true);
    if (!in_loop_thread()) {
        wakeup();
    }
}

// ---------- 跨线程任务投递 ----------

void EventLoop::run_in_loop(Functor f) {
    if (in_loop_thread()) {
        f();
    } else {
        queue_in_loop(std::move(f));
    }
}

void EventLoop::queue_in_loop(Functor f) {
    {
        std::lock_guard<std::mutex> lk(mutex_);
        pending_functors_.push_back(std::move(f));
    }

    // 何时需要 wakeup？两种情况：
    //   (a) 不是 loop 线程提交的：loop 线程可能正阻塞在 epoll_wait，必须戳醒
    //   (b) loop 线程提交、但正在执行 functors：本批 functors 已经 swap 走了，
    //       新塞的这个不戳醒就会等到下次有 I/O 才被处理
    if (!in_loop_thread() || calling_pending_functors_) {
        wakeup();
    }
}

// ---------- 定时器：转发给 TimerQueue ----------

EventLoop::TimerId EventLoop::run_at(Timestamp when, TimerCallback cb) {
    return timer_queue_->run_at(when, std::move(cb));
}

EventLoop::TimerId EventLoop::run_after(int64_t delay_ms, TimerCallback cb) {
    return timer_queue_->run_after(delay_ms, std::move(cb));
}

void EventLoop::cancel_timer(TimerId id) {
    timer_queue_->cancel(id);
}

// ---------- 线程归属 ----------

void EventLoop::assert_in_loop_thread() const {
    if (!in_loop_thread()) {
        std::cerr << "[loop] FATAL: called from wrong thread. "
                  << "owner=" << owner_tid_
                  << " current=" << std::this_thread::get_id() << std::endl;
        std::abort();
    }
}

// ---------- 内部 ----------

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        std::cerr << "[loop] wakeup write returned " << n << std::endl;
    }
}

void EventLoop::handle_wakeup() {
    uint64_t cnt = 0;
    ssize_t n = ::read(wakeup_fd_, &cnt, sizeof(cnt));
    if (n != sizeof(cnt)) {
        std::cerr << "[loop] wakeup read returned " << n << std::endl;
    }
}

void EventLoop::do_pending_functors() {
    // 双 buffer swap：锁内只 swap（O(1)），执行 functor 时不持锁（避免死锁）
    std::vector<Functor> functors;
    calling_pending_functors_ = true;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        functors.swap(pending_functors_);
    }
    for (auto& f : functors) {
        f();
    }
    calling_pending_functors_ = false;
}

}  // namespace epoll_proj
