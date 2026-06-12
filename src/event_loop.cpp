#include "event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

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
    // 这样"构造 → add_fd → loop()"三步天然在同一线程，符合最常见用法
    owner_tid_ = std::this_thread::get_id();

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");

    // EFD_NONBLOCK：read/write 不阻塞，符合 epoll 风格
    // EFD_CLOEXEC：fork+exec 时自动关闭
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) die("eventfd");

    // 把 wakeup_fd_ 注册到 epoll 上，事件回调就是把那 1 字节读走
    // 注意：这里直接操作 epoll_ctl + handlers_，不走 add_fd，
    //       因为 add_fd 会 assert_in_loop_thread —— 而此刻 loop 还没开始，owner 是空。
    epoll_event ev{};
    ev.events = EPOLLIN;  // 这里故意不用 ET：LT 更简单，wakeup 数据小、不会漏读
    ev.data.fd = wakeup_fd_;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakeup_fd_, &ev) < 0) die("epoll_ctl(wakeup_fd)");
    handlers_.emplace(wakeup_fd_, [this](uint32_t) { handle_wakeup(); });
}

EventLoop::~EventLoop() {
    // 只关 epfd / wakeup_fd_；handlers_ 里登记的业务 fd 由各自的所有者负责
    if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    if (epfd_ >= 0) ::close(epfd_);
}

// ---------- fd 注册（只允许 loop 线程调）----------

void EventLoop::add_fd(int fd, uint32_t events, EventCallback cb) {
    assert_in_loop_thread();

    if (handlers_.count(fd)) {
        std::cerr << "[loop] add_fd: fd=" << fd << " already registered, ignored" << std::endl;
        return;
    }

    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::cerr << "[loop] epoll_ctl ADD fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
        return;
    }
    handlers_.emplace(fd, std::move(cb));
}

void EventLoop::mod_fd(int fd, uint32_t events) {
    assert_in_loop_thread();

    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        std::cerr << "[loop] epoll_ctl MOD fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
    }
}

void EventLoop::del_fd(int fd) {
    assert_in_loop_thread();

    // 先从内部 map 移除回调，再 DEL；顺序原因：
    //   万一 DEL 后还有同批 epoll_wait 返回的事件指向这个 fd，
    //   下面 loop() 里查 handlers_ 找不到就会直接跳过，不会触发悬空回调。
    handlers_.erase(fd);

    // DEL 失败通常意味着 fd 已被 ::close 过（内核会自动从 epoll 移除）；
    // 这是 resource-safety.md 第 5 节描述的"反顺序但内核容忍"的妥协，
    // 所以这里只打调试日志、不视为错误。
    if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) < 0 && errno != EBADF && errno != ENOENT) {
        std::cerr << "[loop] epoll_ctl DEL fd=" << fd << " failed: "
                  << std::strerror(errno) << std::endl;
    }
}

// ---------- 主循环 ----------

void EventLoop::loop() {
    // owner_tid_ 已在构造时确定；这里只做一次防御性检查
    assert_in_loop_thread();

    epoll_event events[kMaxEvents];
    while (!quit_.load()) {
        int nready = ::epoll_wait(epfd_, events, kMaxEvents, -1);
        if (nready < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }

        for (int i = 0; i < nready; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            // 关键防御：本轮前面的某个回调可能已经把 fd 从 loop 里 del_fd 掉了
            // （例如 EPOLLIN 回调里关连接），后面如果还有事件指向这个 fd，找不到回调就跳过。
            // 等价于 resource-safety.md 第 4 节"二次检查 connections_.count(fd)"的下沉。
            auto it = handlers_.find(fd);
            if (it == handlers_.end()) continue;

            it->second(ev);
        }

        // 处理跨线程投递的任务（注意：放在事件分发之后）
        // 时序上的语义：epoll_wait 返回的事件本批先处理完，再吃掉这批 functor
        do_pending_functors();
    }
}

void EventLoop::quit() {
    quit_.store(true);
    // 如果是别的线程发起 quit，loop 此刻可能正阻塞在 epoll_wait 上，需要戳醒它
    // 如果是 loop 线程自己调，下一轮 while 条件检查就会退出，无需 wakeup
    if (!in_loop_thread()) {
        wakeup();
    }
}

// ---------- 跨线程任务投递 ----------

void EventLoop::run_in_loop(Functor f) {
    if (in_loop_thread()) {
        f();  // 快路径：零开销
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
    //       新塞的这个不戳醒就会等到下次有 I/O 才被处理，可能很久
    // 反过来：loop 线程在跑 fd 事件回调时提交，不用 wakeup —— 因为
    //         本轮 fd 事件处理完会自然进入 do_pending_functors()。
    if (!in_loop_thread() || calling_pending_functors_) {
        wakeup();
    }
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
    // 写 1 个 8 字节计数器到 eventfd，让 epoll_wait 立刻返回
    uint64_t one = 1;
    ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // 唯一可能失败是 EAGAIN（eventfd 计数溢出 2^64-2），实际上不会发生
        std::cerr << "[loop] wakeup write returned " << n << std::endl;
    }
}

void EventLoop::handle_wakeup() {
    // 把那 8 字节读走，否则 LT 模式下 epoll 会一直触发
    uint64_t cnt = 0;
    ssize_t n = ::read(wakeup_fd_, &cnt, sizeof(cnt));
    if (n != sizeof(cnt)) {
        std::cerr << "[loop] wakeup read returned " << n << std::endl;
    }
    // 不需要根据 cnt 做什么：函子本身已经在 pending_functors_ 里，
    // 回到 loop() 末尾的 do_pending_functors() 会处理。
}

void EventLoop::do_pending_functors() {
    // muduo 经典手法：double-buffer swap
    //   1) 锁内只做 swap，O(1)，持锁时间极短 → 提交方几乎不阻塞
    //   2) 执行 functor 时不持锁 → functor 里再 queue_in_loop 不会死锁
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
