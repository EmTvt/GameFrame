#include "event_loop_thread.h"

#include "event_loop.h"

namespace epoll_proj {

EventLoopThread::EventLoopThread() = default;

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_ != nullptr) {
        // 跨线程 quit：EventLoop::quit 内部会 wakeup，自动唤醒 epoll_wait
        loop_->quit();
        if (thread_.joinable()) thread_.join();
    }
}

EventLoop* EventLoopThread::start_loop() {
    thread_ = std::thread([this]() { thread_func(); });

    // 等子线程把 loop 构造好
    // 这里必须等：直接返回 loop_ 的话主线程读到的可能还是 nullptr
    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lk(mutex_);
        cv_.wait(lk, [this]() { return loop_ != nullptr; });
        loop = loop_;
    }
    return loop;
}

void EventLoopThread::thread_func() {
    // 关键：EventLoop 必须在本线程构造，owner_tid_ 才会绑到本线程
    EventLoop loop;

    {
        std::lock_guard<std::mutex> lk(mutex_);
        loop_ = &loop;
        cv_.notify_one();
    }

    loop.loop();   // 阻塞，直到外部 quit()

    // 退出后把指针清掉 —— 析构里就不会再次调 quit/join
    std::lock_guard<std::mutex> lk(mutex_);
    loop_ = nullptr;
}

}  // namespace epoll_proj
