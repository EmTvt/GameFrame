#include "event_loop_thread_pool.h"

#include "event_loop.h"
#include "event_loop_thread.h"

namespace epoll_proj {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* main_loop, size_t num_threads)
    : main_loop_(main_loop), num_threads_(num_threads) {}

EventLoopThreadPool::~EventLoopThreadPool() = default;
// 子线程的退出由 EventLoopThread 析构负责（quit + join），这里 vector 析构会逐个触发

void EventLoopThreadPool::start() {
    started_ = true;
    for (size_t i = 0; i < num_threads_; ++i) {
        auto t = std::make_unique<EventLoopThread>();
        EventLoop* sub = t->start_loop();   // 阻塞等子线程的 loop 就绪
        loops_.push_back(sub);
        threads_.push_back(std::move(t));
    }
}

EventLoop* EventLoopThreadPool::next_loop() {
    // 兼容点：num_threads == 0 时不开子线程，全部走 main_loop_
    // 这样上层 TcpServer 不用 if 判断单/多 Reactor，统一调 next_loop() 即可
    if (loops_.empty()) return main_loop_;

    EventLoop* loop = loops_[next_];
    next_ = (next_ + 1) % loops_.size();
    return loop;
}

}  // namespace epoll_proj
