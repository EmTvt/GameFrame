// EventLoopThread: "一个线程 + 一个 EventLoop" 的封装
//
// 为什么需要这个类：
//   EventLoop 构造时会绑定 owner 线程（owner_tid_ = this_thread::get_id()），
//   所以它**必须在它要服务的那个线程里 new**，不能主线程 new 完丢过去。
//   主线程要拿到 EventLoop* 才能调 run_in_loop，就需要一个同步机制告诉主线程
//   "子线程的 loop 已经构造好了"——这就是本类用 mutex + condition_variable 的原因。
//
// 生命周期约定：
//   - start_loop() 返回的 EventLoop* 由本对象持有，调用方只借用，不要 delete
//   - 析构里负责让 loop quit + join 线程，所以主线程析构是阻塞的
//
// 教学项目简化：不提供"线程初始化回调"等 muduo 里有的扩展点。

#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

namespace epoll_proj {

class EventLoop;

class EventLoopThread {
public:
    EventLoopThread();
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // 启动子线程，阻塞直到子线程把 EventLoop 构造好，返回其指针
    // 只能调一次；重复调行为未定义（教学项目不加 assert，靠约定）
    EventLoop* start_loop();

private:
    // 子线程入口：构造 EventLoop（绑定到本线程）→ notify 主线程 → 进入 loop()
    void thread_func();

    EventLoop* loop_ = nullptr;   // 由子线程写、主线程读，受 mutex_ 保护
    bool exiting_ = false;

    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace epoll_proj
