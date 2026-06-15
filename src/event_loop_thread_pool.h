// EventLoopThreadPool: 一组 IO 线程（每个线程一个 EventLoop）
//
// 模型对照（muduo 经典 main-sub Reactor）：
//   mainLoop（用户传入，跑在主线程）：只做 accept，不处理已建立连接
//   subLoops（本类管理，每个一个独立线程）：处理被分配的 Connection 的所有 I/O
//
// num_threads 语义：
//   0  → 退化成"单 Reactor"：next_loop() 永远返回 mainLoop，等价于不引入多线程
//        这是个**重要兼容点**：业务代码不改、不开线程也能跑
//   N  → 起 N 个子线程，accept 后 round-robin 分配
//
// 不负责什么：
//   线程优先级、亲和性、动态扩缩容、按 fd 哈希分配 —— 这些是工业级扩展，教学略

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace epoll_proj {

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool {
public:
    EventLoopThreadPool(EventLoop* main_loop, size_t num_threads);
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    // 启动所有子线程，阻塞直到每个子线程的 EventLoop 都跑起来
    // 只能调一次
    void start();

    // round-robin 取下一个 subLoop。
    // num_threads == 0 时返回 main_loop_（退化为单 Reactor）
    // 必须在 main_loop_ 线程调（accept 路径上自然满足）
    EventLoop* next_loop();

    size_t num_threads() const { return num_threads_; }

private:
    EventLoop* main_loop_;       // 不持有，只引用
    size_t num_threads_;
    size_t next_ = 0;            // round-robin 游标
    bool started_ = false;

    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;   // 平铺各 subLoop 指针，便于 next_loop() O(1) 取
};

}  // namespace epoll_proj
