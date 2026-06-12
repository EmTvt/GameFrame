// EventLoop: 事件循环（第六步演进 —— 加入线程绑定 + 跨线程任务投递）
//
// 设计动机：
//   原先 TcpServer 同时承担两件事 ——
//     1) "怎么 accept、怎么管理连接表"（业务逻辑）
//     2) "epoll_create / epoll_wait / epoll_ctl 怎么调"（事件机制）
//   把 (2) 抽到 EventLoop 里，TcpServer 只用它的接口，互不影响。
//
// 本步新增的核心能力（为多 Reactor 铺路）：
//   - 线程绑定：构造时记下 owner_tid_，所有"会改 handlers_"的操作必须在该线程
//     → assert_in_loop_thread() 早暴露误用；好过任由用户态 map 在多线程下 race
//   - eventfd 唤醒：跨线程要让 loop 醒过来时，写一个字节到 wakeup_fd_
//   - 任务队列：pending_functors_ 让别的线程能"请求 loop 线程做事"
//   - run_in_loop / queue_in_loop：把上面三件事打包成简洁的 API
//
// 核心思想 (one loop per thread)：
//   一个 EventLoop = 一个线程。所有挂在它上面的 fd I/O 都串行地在这个线程执行，
//   业务因此可以假设"我永远是单线程"。跨线程的事情统一走 run_in_loop。
//
// 回调签名为 void(uint32_t events) 的取舍：
//   epoll 的事件位本来就是混合（EPOLLIN | EPOLLOUT | EPOLLERR | ...），
//   让上层自己拆分支比"在 EventLoop 里强行拆成 read_cb / write_cb / err_cb 三个"
//   更直接，也避免在没有 Channel 抽象时一个 fd 注册三份回调的麻烦。
//   等引入 Channel 后这里会再演进。

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace epoll_proj {

class EventLoop {
public:
    // fd 上的事件回调：参数是 epoll_wait 返回的事件位掩码
    using EventCallback = std::function<void(uint32_t events)>;
    // 跨线程投递的任务：无参，业务自己用 lambda 捕获上下文
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // ---------- fd 注册（只允许 loop 线程调）----------

    // 注册 fd 到 epoll，附带它的事件回调
    // 同一个 fd 重复 add 会被视为参数错误（打日志 + 忽略），避免静默覆盖回调
    void add_fd(int fd, uint32_t events, EventCallback cb);

    // 修改 fd 关注的事件位（回调不变）
    void mod_fd(int fd, uint32_t events);

    // 从 epoll 注销 fd 并丢弃其回调
    // 注意：本函数只做 EPOLL_CTL_DEL + 移除回调，不 ::close(fd)；
    //       fd 的生命周期由调用方（Connection）负责，这里只管 epoll 视图。
    void del_fd(int fd);

    // ---------- 主循环 ----------

    // 阻塞循环，直到外部调用 quit()
    // 谁调它，谁就成为本 EventLoop 的 owner thread。
    void loop();

    // 标记退出。可以从任意线程调（包括信号 handler，但目前没装信号）。
    // 跨线程调用时会 wakeup() 让 loop 立刻醒过来响应。
    void quit();

    // ---------- 跨线程任务投递 ----------

    // 在 loop 线程上执行 f。
    //   - 若调用者就是 loop 线程：直接执行（零开销快路径）
    //   - 否则：入队 + wakeup
    // 这是所有"跨线程操作 EventLoop / 操作 loop 上的连接"的唯一入口。
    void run_in_loop(Functor f);

    // 强制入队（即使调用者是 loop 线程也不直接执行）
    // 用途：loop 线程内部想"先处理完当前事件，再做某事"，避免重入
    void queue_in_loop(Functor f);

    // ---------- 线程归属 ----------

    bool in_loop_thread() const { return owner_tid_ == std::this_thread::get_id(); }

    // 不在 loop 线程就 abort —— 把"误用"变成"立刻崩"，比 race 好得多
    void assert_in_loop_thread() const;

private:
    void wakeup();                  // 写 1 字节到 wakeup_fd_ 把 epoll_wait 戳醒
    void handle_wakeup();           // 把 wakeup_fd_ 的 1 字节读走，否则 LT 会一直触发
    void do_pending_functors();     // swap 出队列后逐个执行（执行时不持锁）

    int epfd_ = -1;
    int wakeup_fd_ = -1;            // eventfd，用于跨线程戳醒 epoll_wait

    std::atomic<bool> quit_{false};

    // owner 线程 id：在构造时确定（**构造线程即 loop 线程**）
    // 这样"构造 → add_fd → loop()"自然在同一线程，符合 main 线程直接持有的用法。
    // 后续把 EventLoop 放进 EventLoopThread 时，会在新线程里 new EventLoop，
    //   构造和 loop() 都在子线程上，绑定关系仍然成立。
    //
    // 不用 atomic<thread::id>：标准只保证 thread::id 可比较/可拷贝，
    //   atomic 特化各家标准库支持不一（教学项目避免坑）
    // 改用：一次写（构造时）、之后只读 ——
    //   - 一次写 happens-before 任何线程后续读它，无需原子
    //   - 多线程读到的就是构造时写入的值，语义安全
    std::thread::id owner_tid_{};

    // 是否正在执行 pending_functors_（用于 queue_in_loop 决定要不要 wakeup）
    // 不需要 atomic：只在 loop 线程读写
    bool calling_pending_functors_ = false;

    std::mutex mutex_;                       // 仅保护 pending_functors_
    std::vector<Functor> pending_functors_;  // 跨线程投递的任务

    // fd → 事件回调
    // 只在 loop 线程访问，无需加锁（靠 assert_in_loop_thread 守住）
    std::unordered_map<int, EventCallback> handlers_;
};

}  // namespace epoll_proj
