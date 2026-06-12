// EventLoop: 事件循环
//
// 演进时间线：
//   第五步：从 TcpServer 抽离 epoll 操作
//   第六步：加入线程绑定 + 跨线程任务投递 (run_in_loop / wakeup_fd / pending_functors)
//   第七步：引入 Channel 抽象，接口从 fd 级换成 channel 级（本步）
//
// Channel 抽象后的变化：
//   - 不再维护 unordered_map<fd, EventCallback>：epoll data.ptr 直接存 Channel*
//   - 不再有 add_fd / mod_fd / del_fd：统一成 update_channel / remove_channel，
//     Channel 自己负责"我现在关注什么事件"
//   - 事件派发：epoll_wait 返回 → Channel::handle_event(revents)，由 channel 自己分发
//     到 read/write/close/error 回调
//   - wakeup_fd 也是一个 Channel —— 对自己一视同仁，结构更对称
//
// 线程模型保持不变：构造线程即 loop 线程，所有写操作必须在 loop 线程做。

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace epoll_proj {

class Channel;

class EventLoop {
public:
    // 跨线程投递的任务：无参，业务自己用 lambda 捕获上下文
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // ---------- Channel 注册（只允许 loop 线程调）----------

    // 根据 channel 当前的 state_ 决定 EPOLL_CTL_ADD 还是 EPOLL_CTL_MOD：
    //   - kNew    : ADD → 标记 kAdded
    //   - kDeleted: ADD → 标记 kAdded（之前被 remove 过，现在重新加回来）
    //   - kAdded  : MOD（events == 0 时也走 MOD，让 epoll 不再通知，但保留注册）
    // 这个状态机的好处：避免对同一 fd 重复 ADD（EEXIST）或对未注册 fd MOD（ENOENT）。
    void update_channel(Channel* ch);

    // 从 epoll 注销，并把 channel 状态标记为 kDeleted
    // 注意：本函数只做 epoll 视角的注销，**不 close fd、不 delete channel**
    void remove_channel(Channel* ch);

    // ---------- 主循环 ----------

    // 阻塞循环，直到外部调用 quit()
    void loop();

    // 标记退出。可以从任意线程调。跨线程调时会 wakeup() 立刻醒过来。
    void quit();

    // ---------- 跨线程任务投递 ----------

    // 在 loop 线程上执行 f。
    //   - 若调用者就是 loop 线程：直接执行（零开销快路径）
    //   - 否则：入队 + wakeup
    void run_in_loop(Functor f);

    // 强制入队（即使调用者是 loop 线程也不直接执行）
    void queue_in_loop(Functor f);

    // ---------- 线程归属 ----------

    bool in_loop_thread() const { return owner_tid_ == std::this_thread::get_id(); }
    void assert_in_loop_thread() const;

private:
    void wakeup();                  // 写 1 字节到 wakeup_fd_ 把 epoll_wait 戳醒
    void handle_wakeup();           // 把 wakeup_fd_ 的 1 字节读走（Channel 的 read_cb）
    void do_pending_functors();     // swap 出队列后逐个执行（执行时不持锁）

    int epfd_      = -1;
    int wakeup_fd_ = -1;

    // wakeup_fd_ 对应的 Channel —— 让自己也走 Channel 体系，结构更对称
    // 用 unique_ptr 是因为 Channel 持有 EventLoop*，必须在 EventLoop 完全构造好后才能 new
    std::unique_ptr<Channel> wakeup_channel_;

    std::atomic<bool> quit_{false};

    // owner 线程 id：构造时确定（构造线程即 loop 线程）
    std::thread::id owner_tid_{};

    // 是否正在执行 pending_functors_（用于 queue_in_loop 决定要不要 wakeup）
    bool calling_pending_functors_ = false;

    std::mutex mutex_;                       // 仅保护 pending_functors_
    std::vector<Functor> pending_functors_;
};

}  // namespace epoll_proj
