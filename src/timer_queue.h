// TimerQueue: 基于 timerfd 的定时器队列
//
// 设计动机：
//   - 业务需要"N 秒后做点什么"（idle timeout、心跳超时、延迟关闭等）
//   - 不轮询：每 EventLoop 一个 timerfd，到期由内核触发可读事件，跟现有
//     epoll + Channel 体系完美融合，零侵入到 EventLoop::loop()
//   - 不用 epoll_wait 的 timeout 参数：那条路要每轮重算 timeout，且无法
//     从外部线程"插入新 timer 后立即起效"——必须 wakeup epoll
//
// 关键决策：
//   1) std::set 而不是 priority_queue —— 后者不支持 O(log N) 随机删除（cancel 难做）
//   2) (Timestamp, TimerId) 复合 key —— 同一时刻多个 timer 不会互相覆盖
//   3) steady_clock 不用 system_clock —— 防止用户改系统时间导致定时器乱跳
//   4) 每个 EventLoop 一个 TimerQueue —— 定时回调永远在 loop 线程执行，无锁
//   5) cancel 用 TimerId（uint64_t）而不是迭代器 —— 不向用户暴露内部数据结构
//   6) 额外维护 unordered_map<TimerId, set::iterator> —— cancel 从 O(N) 降到 O(log N)

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <unordered_map>

namespace epoll_proj {

class EventLoop;
class Channel;

class TimerQueue {
public:
    using TimerCallback = std::function<void()>;
    using Timestamp     = std::chrono::steady_clock::time_point;
    using TimerId       = uint64_t;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    // 在指定绝对时刻执行一次 cb。返回 TimerId 供 cancel 使用。
    // 任何线程调用都安全（内部 run_in_loop 派进 loop 线程）。
    TimerId run_at(Timestamp expiration, TimerCallback cb);

    // delay_ms 毫秒之后执行一次 cb
    TimerId run_after(int64_t delay_ms, TimerCallback cb);

    // 取消尚未触发的 timer。已触发或已取消的 ID 是 no-op。
    // 任何线程调用都安全。
    void cancel(TimerId id);

private:
    // 内部存储项：按 (expiration, id) 排序
    //   - 时间相同时按 id 区分，避免 set 把它们认成同一个 key
    //   - id 唯一递增，全局有序
    struct Entry {
        Timestamp     expiration;
        TimerId       id;
        TimerCallback cb;

        bool operator<(const Entry& other) const {
            if (expiration != other.expiration) return expiration < other.expiration;
            return id < other.id;
        }
    };

    // 以下都在 loop 线程执行
    void add_timer_in_loop(Entry entry);
    void cancel_in_loop(TimerId id);
    void handle_read();                        // timerfd 可读时触发
    void reset_timerfd(Timestamp earliest);    // 把 timerfd 设到下次到期时刻

    EventLoop*               loop_;
    int                      timerfd_;
    std::unique_ptr<Channel> timerfd_channel_;

    // 主存储 —— 仅 loop 线程访问，无需加锁
    std::set<Entry> timers_;

    // 辅助索引：TimerId → 主存储中的 iterator。
    //
    // 安全性前提：std::set 的迭代器在 insert/erase 其他元素时不失效，
    //   只有被 erase 的那一个失效。所以缓存 iterator 是稳的，不需要每次 cancel 重新找。
    //
    // 维护点：add_timer_in_loop 插入时记一笔，cancel_in_loop / handle_read 删除时擦掉。
    std::unordered_map<TimerId, std::set<Entry>::iterator> id_to_iter_;

    // ID 生成器：可能从任意线程被调（run_at 立刻分配 id 后再派进 loop）
    std::atomic<TimerId> next_id_{1};
};

}  // namespace epoll_proj
