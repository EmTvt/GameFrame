// MPSCQueue<T>: 多生产者单消费者的有界队列
//
// 用途：
//   业务线程（任意数量）→ LOG_INFO("...") → MPSCQueue::push → ... drain → LogSender 线程
//
// 核心目标（按重要性）：
//   1. 业务线程 push 路径不能拖累 TcpServer 的 epoll 主循环。
//      - push 临界区只做 O(1) 的指针/计数器移动 + 一次 std::move(T)，绝不做 I/O / 分配。
//      - 容量满时直接 return false（dropped_++），不阻塞、不等待、不唤醒生产者。
//      - 第一版用 std::mutex + ring buffer。malloc 全部发生在构造时，push 路径不会再 new。
//        profile 后如果 mutex 是瓶颈，再换 lock-free（SPSC ring + 多生产者 CAS / Vyukov MPMC）。
//   2. 满时"丢最新"（与 muduo / spdlog 一致）。push 失败计入 dropped_，由消费者侧
//      合成一条 "[LOG_DROPPED] N messages lost" 注入下游，避免静默丢失。
//   3. 单消费者批量 drain：drain 一次最多取 max_batch 条，避免 LogSender 在一个网络包里
//      塞太多东西阻塞 send buffer。
//
// 边沿唤醒（has_pending_）：
//   push 成功后，外层（LogSender 包装层）只在"队列原本是空 → 现在有了"那一次需要 wakeup
//   消费者线程的 EventLoop。如果每次 push 都 wakeup，高吞吐时 wakeup 自身就成了瓶颈。
//   has_pending_ 是 atomic<bool>：push 成功且当前为 false 时翻 true（只有这次返回的
//   should_wake==true 才需要 wakeup）；drain 完成后若队列空则翻回 false。
//
// 不做的事（避免过度设计）：
//   - 不阻塞 push（不是流量控制队列，是丢弃队列）
//   - 不暴露 pop 单条接口（只给 drain，强迫批处理）
//   - 不支持非平凡的 T 拷贝（只 move，要求 T 可 move 构造 + move 赋值）

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

namespace epoll_proj {

template <class T>
class MPSCQueue {
public:
    // capacity 必须 >= 1。0 容量没有意义（永远满，永远丢）。
    explicit MPSCQueue(size_t capacity)
        : cap_(capacity == 0 ? 1 : capacity),
          slots_(cap_) {  // 一次性分配，push 路径之后再不会进 allocator
    }

    MPSCQueue(const MPSCQueue&) = delete;
    MPSCQueue& operator=(const MPSCQueue&) = delete;

    // 业务线程调。临界区只做：判满 + 写槽位 + 移动 tail + count++。
    //
    // 返回值：
    //   { ok      : true 表示入队成功；false 表示队列满，已计入 dropped_。
    //     wake_up : true 表示这次 push 是"空 → 非空"的边沿翻转，调用方应该
    //               wakeup 消费者 loop；false 表示无需 wakeup。
    //               ok==false 时 wake_up 一定为 false。 }
    //
    // 设计说明：
    //   - has_pending_ 翻转放在锁外用 CAS 做，不进临界区。
    //   - 因为只有消费者会把 has_pending_ 翻 false（drain 收尾时），生产者侧只会
    //     "false → true" 单向翻转，CAS 不会出现 ABA 风险。
    struct PushResult {
        bool ok;
        bool wake_up;
    };

    PushResult push(T v) {
        bool ok;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (count_ == cap_) {
                ok = false;   // 满：丢最新。dropped_ 放锁外++，让临界区更短。
            } else {
                slots_[tail_] = std::move(v);
                tail_ = (tail_ + 1) % cap_;
                ++count_;
                ok = true;
            }
        }
        if (!ok) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return {false, false};
        }
        // 边沿通知：只有从 false → true 的那次返回 wake_up = true。
        // 多生产者并发时只有一个能 CAS 成功，避免重复 wakeup。
        // CAS 放锁外做：拿着 mutex 去做 atomic 是双重同步，没必要。
        bool expected = false;
        const bool wake = has_pending_.compare_exchange_strong(
            expected, true,
            std::memory_order_release,   // 让消费者看到 has_pending_=true 时也能看到队列里的新元素
            std::memory_order_relaxed);
        return {true, wake};
    }

    // LogSender 线程调。最多取 max_batch 条 move 到 out 末尾，返回实际取出条数。
    //
    // out 不会被清空，drain 是 append 语义，方便消费者把多个来源拼到同一个 batch 里。
    //
    // 返回 0 表示队列当前为空。这种情况下会把 has_pending_ 翻回 false：
    //   - 翻 false 之后才解锁，避免"消费者刚翻 false → 生产者立刻翻 true 但同时
    //     生产者已经看到 false 触发 wakeup"这种交叉，has_pending_ 边沿语义不丢。
    //   - 注意：drain 出 N>0 条但不一定取空了队列时，has_pending_ 保持 true，下次
    //     drain 还会被定时器拉起来（LogSender 用短周期定时器 + 边沿 wakeup 双保险）。
    size_t drain(std::vector<T>& out, size_t max_batch) {
        if (max_batch == 0) return 0;

        size_t taken = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            while (taken < max_batch && count_ > 0) {
                out.emplace_back(std::move(slots_[head_]));
                head_ = (head_ + 1) % cap_;
                --count_;
                ++taken;
            }
            // 队列在锁内被取空，那么把 has_pending_ 翻回 false，与下一次 push 的
            // CAS 形成边沿对。如果还没取空（count_ > 0），保持 true。
            if (count_ == 0) {
                has_pending_.store(false, std::memory_order_release);
            }
        }
        return taken;
    }

    // 原子读出 dropped 计数并清零。LogSender 在每次 drain 后调一次：
    // 拿到非 0 就在下游合成一条 "[LOG_DROPPED] N messages lost"。
    uint64_t take_dropped() {
        return dropped_.exchange(0, std::memory_order_relaxed);
    }

    // 仅供观测。不要拿这个值做边沿判断 —— 用 push 返回的 wake_up。
    bool has_pending() const {
        return has_pending_.load(std::memory_order_acquire);
    }

    // 仅供测试 / 监控。生产路径不要依赖这个值做决策（拿到时已经过期）。
    size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return count_;
    }

    size_t capacity() const { return cap_; }

private:
    const size_t cap_;

    // ring buffer：构造时一次性分配 cap_ 个槽位，push/drain 路径只移动元素，不再 new。
    // 选 vector<T> 而不是 unique_ptr<T[]>：生命周期管理更省心，T 析构 vector 自动负责。
    std::vector<T> slots_;

    mutable std::mutex mu_;     // 保护 head_ / tail_ / count_ / slots_。mutable 让 const size() 也能上锁。
    size_t head_ = 0;           // 下一次 drain 从这里开始
    size_t tail_ = 0;           // 下一次 push 写到这里
    size_t count_ = 0;          // 当前在队元素数；用计数器而不是"留一格"判满，逻辑更直白

    // 满时丢的条数。relaxed 即可：take_dropped 的调用方（LogSender）不依赖它和队列内具体哪条消息形成顺序关系，只要总数对就够了。
    std::atomic<uint64_t> dropped_{0};

    // 边沿通知标志。push 成功 + 原本为 false → CAS 翻 true（这一次需要 wakeup）。
    // drain 取空 → 翻 false。其他时候不动。
    std::atomic<bool> has_pending_{false};
};

}  // namespace epoll_proj
