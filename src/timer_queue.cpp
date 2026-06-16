#include "timer_queue.h"

#include <sys/timerfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <utility>

#include "channel.h"
#include "event_loop.h"

namespace epoll_proj {

namespace {

// 把 steady_clock::time_point 拆成 itimerspec（绝对时刻没法直接用，timerfd_settime
// 在非 ABSTIME 模式下要的是"相对当前的剩余时间"；这里用相对时间方案，避免和 CLOCK_REALTIME
// / CLOCK_MONOTONIC 选择纠缠）
itimerspec to_itimerspec(TimerQueue::Timestamp expiration) {
    auto now = std::chrono::steady_clock::now();
    auto remaining = expiration - now;
    if (remaining < std::chrono::nanoseconds(1)) {
        // 已经到期 / 过期：给一个极小正值，让 timerfd 立刻触发
        // 不能给 0：itimerspec 全 0 在 timerfd_settime 里表示"取消定时器"，
        // 我们要的是"立刻触发"
        remaining = std::chrono::nanoseconds(1);
    }
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(remaining);
    auto ns   = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining - secs);

    itimerspec spec{};
    spec.it_value.tv_sec  = secs.count();
    spec.it_value.tv_nsec = ns.count();
    // it_interval 留 0 → 一次性定时器（每次到期我们手动重设到下一个最早项）
    return spec;
}

}  // namespace

TimerQueue::TimerQueue(EventLoop* loop) : loop_(loop) {
    // CLOCK_MONOTONIC：跟 steady_clock 语义一致，不受系统时间调整影响
    // TFD_NONBLOCK：read 不要阻塞 loop 线程
    // TFD_CLOEXEC：fork/exec 时自动关闭，避免泄露给子进程
    timerfd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timerfd_ < 0) {
        std::cerr << "timerfd_create failed: " << std::strerror(errno) << std::endl;
        std::abort();
    }

    timerfd_channel_ = std::make_unique<Channel>(loop_, timerfd_);
    timerfd_channel_->set_read_cb([this]() { handle_read(); });
    // 注意：这里不立即 enable_reading()更好。
    //   timerfd 在没设置过 timerfd_settime 之前永远不会触发，挂上 epoll 也不会有事件；
    //   但更稳妥的做法是"有 timer 时才挂上 epoll"——延迟到第一次 add_timer_in_loop。
    //   不过 muduo 选择直接挂上也无害（无 timer 时 timerfd 不可读），我们也保持简单：
    timerfd_channel_->enable_reading();
}

TimerQueue::~TimerQueue() {
    // 必须在 loop 线程析构（EventLoop 自己也是这个时序）
    if (timerfd_channel_) {
        timerfd_channel_->disable_all();
        timerfd_channel_->remove();
        timerfd_channel_.reset();
    }
    if (timerfd_ >= 0) ::close(timerfd_);
}

// ---------- 公开接口 ----------

TimerQueue::TimerId TimerQueue::run_at(Timestamp expiration, TimerCallback cb) {
    // 立刻分配 id，让调用方拿到返回值后能马上调 cancel —— 即使 add_timer_in_loop
    // 还没真正执行（在跨线程场景下，add 任务可能在队列里排队），cancel_in_loop 也会
    // 在它之后执行，set 操作天然有序，不会出错
    TimerId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    Entry entry{expiration, id, std::move(cb)};

    // 派进 loop 线程操作 set 和 timerfd —— set 不加锁的前提
    loop_->run_in_loop([this, e = std::move(entry)]() mutable {
        add_timer_in_loop(std::move(e));
    });
    return id;
}

TimerQueue::TimerId TimerQueue::run_after(int64_t delay_ms, TimerCallback cb) {
    auto when = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);
    return run_at(when, std::move(cb));
}

void TimerQueue::cancel(TimerId id) {
    loop_->run_in_loop([this, id]() { cancel_in_loop(id); });
}

// ---------- loop 线程内部 ----------

void TimerQueue::add_timer_in_loop(Entry entry) {
    loop_->assert_in_loop_thread();

    // 是否变成了"新的最早到期项"——是的话需要重设 timerfd
    bool earliest_changed = timers_.empty() || entry < *timers_.begin();

    TimerId id = entry.id;
    auto [it, ok] = timers_.insert(std::move(entry));
    // id 全局递增，且 (expiration, id) 复合 key 不可能撞 —— 一定 ok
    // 记到辅助索引里供 cancel O(log N) 用
    id_to_iter_.emplace(id, it);

    if (earliest_changed) {
        reset_timerfd(timers_.begin()->expiration);
    }
}

void TimerQueue::cancel_in_loop(TimerId id) {
    loop_->assert_in_loop_thread();

    // 经由 id_to_iter_ 直查 set 节点 —— 平均 O(1) 查 + O(log N) erase
    auto map_it = id_to_iter_.find(id);
    if (map_it == id_to_iter_.end()) {
        // 找不到：可能已经触发了 / 已经被取消了，no-op
        return;
    }

    auto set_it = map_it->second;
    bool was_earliest = (set_it == timers_.begin());

    timers_.erase(set_it);
    id_to_iter_.erase(map_it);

    // 删的是最早项 → 需要重设 timerfd 到新的最早项
    //   不删最早项的话，下次到期时刻没变，timerfd 不需要动
    if (was_earliest && !timers_.empty()) {
        reset_timerfd(timers_.begin()->expiration);
    }
    // 空了也不用 disarm timerfd：下次 add 时会重设；
    // 即使 timerfd 还指向已过去的时刻，handle_read 进来发现 timers_ 空，
    // 直接返回也无害
}

void TimerQueue::handle_read() {
    loop_->assert_in_loop_thread();

    // 把 timerfd 的"到期次数"读走 —— 不读 epoll 会一直认为它可读（即便 LT；ET 下读了
    // 才能等下次 EPOLLIN，所以这步必须做）
    uint64_t expirations = 0;
    ssize_t n = ::read(timerfd_, &expirations, sizeof(expirations));
    if (n != sizeof(expirations)) {
        // EAGAIN 在 ET 模式下也可能出现（spurious wakeup），不致命
        if (errno != EAGAIN) {
            std::cerr << "[timer] read timerfd returned " << n
                      << " errno=" << std::strerror(errno) << std::endl;
        }
    }

    // 取出所有到期的 timer
    auto now = std::chrono::steady_clock::now();

    // 先把到期项从 set 里搬出来，再统一执行回调。
    // 为什么要分两步：
    //   timer 的回调里可能调 run_after / cancel —— 这两者都派 functor 进 loop 队列，
    //   add_timer_in_loop / cancel_in_loop 会在本函数返回后才被执行。所以这里不会
    //   在迭代过程中修改 set，安全。
    // 但仍然分两步：执行回调时持有的 Entry 是从 set 里 erase 出来的副本，
    //   即使回调内部以某种方式直接动了 timers_（理论上我们的设计不允许，但防御一下），
    //   也不会让正在迭代的 set 失效
    std::vector<Entry> expired;
    while (!timers_.empty() && timers_.begin()->expiration <= now) {
        // set 节点不能直接 move（it->cb 是 const），但 extract 可以 —— C++17
        auto node = timers_.extract(timers_.begin());
        // 同步擦掉辅助索引，避免 cancel 拿到已失效的 iterator
        id_to_iter_.erase(node.value().id);
        expired.push_back(std::move(node.value()));
    }

    for (auto& e : expired) {
        if (e.cb) e.cb();
    }

    // 重设 timerfd 到下一个最早项
    if (!timers_.empty()) {
        reset_timerfd(timers_.begin()->expiration);
    }
}

void TimerQueue::reset_timerfd(Timestamp earliest) {
    itimerspec spec = to_itimerspec(earliest);
    // 不用 TFD_TIMER_ABSTIME：to_itimerspec 已经算成了相对时间
    if (::timerfd_settime(timerfd_, 0, &spec, nullptr) < 0) {
        std::cerr << "[timer] timerfd_settime failed: "
                  << std::strerror(errno) << std::endl;
    }
}

}  // namespace epoll_proj
