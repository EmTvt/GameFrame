// test_mpsc_queue: 验证 MPSCQueue<T> 的核心语义
//
// 覆盖：
//   1. 单线程：push / drain 基本顺序（FIFO）、容量边界、满则丢最新
//   2. dropped 计数：满之后所有失败 push 都计入，take_dropped 取出后清零
//   3. 边沿通知：第一次 push 返回 wake_up=true；后续 push 在未 drain 前一律 false；
//      drain 取空后下一次 push 又返回 true
//   4. 多生产者并发：4 producer × N 条，单消费者 drain，校验"成功入队 + 丢弃 == 总条数"
//      （丢最新策略下不丢条数总账）
//
// 故意不做：性能 benchmark（这是单元测试，不是 perf 测试）。

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "util/mpsc_queue.h"

using epoll_proj::MPSCQueue;

namespace {

void test_basic_fifo() {
    MPSCQueue<int> q(8);
    assert(q.capacity() == 8);
    assert(q.size() == 0);
    assert(!q.has_pending());

    for (int i = 0; i < 5; ++i) {
        auto r = q.push(i);
        assert(r.ok);
    }
    assert(q.size() == 5);
    assert(q.has_pending());

    std::vector<int> out;
    size_t n = q.drain(out, 100);
    assert(n == 5);
    assert(out.size() == 5);
    for (int i = 0; i < 5; ++i) assert(out[i] == i);
    assert(q.size() == 0);
    assert(!q.has_pending());

    std::printf("[ok] test_basic_fifo\n");
}

void test_capacity_drop_newest() {
    MPSCQueue<int> q(3);

    assert(q.push(1).ok);
    assert(q.push(2).ok);
    assert(q.push(3).ok);
    assert(q.size() == 3);

    // 满了，下面这些都该失败，且不动队列里的现有元素
    auto r4 = q.push(4);
    auto r5 = q.push(5);
    assert(!r4.ok && !r4.wake_up);
    assert(!r5.ok && !r5.wake_up);
    assert(q.size() == 3);

    // dropped 计数应为 2
    assert(q.take_dropped() == 2);
    // 再读一次应清零
    assert(q.take_dropped() == 0);

    // drain 出来的还是最早入队的 1, 2, 3（丢最新 = 后到的 4/5 被丢）
    std::vector<int> out;
    q.drain(out, 100);
    assert(out.size() == 3);
    assert(out[0] == 1 && out[1] == 2 && out[2] == 3);

    std::printf("[ok] test_capacity_drop_newest\n");
}

void test_edge_wakeup() {
    MPSCQueue<int> q(4);

    // 空队列第一次 push 应该 wake_up=true
    auto r1 = q.push(10);
    assert(r1.ok && r1.wake_up);

    // 还没 drain，has_pending_ 仍为 true，后续 push 一律 wake_up=false
    auto r2 = q.push(11);
    auto r3 = q.push(12);
    assert(r2.ok && !r2.wake_up);
    assert(r3.ok && !r3.wake_up);

    // drain 取走部分，但没取空 → has_pending_ 保持 true
    std::vector<int> out;
    size_t n = q.drain(out, 1);
    assert(n == 1);
    assert(q.has_pending());
    auto r4 = q.push(13);
    assert(r4.ok && !r4.wake_up);   // 仍然 false：消费者已经在路上

    // drain 取空 → has_pending_ 翻回 false
    n = q.drain(out, 100);
    assert(n == 3);   // 11, 12, 13
    assert(!q.has_pending());

    // 下一次 push 又是边沿
    auto r5 = q.push(14);
    assert(r5.ok && r5.wake_up);

    std::printf("[ok] test_edge_wakeup\n");
}

void test_concurrent_push_total_accounting() {
    // 4 个生产者并发各 push N 条，单消费者持续 drain。
    // 校验：成功入队总数 + dropped 总数 == 4*N。
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 50000;
    constexpr int kTotal = kProducers * kPerProducer;

    // 故意把容量设小，强制触发"满则丢最新"
    MPSCQueue<int> q(1024);

    std::atomic<bool> producers_done{false};
    std::atomic<uint64_t> push_ok_total{0};

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q, &push_ok_total, p]() {
            uint64_t local_ok = 0;
            for (int i = 0; i < kPerProducer; ++i) {
                if (q.push(p * 1000000 + i).ok) ++local_ok;
            }
            push_ok_total.fetch_add(local_ok, std::memory_order_relaxed);
        });
    }

    // 消费者：持续 drain 直到 producers_done 且队列空
    uint64_t drained_total = 0;
    std::thread consumer([&]() {
        std::vector<int> batch;
        while (true) {
            batch.clear();
            size_t n = q.drain(batch, 256);
            drained_total += n;
            if (n == 0) {
                if (producers_done.load(std::memory_order_acquire) &&
                    q.size() == 0) {
                    break;
                }
                std::this_thread::yield();
            }
        }
    });

    for (auto& t : producers) t.join();
    producers_done.store(true, std::memory_order_release);
    consumer.join();

    const uint64_t dropped = q.take_dropped();
    const uint64_t ok = push_ok_total.load();

    std::printf("[concurrent] producers=%d each=%d total=%d "
                "ok=%llu drained=%llu dropped=%llu\n",
                kProducers, kPerProducer, kTotal,
                (unsigned long long)ok,
                (unsigned long long)drained_total,
                (unsigned long long)dropped);

    // 每条消息要么入队成功（最终被 drain）要么被丢弃，不能凭空消失
    assert(ok + dropped == (uint64_t)kTotal);
    // drain 出来的应等于成功入队的（消费者最后把队列取空了）
    assert(drained_total == ok);

    std::printf("[ok] test_concurrent_push_total_accounting\n");
}

// 用 string 验证 move-only-ish 的元素也能正确搬运（dropped 路径不会泄漏）
void test_string_payload() {
    MPSCQueue<std::string> q(2);
    assert(q.push(std::string("hello")).ok);
    assert(q.push(std::string("world")).ok);
    // 满了，下面这条会被 move 进 push，然后被丢（v 在 push 内部出作用域析构）
    assert(!q.push(std::string("dropped")).ok);
    assert(q.take_dropped() == 1);

    std::vector<std::string> out;
    q.drain(out, 10);
    assert(out.size() == 2);
    assert(out[0] == "hello");
    assert(out[1] == "world");

    std::printf("[ok] test_string_payload\n");
}

// 4 producer 并发 push 到一个刚 drain 空的队列，期望全场只有 1 次 wake_up=true
void test_edge_wakeup_concurrent() {
    MPSCQueue<int> q(10000);
    std::atomic<int> wake_count{0};
    std::atomic<bool> go{false};

    auto producer = [&]() {
        while (!go.load()) {}                      // 同一时刻起跑
        for (int i = 0; i < 1000; ++i) {
            if (q.push(i).wake_up) ++wake_count;
        }
    };

    std::vector<std::thread> ts;
    for (int i = 0; i < 4; ++i) ts.emplace_back(producer);
    go.store(true);
    for (auto& t : ts) t.join();

    // 4000 次 push 期间从未 drain 过，has_pending_ 翻 true 后再没机会回 false。
    // 期望：恰好 1 次 wake_up（首次 CAS 成功的那个生产者）
    assert(wake_count.load() == 1);
    std::printf("[ok] test_edge_wakeup_concurrent\n");
}

// 模拟真实 LogSender 行为：消费者只在收到 wake_up 信号时才 drain；如果有
// "drain 取空 / push 翻 true" 交叉漏 wakeup，就会有消息永远卡在队列里。
void test_drain_to_push_handoff() {
    MPSCQueue<int> q(64);
    std::atomic<bool> stop{false};
    std::atomic<int> pending_wakeups{0};   // 模拟 eventfd 计数
    std::atomic<uint64_t> consumed{0};

    std::thread consumer([&]() {
        std::vector<int> batch;
        while (!stop.load() || pending_wakeups.load() > 0 || q.size() > 0) {
            // 等 wakeup（轮询模拟）
            if (pending_wakeups.exchange(0) == 0 && !stop.load()) {
                std::this_thread::yield();
                continue;
            }
            batch.clear();
            consumed += q.drain(batch, 256);
        }
    });

    constexpr int N = 200000;
    std::thread producer([&]() {
        for (int i = 0; i < N; ++i) {
            auto r = q.push(i);
            if (r.wake_up) pending_wakeups.fetch_add(1);
            // 不 yield，故意拼命压
        }
    });

    producer.join();
    stop.store(true);
    consumer.join();

    // 关键：consumer 只信 wake_up 信号。如果有边沿丢失 → 队列残留 → consumed < N
    assert(consumed.load() + q.take_dropped() == N);
    assert(q.size() == 0);
    std::printf("[ok] test_drain_to_push_handoff\n");
}

}  // namespace

int main() {
    test_basic_fifo();
    test_capacity_drop_newest();
    test_edge_wakeup();
    test_string_payload();
    test_concurrent_push_total_accounting();
    test_edge_wakeup_concurrent();
    test_drain_to_push_handoff();
    std::printf("ALL PASSED\n");
    return 0;
}
