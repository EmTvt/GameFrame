// test_log_sender: LogSender 的 smoke 测试
//
// 验证三件事：
//   1) 多生产者并发 push 不会崩、不丢"总数"
//      （丢消息会被记到 MPSCQueue::dropped_，由 LogSender 合成 LOG_DROPPED 暴露给 server）
//   2) sender_loop_ 在自己线程里 drain → encode → send，与生产者完全异步
//   3) stop() 能等到剩余消息发完（best-effort）+ 安全断开
//
// 用法：
//   终端 1: ./build/log_server 9099 ./build/res/log
//   终端 2: ./build/test_log_sender                    # 默认连 127.0.0.1:9099
//           ./build/test_log_sender 127.0.0.1 9099
//           ./build/test_log_sender 127.0.0.1 9099 NPROD NMSG
//             例：./build/test_log_sender 127.0.0.1 9099 4 10000
//             → 4 个生产者线程，每个 push 10000 条
//
// 不启动 log_server 也能跑：观察到一直在 retry 即可，stop 时会断干净。
//
// 不是单元测试，结果靠：
//   - stderr 没有 crash / sanitizer 告警
//   - log_server 的落盘文件里能看到 NPROD * NMSG 条（或 NPROD*NMSG - LOG_DROPPED 计数条 + 1 条 LOG_DROPPED 合成）
//   - 进程能正常退出，0 hang

#include <signal.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "log_sender/log_sender.h"
#include "src/event_loop_thread.h"

using namespace epoll_proj;

int main(int argc, char** argv) {
    ::signal(SIGPIPE, SIG_IGN);

    std::string host = "127.0.0.1";
    uint16_t    port = 9099;
    int         n_producers       = 4;
    int         msgs_per_producer = 10000;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<uint16_t>(std::stoi(argv[2]));
    if (argc >= 4) n_producers       = std::stoi(argv[3]);
    if (argc >= 5) msgs_per_producer = std::stoi(argv[4]);

    std::cout << "[test_log_sender] connecting to " << host << ":" << port
              << " | producers=" << n_producers
              << " msgs_per_producer=" << msgs_per_producer << std::endl;

    // 起一个独立的 sender 线程 + 它自己的 EventLoop
    EventLoopThread sender_thread;
    EventLoop* sender_loop = sender_thread.start_loop();

    // 用相对小的 queue 容量制造一些"丢最新"事件 —— 让 LOG_DROPPED 链路也被走到。
    // 4 * 10000 = 40000 条压在一个 capacity=5000 的队列上，必然丢一部分
    LogSender::Options opts;
    opts.queue_capacity      = 5000;
    opts.max_batch_per_tick  = 500;
    opts.tick_interval_ms    = 50;
    opts.high_water_mark     = 1024 * 1024;   // 1 MiB 故意小一点测试 paused 路径

    LogSender sender(sender_loop, host, port, opts);
    sender.start();

    // 4 个生产者线程并发 push
    std::atomic<int> push_ok{0};
    std::atomic<int> push_dropped{0};

    auto producer_fn = [&](int producer_id) {
        for (int i = 0; i < msgs_per_producer; ++i) {
            // 内容里带 producer_id + 序号，方便 server 端落盘后肉眼比对
            std::string msg = "[prod " + std::to_string(producer_id) +
                              "] msg #" + std::to_string(i);
            if (sender.push(std::move(msg))) {
                push_ok.fetch_add(1, std::memory_order_relaxed);
            } else {
                push_dropped.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> producers;
    producers.reserve(n_producers);
    for (int i = 0; i < n_producers; ++i) {
        producers.emplace_back(producer_fn, i);
    }
    for (auto& t : producers) t.join();
    auto t1 = std::chrono::steady_clock::now();

    auto push_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "[test_log_sender] all pushes done in " << push_ms
              << "ms | ok=" << push_ok.load()
              << " dropped=" << push_dropped.load() << std::endl;

    // 留 2 秒让 sender 把队列里剩下的发出去（断连重试场景下可能更久）
    std::cout << "[test_log_sender] sleeping 2s for sender to drain..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "[test_log_sender] calling stop()..." << std::endl;
    sender.stop();
    // sender_thread 析构会 quit sender_loop_ + join；不需要手动操作
    std::cout << "[test_log_sender] bye" << std::endl;
    return 0;
}
