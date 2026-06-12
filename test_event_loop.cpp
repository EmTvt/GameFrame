// EventLoop 跨线程能力的最小验证
//
// 测什么：
//   1) 在 thread A 跑 loop()
//   2) 在 thread B 通过 run_in_loop 投递任务
//   3) 任务确实在 thread A 上执行（不是 thread B）
//   4) loop.quit() 能让 loop 退出（跨线程调用也行）
//
// 不测 TcpServer，独立可执行文件。

#include <chrono>
#include <iostream>
#include <thread>

#include "event_loop.h"

int main() {
    epoll_proj::EventLoop loop;

    std::thread::id main_tid = std::this_thread::get_id();
    std::thread::id loop_tid;  // 用来记 loop 实际跑在哪个线程

    // 后台线程：sleep 一会儿，然后跨线程往 loop 塞任务，最后 quit
    std::thread worker([&loop, &loop_tid, main_tid]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::cout << "[worker] in tid=" << std::this_thread::get_id()
                  << ", posting task..." << std::endl;

        loop.run_in_loop([&loop_tid]() {
            loop_tid = std::this_thread::get_id();
            std::cout << "[task ] running in tid=" << loop_tid << std::endl;
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::cout << "[worker] requesting quit..." << std::endl;
        loop.quit();
    });

    std::cout << "[main  ] main tid=" << main_tid
              << ", starting loop..." << std::endl;
    loop.loop();
    std::cout << "[main  ] loop returned." << std::endl;

    worker.join();

    // 验证：任务跑在 main（loop）线程，不是 worker 线程
    if (loop_tid == main_tid) {
        std::cout << "[check ] PASS: task ran on loop thread." << std::endl;
        return 0;
    } else {
        std::cout << "[check ] FAIL: task ran on tid=" << loop_tid
                  << ", expected main_tid=" << main_tid << std::endl;
        return 1;
    }
}
