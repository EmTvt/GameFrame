// test_logging: LOG_* 宏的自包含端到端测试
//
// 与 test_log_sender 的区别：test_log_sender 需要**外部**先起 log_server；
// 本测试**进程内**自建一个 mini 接收端（TcpServer + LengthPrefixedCodec 解码计数），
// 不依赖任何外部进程、不写磁盘，可直接 `./build/test_logging` 一键跑，返回码即结论。
//
// 验证四件事：
//   1) 全链路：LOG_*（业务线程格式化）→ LogSender::push → MPSCQueue → drain/encode
//      → TcpClient → mini sink 解码。收到的帧数 == 发出的日志条数。
//   2) format_log 级别格式：每条 payload 以正确的 "INFO " / "DEBUG " / "WARN " /
//      "ERROR " 前缀开头（即宏选对了级别）。
//   3) nullptr 安全：set_global 之前、set_global(nullptr) 之后调 LOG_* 必须静默 no-op，
//      不崩、也不会有多余的帧到达 sink。
//   4) 优雅停止：sender.stop() + sink quit + join 全部 0 hang。
//
// 结果判定（非交互）：
//   - 各级别计数与期望精确相等 → 打印 [PASS] 返回 0
//   - 任何一项不符 → 打印 [FAIL ...] 返回 1
//
// 注意：故意把日志条数（合计 1600）压在远低于队列容量（默认 10000）之下，
//   且 sink 是本机 loopback 快消费，因此**不应**发生 MPSCQueue 丢弃；一旦计数
//   对不上（尤其 sink 收到 1601 条且多出一条 [LOG_DROPPED]）即视为失败，便于发现回归。

#include <signal.h>

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "log_sender/log_sender.h"
#include "log_sender/logging.h"
#include "src/connection.h"
#include "src/event_loop.h"
#include "src/event_loop_thread.h"
#include "src/server.h"
#include "util/buffer.h"
#include "util/length_prefixed_codec.h"

using namespace epoll_proj;

namespace {

// mini sink 的端口。挑一个不常用的，避开 9099（log_server 约定端口）/8888（echo）。
constexpr uint16_t kSinkPort = 19099;

// 每个级别发多少条。合计 1600，远小于默认队列容量 10000 → 正常路径不应丢。
constexpr int kInfoCount  = 400;
constexpr int kDebugCount = 400;
constexpr int kWarnCount  = 400;
constexpr int kErrorCount = 400;

// sink 收到的各级别计数。只被 sink 线程写；主线程在 join 之后才读（happens-before）。
struct Tally {
    int info  = 0;
    int debug = 0;
    int warn  = 0;
    int error = 0;
    int other = 0;   // 前缀对不上任何已知级别 → 异常
    int total() const { return info + debug + warn + error + other; }
};

// 从 payload "LEVEL file:line | body" 里取出开头的级别词，归类计数。
void classify(const std::string& payload, Tally& t) {
    if (payload.rfind("INFO ", 0) == 0)       ++t.info;
    else if (payload.rfind("DEBUG ", 0) == 0) ++t.debug;
    else if (payload.rfind("WARN ", 0) == 0)  ++t.warn;
    else if (payload.rfind("ERROR ", 0) == 0) ++t.error;
    else                                       ++t.other;
}

}  // namespace

int main() {
    ::signal(SIGPIPE, SIG_IGN);

    // ---- 1) 进程内 mini sink（TcpServer 必须在它自己的线程里构造 + run）----
    Tally tally;
    std::promise<EventLoop*> sink_loop_promise;
    auto sink_loop_future = sink_loop_promise.get_future();

    std::thread sink_thread([&]() {
        TcpServer sink(kSinkPort, /*num_threads=*/0);
        sink.set_message_callback([&](const ConnectionPtr& /*conn*/, Buffer& input) {
            std::vector<std::string> msgs;
            // 我们发的都是合法帧，decode 不会返回 false；忽略返回值
            LengthPrefixedCodec::decode(input, msgs);
            for (const auto& m : msgs) classify(m, tally);
        });
        // 把 mainLoop 指针交回主线程，供 quit 用；run() 会阻塞到被 quit
        sink_loop_promise.set_value(sink.main_loop());
        sink.run();
    });

    EventLoop* sink_loop = sink_loop_future.get();
    // 给 sink 一点时间把 listen socket 挂起来
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // ---- 2) LogSender（独立线程），连本机 sink ----
    EventLoopThread sender_thread;
    EventLoop*      sender_loop = sender_thread.start_loop();
    LogSender       sender(sender_loop, "127.0.0.1", kSinkPort);
    sender.start();

    // ---- 3) nullptr 安全：注册之前调宏，应当 no-op ----
    LOG_INFO("must-not-arrive: before set_global");
    LOG_ERROR("must-not-arrive: before set_global e=%d", -1);

    // 注册全局入口，等连接建立（sender 有重连，这里等一下确保已连上再发，
    // 让计数判定不依赖断连补发时序）
    LogSender::set_global(&sender);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // ---- 4) 各级别发已知条数 ----
    for (int i = 0; i < kInfoCount; ++i)  LOG_INFO("info #%d", i);
    for (int i = 0; i < kDebugCount; ++i) LOG_DEBUG("debug #%d val=%s", i, "x");
    for (int i = 0; i < kWarnCount; ++i)  LOG_WARN("warn #%d", i);
    for (int i = 0; i < kErrorCount; ++i) LOG_ERROR("error #%d code=%d", i, i * 2);

    // 留足时间让 50ms tick 把队列全部 drain + send 到 sink
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    // ---- 5) 注销后再调宏，应当 no-op ----
    LogSender::set_global(nullptr);
    LOG_INFO("must-not-arrive: after set_global(nullptr)");
    LOG_WARN("must-not-arrive: after set_global(nullptr)");

    // ---- 6) 优雅停止 ----
    sender.stop();
    // 再等一下确保 sink 收完最后的字节，再 quit sink
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    sink_loop->quit();
    sink_thread.join();
    // sender_thread 析构会 quit sender_loop + join

    // ---- 7) 判定 ----
    const int expected_total = kInfoCount + kDebugCount + kWarnCount + kErrorCount;
    std::cout << "[test_logging] received: info=" << tally.info
              << " debug=" << tally.debug
              << " warn=" << tally.warn
              << " error=" << tally.error
              << " other=" << tally.other
              << " total=" << tally.total()
              << " | expected_total=" << expected_total << std::endl;

    bool ok = true;
    auto check = [&](const char* name, int got, int want) {
        if (got != want) {
            std::cout << "[FAIL] " << name << " got=" << got << " want=" << want << std::endl;
            ok = false;
        }
    };
    check("info",  tally.info,  kInfoCount);
    check("debug", tally.debug, kDebugCount);
    check("warn",  tally.warn,  kWarnCount);
    check("error", tally.error, kErrorCount);
    check("other", tally.other, 0);
    check("total", tally.total(), expected_total);

    if (ok) {
        std::cout << "[PASS] all LOG_* delivered with correct levels; "
                     "nullptr-safe before/after registration" << std::endl;
        return 0;
    }
    std::cout << "[FAIL] test_logging assertions failed" << std::endl;
    return 1;
}
