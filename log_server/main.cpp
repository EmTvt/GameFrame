// log_server: 独立的日志收集进程
//
// 职责（第一步，最小可用）：
//   - 监听 TCP 端口（默认 9000）
//   - 复用 epoll_proj 的 TcpServer，每来一条客户端连接就维护一个解码状态
//   - 协议：4 字节大端 length + payload（payload 直接当作一条日志写盘）
//   - 落盘：通过 LogFile 按文件大小滚动
//
// 故意不做的事（避免过度设计）：
//   - 不做协议版本号 / type 字段：先跑通最朴素的，等接入 AsyncLogger 时再加
//   - 不做 ACK：日志是 fire-and-forget，客户端只关心 TCP 层送达
//   - 不做多线程：单 EventLoop 处理网络 + 落盘，足够
//
// 解码：复用 util/length_prefixed_codec.h（client/server 共享，避免拆帧逻辑两份）
//
// 解耦点：
//   - 与 epoll_proj 业务代码 0 耦合：只依赖 src/ 里的网络库组件
//   - LogFile 不知道网络，TcpServer 不知道日志格式

#include <netinet/in.h>
#include <signal.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "log_server/log_file.h"
#include "util/buffer.h"
#include "src/connection.h"
#include "src/event_loop.h"
#include "util/length_prefixed_codec.h"
#include "src/server.h"

using epoll_proj::Buffer;
using epoll_proj::ConnectionPtr;
using epoll_proj::EventLoop;
using epoll_proj::LengthPrefixedCodec;
using epoll_proj::TcpServer;
using log_server::LogFile;

namespace {

// 单条最大允许长度，防止恶意/异常 length 把内存吃爆。
// 比 codec 默认的 16 MiB 更严：日志单条不该这么大。
constexpr uint32_t kMaxFrameSize = 1 * 1024 * 1024;   // 1 MiB

// 周期 flush 间隔。1s 是吞吐和"最坏丢多少"的折中：
//   - 进程被 kill 时，最坏丢这 1s 内尚未冲到内核的日志
//   - 进程优雅退出（SIGINT/SIGTERM）时走 LogFile 析构 fflush+fclose，0 丢失
constexpr int64_t kFlushIntervalMs = 1000;

EventLoop* g_loop_for_signal = nullptr;
void handle_signal(int /*sig*/) {
    if (g_loop_for_signal) g_loop_for_signal->quit();
}

// 自递归装下一次 flush。回调里若 log_file 还活着就 flush + 再挂下一次。
// 用 weak_ptr 避免定时器和 LogFile 互相延寿（其实当前 log_file 由 main 拥有，
// 用 shared 也行；weak 是为未来 LogFile 被多处持有时的安全网）。
void schedule_flush(EventLoop* loop, std::weak_ptr<LogFile> weak_log) {
    loop->run_after(kFlushIntervalMs, [loop, weak_log]() {
        auto log = weak_log.lock();
        if (!log) return;        // LogFile 已析构（理论上不会，main 还在）
        log->flush();
        schedule_flush(loop, weak_log);   // 链式自递归，下一次仍在 loop 线程
    });
}

}  // namespace

int main(int argc, char* argv[]) {
    // 简单参数：端口、日志目录、滚动大小
    // basedir 不存在时 LogFile 会自动 mkdir -p，不需要外部预创建
    uint16_t port = 9099;
    std::string basedir = "./res/log";
    std::string basename = "epoll_proj";
    std::size_t roll_size = 10 * 1024 * 1024;   // 10 MiB：单文件写满即翻号到下一个 .log.N

    if (argc >= 2) port = static_cast<uint16_t>(std::atoi(argv[1]));
    if (argc >= 3) basedir = argv[2];
    if (argc >= 4) {
        // 第 4 个参数：roll_size，单位 MiB（命令行写 "10" 就是 10 MiB，便于压测时调小验证）
        const int mb = std::atoi(argv[3]);
        if (mb > 0) roll_size = static_cast<std::size_t>(mb) * 1024 * 1024;
    }

    auto log_file = std::make_shared<LogFile>(basedir, basename, roll_size);

    // log_server 单线程：num_threads = 0
    TcpServer server(port, /*num_threads=*/0);

    // 信号处理：SIGINT/SIGTERM 转 loop->quit()，让 server.run() 返回，main 走完
    // 析构 → log_file 析构 → fflush + fclose，0 丢失退出。
    // SIGPIPE：客户端断开时 write 不要让进程挂掉，直接忽略。
    g_loop_for_signal = server.main_loop();
    struct sigaction sa{};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;   // 不加 SA_RESTART：让 epoll_wait 被打断，立刻处理 quit
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
    ::signal(SIGPIPE, SIG_IGN);

    // 启动周期 flush（在 mainLoop 上挂第一发，后续在回调里自递归续）
    schedule_flush(server.main_loop(), log_file);

    server.set_connection_callback([](const ConnectionPtr& conn) {
        std::fprintf(stderr, "[log_server] %s %s\n",
                     conn->connected() ? "connected:" : "disconnected:",
                     conn->peer().c_str());
    });

    server.set_message_callback([log_file](const ConnectionPtr& conn, Buffer& input) {
        // 拆帧用共享 codec：协议改了只改 codec 一处。
        // decode 返回 false 表示遇到非法 length（0 或超限），buffer 已对不上边界，必须断连。
        std::vector<std::string> msgs;
        if (!LengthPrefixedCodec::decode(input, msgs, kMaxFrameSize)) {
            std::fprintf(stderr,
                         "[log_server] bad frame from %s, closing\n",
                         conn->peer().c_str());
            conn->close();
            return;
        }
        // 正常路径：把这一轮解出的所有完整帧写盘。
        // append_line 会自动加 "[时间] " 前缀和末尾换行，payload 本身不应含换行。
        for (auto& payload : msgs) {
            log_file->append_line(payload);
        }
    });

    std::fprintf(stderr, "[log_server] listening on :%u, basedir=%s\n",
                 port, basedir.c_str());
    server.run();
    std::fprintf(stderr, "[log_server] shutting down, final flush\n");
    // log_file 这里随作用域结束析构，析构里 fflush + fclose，落盘完成
    return 0;
}
