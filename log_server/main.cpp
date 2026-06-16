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
//   - 不抽 LengthPrefixedCodec：第 2 步统一做（client/server 共享）
//   - 不做多线程：单 EventLoop 处理网络 + 落盘，足够
//
// 解耦点：
//   - 与 epoll_proj 业务代码 0 耦合：只依赖 src/ 里的网络库组件
//   - LogFile 不知道网络，TcpServer 不知道日志格式

#include <netinet/in.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "log_server/log_file.h"
#include "src/buffer.h"
#include "src/connection.h"
#include "src/event_loop.h"
#include "src/server.h"

using epoll_proj::Buffer;
using epoll_proj::ConnectionPtr;
using epoll_proj::TcpServer;
using log_server::LogFile;

namespace {

// 4 字节大端读法。Buffer 没有 peek_int32_be，所以这里手动从 readable 区前 4 字节取。
// 调用方应保证 buf.readable_bytes() >= 4。
uint32_t peek_uint32_be(const Buffer& buf) {
    const char* p = buf.peek();
    uint32_t n = 0;
    n |= static_cast<uint8_t>(p[0]) << 24;
    n |= static_cast<uint8_t>(p[1]) << 16;
    n |= static_cast<uint8_t>(p[2]) << 8;
    n |= static_cast<uint8_t>(p[3]);
    return n;
}

// 单条最大允许长度，防止恶意/异常 length 把内存吃爆
constexpr uint32_t kMaxFrameSize = 1 * 1024 * 1024;   // 1 MiB

}  // namespace

int main(int argc, char* argv[]) {
    // 简单参数：端口、日志目录、滚动大小
    uint16_t port = 9000;
    std::string basedir = ".";
    std::string basename = "epoll_proj";
    std::size_t roll_size = 64 * 1024 * 1024;   // 64 MiB

    if (argc >= 2) port = static_cast<uint16_t>(std::atoi(argv[1]));
    if (argc >= 3) basedir = argv[2];

    auto log_file = std::make_shared<LogFile>(basedir, basename, roll_size);

    // log_server 单线程：num_threads = 0
    TcpServer server(port, /*num_threads=*/0);

    // 每条连接的解码状态：这里 buffer 已经由 Connection 提供（input_buffer_），
    // 我们只需要在 message_cb 里循环"拆帧"即可，不需要额外状态对象。
    server.set_connection_callback([](const ConnectionPtr& conn) {
        std::fprintf(stderr, "[log_server] %s %s\n",
                     conn->connected() ? "connected:" : "disconnected:",
                     conn->peer().c_str());
    });

    server.set_message_callback([log_file](const ConnectionPtr& conn, Buffer& input) {
        // 循环拆帧：可能一次 EPOLLIN 带来好多条日志，必须吃干净
        while (true) {
            if (input.readable_bytes() < 4) return;   // 头还没收齐

            const uint32_t len = peek_uint32_be(input);
            if (len == 0 || len > kMaxFrameSize) {
                std::fprintf(stderr,
                             "[log_server] bad frame length=%u from %s, closing\n",
                             len, conn->peer().c_str());
                conn->close();
                return;
            }

            if (input.readable_bytes() < 4 + len) return;   // body 还不够

            // 头收齐 + body 收齐 → 取一条
            input.retrieve(4);
            std::string payload = input.retrieve_as_string(len);

            // 写盘。LogFile 不加换行，所以这里补一个，方便 tail -f 看
            log_file->append(payload);
            log_file->append("\n");
        }
    });

    std::fprintf(stderr, "[log_server] listening on :%u, basedir=%s\n",
                 port, basedir.c_str());
    server.run();
    return 0;
}
