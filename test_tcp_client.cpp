// test_tcp_client: TcpClient 的 smoke 测试
//
// 覆盖的事：
//   1) 正常拨号 + 收到 connection_callback（connected=true）
//   2) 用 LengthPrefixedCodec::encode 拼几条帧 → send
//   3) 等 1 秒（让数据走完，log_server 落盘）
//   4) 主动 disconnect → 收到 connection_callback（connected=false）
//   5) （可选）再次 connect 验证 backoff 复位 + 可重新拨号
//
// 用法：
//   终端 1: ./build/log_server 9099 ./build/res/log
//   终端 2: ./build/test_tcp_client            # 默认连 127.0.0.1:9099
//           ./build/test_tcp_client 127.0.0.1 9099
//
// 也可以**故意不启动 log_server** 运行本程序，观察 stderr 上的退避日志：
//   "[tcp_client] connect ... failed immediately: Connection refused"
//   "[tcp_client] will retry ... in 500 ms"   → 1000 → 2000 → ...
// 然后再起 log_server，应当能恢复连接。
//
// 不是单元测试：失败靠肉眼看 stderr / log_server 落盘文件。先把链路打通，
// 后面 LogSender 上线后再写真正的端到端断言。

#include <signal.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "src/event_loop.h"
#include "src/tcp_client.h"
#include "util/length_prefixed_codec.h"

using namespace epoll_proj;

int main(int argc, char** argv) {
    // 业务进程一律忽略 SIGPIPE（对端关闭后我们仍可能 write → SIGPIPE → 默认杀进程）
    ::signal(SIGPIPE, SIG_IGN);

    std::string host = "127.0.0.1";
    uint16_t    port = 9099;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = static_cast<uint16_t>(std::stoi(argv[2]));

    EventLoop loop;
    TcpClient client(&loop, host, port);

    client.set_connection_callback([](const ConnectionPtr& conn) {
        if (conn->connected()) {
            std::cout << "[test] connected to " << conn->peer() << std::endl;
        } else {
            std::cout << "[test] disconnected from " << conn->peer() << std::endl;
        }
    });

    client.set_message_callback([](const ConnectionPtr&, Buffer& input) {
        // log_server 不会回数据，但留着以防本程序被改去连别的 server
        std::cout << "[test] got " << input.readable_bytes() << " bytes back" << std::endl;
        input.retrieve_all();
    });

    // 拨号
    client.connect();

    // 连上后发 3 条日志，然后 2 秒后 disconnect，再 2 秒后 quit
    // 全部通过 run_after 派进 loop 线程 —— 哪怕 connect 还没握手成功也没关系，
    //   send 时会判 connected()，没连上就不发（这里 1 秒等握手已经够了）
    loop.run_after(1000, [&client]() {
        if (auto conn = client.connection()) {
            for (int i = 0; i < 3; ++i) {
                std::string payload =
                    "[test_tcp_client] hello #" + std::to_string(i);
                conn->send(LengthPrefixedCodec::encode(payload));
            }
            std::cout << "[test] sent 3 frames" << std::endl;
        } else {
            std::cout << "[test] not connected yet, skip send" << std::endl;
        }
    });

    loop.run_after(3000, [&client]() {
        std::cout << "[test] calling disconnect()" << std::endl;
        client.disconnect();
    });

    loop.run_after(5000, [&loop]() {
        std::cout << "[test] quit" << std::endl;
        loop.quit();
    });

    loop.loop();
    std::cout << "[test] bye" << std::endl;
    return 0;
}
