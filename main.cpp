// 入口：演示 echo 服务 + idle timeout（10 秒没数据自动关闭连接）
//
// 测试方式：
//   终端1：./build/epoll_proj
//   终端2：telnet 127.0.0.1 8888  # 发不发数据都会在 10s 后被踢
//
// 这个 demo 同时在验证三件东西：
//   1) TimerQueue：定时回调触发
//   2) Connection::set_context<T>：每条连接附挂自己的 TimerId，业务零全局表
//   3) TcpServer::set_connection_callback：连接建立/断开两端都有挂钩点，
//      idle timer 在"建立"时就装上 —— 客户端连进来不发任何数据也照样会被踢

#include <signal.h>

#include <iostream>
#include <memory>
#include <thread>

#include "util/buffer.h"
#include "src/connection.h"
#include "src/event_loop.h"
#include "src/event_loop_thread.h"
#include "src/server.h"
#include "log_sender/log_sender.h"
#include "log_sender/logging.h"

namespace {

constexpr int64_t kIdleTimeoutMs = 10000;   // 10 秒空闲就踢

// 信号 → 让 server 的 mainLoop quit，使 server.run() 返回，main 走完优雅收尾
// （注销全局 LogSender + sender.stop()）。与 log_server/main.cpp 同款做法。
epoll_proj::EventLoop* g_server_loop = nullptr;
void handle_signal(int /*sig*/) {
    if (g_server_loop) g_server_loop->quit();
}

// 每条连接挂一份这个上下文（通过 Connection::set_context）。
// 只在 conn 所属的 subLoop 里访问，无锁。
struct IdleCtx {
    epoll_proj::EventLoop::TimerId timer_id = 0;
};

// 装/重置 idle timer：
//   1) 取消上一个（如果有）—— 否则旧 timer 到点还会触发，errantly 关掉已经活跃的连接
//   2) 装一个新的，10s 后踢
//
// 回调里用 weak_ptr<Connection>：避免"timer 持有 conn 强引用 → 连接关闭后还得等 10s
//   才真正析构"的生命周期拖尾。weak_ptr.lock() 失败说明 conn 早就走了，no-op。
void arm_idle_timer(const epoll_proj::ConnectionPtr& conn) {
    auto* loop = conn->loop();
    auto  ctx  = conn->context<IdleCtx>();

    if (ctx->timer_id) {
        loop->cancel_timer(ctx->timer_id);
        ctx->timer_id = 0;
    }

    std::weak_ptr<epoll_proj::Connection> weak = conn;
    ctx->timer_id = loop->run_after(kIdleTimeoutMs, [weak]() {
        auto c = weak.lock();
        if (!c || !c->connected()) return;  // 已经关了：no-op
        // 业务事件落盘：空闲踢人属于"值得关注但非错误"，用 WARN 级别
        LOG_WARN("idle kick fd=%d peer=%s after %lldms idle",
                 c->fd(), c->peer().c_str(),
                 static_cast<long long>(kIdleTimeoutMs));
        c->close();
    });
}

}  // namespace

int main() {
    constexpr uint16_t kPort = 8888;
    constexpr size_t   kIoThreads = 4;   // 0 = 单 Reactor；>0 = 主从 Reactor

    // ---- 业务侧日志接入 ----
    // LogSender 必须跑在自己专属的 EventLoop 线程上（与 server 的 io 线程隔离）。
    // 连本机 log_server 的 9099（与 test_log_sender / CLAUDE.md 约定一致）。
    // log_server 没起也不影响业务：TcpClient 会指数退避重连，日志暂存在 MPSCQueue，
    //   连上后补发；队列满则丢最新并由 LOG_DROPPED 暴露。
    epoll_proj::EventLoopThread log_thread;
    epoll_proj::EventLoop*      log_loop = log_thread.start_loop();
    epoll_proj::LogSender       log_sender(log_loop, "127.0.0.1", 9099);
    log_sender.start();
    epoll_proj::LogSender::set_global(&log_sender);   // 注册：业务线程起来之前

    epoll_proj::TcpServer server(kPort, kIoThreads);

    // 信号：SIGINT/SIGTERM → quit server mainLoop → run() 返回走收尾。
    // SIGPIPE 忽略：对端断开时 write 不要打挂进程。
    g_server_loop = server.main_loop();
    struct sigaction sa{};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
    ::signal(SIGPIPE, SIG_IGN);

    // 连接建立 → 挂 IdleCtx 并装第一个 idle timer
    // 连接断开 → cancel 还活着的 timer，context 会随 Connection 析构自动释放
    server.set_connection_callback(
        [](const epoll_proj::ConnectionPtr& conn) {
            if (conn->connected()) {
                // 建立：在 subLoop 线程里跑（server 派过来的）
                conn->set_context(std::make_shared<IdleCtx>());
                arm_idle_timer(conn);
                // 业务事件落盘：连接建立
                LOG_INFO("connected fd=%d peer=%s", conn->fd(), conn->peer().c_str());
            } else {
                // 断开：cancel 还没到点的 timer，省得它白白拖到 10s 后才被回收
                auto ctx = conn->context<IdleCtx>();
                if (ctx && ctx->timer_id) {
                    conn->loop()->cancel_timer(ctx->timer_id);
                    ctx->timer_id = 0;
                }
                LOG_INFO("disconnected fd=%d peer=%s", conn->fd(), conn->peer().c_str());
            }
        });

    server.set_message_callback(
        [](const epoll_proj::ConnectionPtr& conn, epoll_proj::Buffer& input) {
            std::string data = input.retrieve_all_as_string();

            // 收包明细走 DEBUG：高频、调试用。
            // 想在日志里看到具体内容，但又不能让 payload 里的 \r\n 破坏 log_server
            //   "一行一条" 的格式 —— 所以先做成单行预览：换行替换为空格、超长截断。
            std::string preview = data;
            for (char& c : preview) {
                if (c == '\n' || c == '\r') c = ' ';
            }
            constexpr size_t kPreviewMax = 200;
            if (preview.size() > kPreviewMax) {
                preview.resize(kPreviewMax);
                preview += "...";
            }
            LOG_DEBUG("recv %zu bytes from fd=%d peer=%s | \"%s\"",
                      data.size(), conn->fd(), conn->peer().c_str(), preview.c_str());

            // 演示性的业务错误路径：收到含 "BOOM" 的数据就当作一次业务异常，打 ERROR。
            //   纯粹为了让 LOG_ERROR 在 demo 里有一个可手动触发的入口
            //   （echo demo 本身没有真实错误场景）。客户端发 -m BOOM 即可看到。
            if (data.find("BOOM") != std::string::npos) {
                LOG_ERROR("business error triggered by client fd=%d peer=%s (payload had 'BOOM')",
                          conn->fd(), conn->peer().c_str());
            }

            conn->send(data);

            // 有数据来 → 把 deadline 往后推
            arm_idle_timer(conn);
        });

    std::cout << "[app] echo server listening on :" << kPort
              << " | logging to log_server 127.0.0.1:9099" << std::endl;
    server.run();

    // ---- 优雅收尾 ----
    // server.run() 因信号返回后：先注销全局指针（杜绝 stop 期间还有业务线程 LOG_*
    //   拿到悬空 sender），再 stop（cancel tick → 最后一次 drain+send → 在 log_loop
    //   线程内 reset TcpClient）。log_thread 析构会 quit log_loop + join。
    std::cout << "[app] shutting down, flushing logs..." << std::endl;
    epoll_proj::LogSender::set_global(nullptr);
    log_sender.stop();
    return 0;
}
