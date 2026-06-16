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

#include <iostream>
#include <memory>
#include <thread>

#include "src/buffer.h"
#include "src/connection.h"
#include "src/event_loop.h"
#include "src/server.h"

namespace {

constexpr int64_t kIdleTimeoutMs = 10000;   // 10 秒空闲就踢

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
        std::cout << "[idle] kicking fd=" << c->fd()
                  << " (" << c->peer() << ") after "
                  << kIdleTimeoutMs << "ms idle" << std::endl;
        c->close();
    });
}

}  // namespace

int main() {
    constexpr uint16_t kPort = 8888;
    constexpr size_t   kIoThreads = 4;   // 0 = 单 Reactor；>0 = 主从 Reactor

    epoll_proj::TcpServer server(kPort, kIoThreads);

    // 连接建立 → 挂 IdleCtx 并装第一个 idle timer
    // 连接断开 → cancel 还活着的 timer，context 会随 Connection 析构自动释放
    server.set_connection_callback(
        [](const epoll_proj::ConnectionPtr& conn) {
            if (conn->connected()) {
                // 建立：在 subLoop 线程里跑（server 派过来的）
                conn->set_context(std::make_shared<IdleCtx>());
                arm_idle_timer(conn);
                std::cout << "[app] connected fd=" << conn->fd()
                          << " (" << conn->peer() << ")" << std::endl;
            } else {
                // 断开：cancel 还没到点的 timer，省得它白白拖到 10s 后才被回收
                auto ctx = conn->context<IdleCtx>();
                if (ctx && ctx->timer_id) {
                    conn->loop()->cancel_timer(ctx->timer_id);
                    ctx->timer_id = 0;
                }
                std::cout << "[app] disconnected fd=" << conn->fd()
                          << " (" << conn->peer() << ")" << std::endl;
            }
        });

    server.set_message_callback(
        [](const epoll_proj::ConnectionPtr& conn, epoll_proj::Buffer& input) {
            std::string data = input.retrieve_all_as_string();

            std::cout << "[app tid=" << std::this_thread::get_id() << "] fd="
                      << conn->fd() << " (" << conn->peer() << ") recv "
                      << data.size() << " bytes: ";
            std::cout.write(data.data(), static_cast<std::streamsize>(data.size()));
            std::cout.flush();

            conn->send(data);

            // 有数据来 → 把 deadline 往后推
            arm_idle_timer(conn);
        });

    server.run();
    return 0;
}
