// TcpServer: 基于 EventLoop 的多客户端 TCP 服务器
//
// 第八步演进（主从 Reactor）：
//   - mainLoop：跑在主线程，只负责 listen / accept
//   - subLoops：由 EventLoopThreadPool 管理，每个一个独立线程
//     accept 出新连接后，round-robin 选一个 subLoop，把 Connection 的 I/O 全部交给它
//   - num_threads = 0 时退化为单 Reactor，业务代码无需感知
//
// 关键并发约定：
//   - connections_ 这张表只在 mainLoop 线程访问（增/删）
//   - Connection 自己的所有 I/O 操作在它归属的 subLoop 线程做
//   - subLoop 触发 close 时，close_cb_ 跨线程派回 mainLoop 才能安全 erase

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "connection.h"
#include "event_loop.h"

namespace epoll_proj {

class Channel;
class EventLoopThreadPool;

class TcpServer {
public:
    using MessageCallback    = Connection::MessageCallback;
    // 连接建立 / 断开都用这一个回调，业务靠 conn->connected() 区分（muduo 风格）。
    //   - 建立时：state==kConnected，可以挂 idle timer、初始化 context
    //   - 断开时：state==kDisconnected，可以清理 timer、释放 context（虽然 context
    //     会随 Connection 析构自动释放，但 timer 这种"外部资源"必须显式 cancel）
    using ConnectionCallback = std::function<void(const ConnectionPtr&)>;

    // num_threads == 0 → 单 Reactor，所有 I/O 仍在主线程（兼容旧用法）
    // num_threads >  0 → 起 N 个 IO 线程作为 subLoop
    explicit TcpServer(uint16_t port, size_t num_threads = 0);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void set_connection_callback(ConnectionCallback cb) { connection_cb_ = std::move(cb); }

    void run();

private:
    void create_listen_socket();
    void handle_accept();
    // 永远在 mainLoop 线程执行
    //   接收 ConnectionPtr by-value：close_cb 里捕获了 ConnectionPtr，派进 mainLoop
    //   pending_functors_ 时连接的生命周期就跟着 functor 走 —— 直到这里 erase 完才释放
    void remove_connection_in_loop(const ConnectionPtr& conn);

    uint16_t port_;
    int listen_fd_ = -1;

    EventLoop loop_;   // mainLoop，跑在构造它的线程（= 主线程）
    std::unique_ptr<EventLoopThreadPool> thread_pool_;

    // listen_fd 的 epoll 代言人，挂在 mainLoop 上
    std::unique_ptr<Channel> accept_channel_;

    // 仅由 mainLoop 线程访问 —— 不需要锁
    // shared_ptr：让业务和异步回调可以安全持有 Connection 跨事件，
    //   map 不再是连接的唯一 owner
    std::unordered_map<int, ConnectionPtr> connections_;

    MessageCallback message_cb_;
    ConnectionCallback connection_cb_;
};

}  // namespace epoll_proj
