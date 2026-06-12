// TcpServer: 基于 EventLoop 的多客户端 TCP 服务器
//
// 拆分动作（第六步预备）：
//   - epoll 的创建 / wait / ctl 全部下沉到 EventLoop
//   - TcpServer 只负责：监听 socket、accept、Connection 生命周期管理
//   - 通过向 EventLoop 注册"fd → 事件回调"挂上自己
//
// Connection 的接口完全不变，所以这次拆分对业务代码（main.cpp）零影响。

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "connection.h"
#include "event_loop.h"

namespace epoll_proj {

class TcpServer {
public:
    using MessageCallback = Connection::MessageCallback;

    explicit TcpServer(uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }

    void run();

private:
    void create_listen_socket();
    void handle_accept();
    void handle_conn_event(int fd, uint32_t events);  // 单个连接 fd 的事件分发
    void remove_connection(int fd);                   // 由 Connection 关闭时回调
    void update_events(int fd, uint32_t events);      // 由 Connection 调整关注事件

    uint16_t port_;
    int listen_fd_ = -1;

    EventLoop loop_;

    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    MessageCallback message_cb_;
};

}  // namespace epoll_proj
