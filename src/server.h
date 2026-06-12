// TcpServer: 基于 EventLoop 的多客户端 TCP 服务器
//
// 第七步演进（引入 Channel）：
//   - 不再持有 update_events_cb_ 这条注入链：Connection 自己通过 channel 改事件
//   - 不再有 handle_conn_event：事件分发回到 Connection 自己（由它的 Channel 调用 read/write/close/error_cb）
//   - listen_fd 用一个 accept_channel_ 包起来，与 Connection 路径对称
//
// 业务对外接口（set_message_callback / run）保持不变。

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "connection.h"
#include "event_loop.h"

namespace epoll_proj {

class Channel;

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
    void remove_connection(int fd);   // 由 Connection 关闭时回调

    uint16_t port_;
    int listen_fd_ = -1;

    EventLoop loop_;

    // listen_fd 的 epoll 代言人：把 accept 路径也纳入 Channel 体系
    // 注意 unique_ptr —— Channel 持 EventLoop*，必须在 loop_ 构造完后才能 new
    std::unique_ptr<Channel> accept_channel_;

    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    MessageCallback message_cb_;
};

}  // namespace epoll_proj
