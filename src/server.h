// TcpServer: 基于 epoll 的多客户端 TCP 服务器
//
// 第二步新增：
//   - 处理 EPOLLOUT 事件（分发到 Connection::handle_write）
//   - 暴露 update_events 接口给 Connection 用于动态开关 EPOLLOUT
//   - Connection::close 时记下 fd 由 Server 统一处理 epoll DEL，不再"遍历找对象"

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "connection.h"

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
    void remove_connection(int fd);                  // 由 Connection 关闭时回调
    void update_events(int fd, uint32_t events);     // 由 Connection 调整关注事件

    uint16_t port_;
    int listen_fd_ = -1;
    int epfd_ = -1;

    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    MessageCallback message_cb_;
};

}  // namespace epoll_proj
