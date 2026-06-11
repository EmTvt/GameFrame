// TcpServer: 基于 epoll 的多客户端 TCP 服务器骨架
//
// 当前职责（第一步重构）：
//   - 创建 / 绑定 / 监听套接字
//   - 创建 epoll 实例，挂上 listen_fd
//   - 事件循环：accept 新连接（生成 Connection），分发可读事件到对应 Connection
//   - 维护 fd -> Connection 的映射
//   - 提供"收到消息"业务回调接口给使用方注入
//
// 不负责：
//   - 协议解析（业务在 MessageCallback 里自己处理）
//   - 业务线程池（单线程 Reactor）

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "connection.h"

namespace epoll_proj {

class TcpServer {
public:
    // 业务消息回调签名（直接复用 Connection::MessageCallback）
    using MessageCallback = Connection::MessageCallback;

    explicit TcpServer(uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // 设置收到数据时的业务回调（在 accept 出新连接时会注入到每个 Connection）
    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }

    // 启动事件循环（阻塞，永不返回，除非致命错误）
    void run();

private:
    void create_listen_socket();          // socket + bind + listen + 非阻塞
    void handle_accept();                 // listen_fd 可读：循环 accept
    void remove_connection(Connection& conn);  // Connection 关闭时回调到这里

    uint16_t port_;
    int listen_fd_ = -1;
    int epfd_ = -1;

    // fd -> Connection 的映射；unique_ptr 自动管理生命周期
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    MessageCallback message_cb_;
};

}  // namespace epoll_proj
