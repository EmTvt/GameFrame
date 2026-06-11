// Connection: 单个 TCP 连接的抽象
//
// 当前职责（第一步重构，先做最小可用版本）：
//   - 持有 conn_fd 与对端地址信息（ip:port）
//   - 维护连接状态（连接中 / 已断开）
//   - 封装 read / send / close 系统调用，对上层屏蔽 errno 细节
//   - 通过回调把"收到数据后怎么处理"交给业务层（解耦 I/O 与业务）
//
// 后续步骤会扩展：
//   - 读缓冲区 / 写缓冲区
//   - EPOLLOUT 管理（write 不完时的暂存）
//   - 协议解析（粘包/拆包）
//   - 空闲超时

#pragma once

#include <netinet/in.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace epoll_proj {

class Connection {
public:
    enum class State {
        kConnected,    // 已连接，可读写
        kDisconnected, // 已关闭，等待回收
    };

    // 收到数据时的回调签名：
    //   conn ：当前连接对象（业务可用它回写、关闭）
    //   data ：本次读取到的字节数据（注意 TCP 流式，可能是半个包）
    using MessageCallback = std::function<void(Connection& conn, std::string_view data)>;

    // 关闭事件回调（对端关闭 / 出错时由 Connection 通知给上层 Server 移除自己）
    using CloseCallback = std::function<void(Connection& conn)>;

    // 构造：托管一个已经 accept 出来的 conn_fd（应该是非阻塞的）
    Connection(int fd, const sockaddr_in& peer_addr);

    // 禁止拷贝（持有 fd 资源），允许移动也不必要，直接禁止
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // 析构：若仍持有 fd 则 close，避免泄漏
    ~Connection();

    // ===== 基本访问器 =====
    int fd() const { return fd_; }
    State state() const { return state_; }
    bool connected() const { return state_ == State::kConnected; }
    const std::string& peer() const { return peer_; }  // "ip:port"

    // ===== 业务回调注入 =====
    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void set_close_callback(CloseCallback cb)     { close_cb_ = std::move(cb); }

    // ===== I/O 接口（由 Server 的事件循环调用）=====

    // EPOLLIN 触发时调用：循环 read 直到 EAGAIN，把数据通过 message_cb_ 抛给业务。
    // 遇到对端关闭 / 错误时，会把状态置为 kDisconnected 并调用 close_cb_。
    void handle_read();

    // 业务侧主动发送数据。
    // 当前实现：直接写；写不完的部分会打印警告并丢弃（后续步骤会换成写缓冲区+EPOLLOUT）。
    // 返回成功写出的字节数。
    ssize_t send(std::string_view data);

    // 主动关闭连接：关闭 fd、状态变 Disconnected、触发 close_cb_。
    // 可由业务层在 message_cb_ 里调用。
    void close();

private:
    int fd_;
    std::string peer_;     // "ip:port"，仅用于日志
    State state_;

    MessageCallback message_cb_;
    CloseCallback   close_cb_;
};

}  // namespace epoll_proj
