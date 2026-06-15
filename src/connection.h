// Connection: 单个 TCP 连接的抽象
//
// 第七步演进（引入 Channel）：
//   - 不再持有 events_ 和 update_events_cb_
//   - 持一个 Channel，把 read/write/close/error 回调挂上去
//   - epoll 关注事件的开关 = channel_->enable_writing() / disable_writing()
//
// 业务侧的对外接口（MessageCallback / CloseCallback / send / close 等）保持不变。

#pragma once

#include <netinet/in.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "buffer.h"

namespace epoll_proj {

class EventLoop;
class Channel;

class Connection {
public:
    enum class State {
        kConnected,
        kDisconnected,
    };

    using MessageCallback = std::function<void(Connection& conn, Buffer& input)>;
    using CloseCallback   = std::function<void(Connection& conn)>;

    // 新增 loop 参数：Channel 需要它来调 update_channel / remove_channel
    Connection(EventLoop* loop, int fd, const sockaddr_in& peer_addr);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int fd() const { return fd_; }
    State state() const { return state_; }
    bool connected() const { return state_ == State::kConnected; }
    const std::string& peer() const { return peer_; }
    EventLoop* loop() const { return loop_; }   // TcpServer 析构时按 conn 归属派任务用

    Buffer& input_buffer() { return input_buffer_; }
    const Buffer& output_buffer() const { return output_buffer_; }

    void set_message_callback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void set_close_callback(CloseCallback cb)     { close_cb_ = std::move(cb); }

    // 真正把 channel 注册到 epoll 上 —— 让构造和注册分离，
    // 调用方 (TcpServer::handle_accept) 在 connections_ 表填好后再 enable_reading()，
    // 这样万一 EPOLLIN 立刻触发也能在 close_cb 里 erase 到自己。
    void start();

    // EPOLLIN：循环 read 到 input_buffer_，再回调给业务
    void handle_read();

    // EPOLLOUT：把 output_buffer_ 中的数据继续 write 出去
    void handle_write();

    // 业务侧主动发数据
    void send(std::string_view data);

    // 主动关闭（也会被 read=0 / 出错时内部调用）
    void close();

private:
    EventLoop* loop_;
    int fd_;
    std::string peer_;
    State state_;

    // Channel：本连接在 epoll 上的代言人
    // unique_ptr 是因为 Channel 不可拷贝/不可移动（持有 EventLoop*）
    std::unique_ptr<Channel> channel_;

    Buffer input_buffer_;
    Buffer output_buffer_;

    MessageCallback message_cb_;
    CloseCallback close_cb_;
};

}  // namespace epoll_proj
