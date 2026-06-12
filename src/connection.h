// Connection: 单个 TCP 连接的抽象（第二步：引入读/写缓冲区）
//
// 本步新增能力：
//   - input_buffer_ ：内核数据先 read 到这里，业务从 buffer 里按协议解析消息
//   - output_buffer_：业务 send 写不完的数据暂存这里，等 EPOLLOUT 再继续发
//   - handle_write()：被 Server 在 EPOLLOUT 触发时调用
//   - 通过 update_events_cb_ 通知 Server 修改 epoll 关注的事件（开/关 EPOLLOUT）
//
// 业务回调签名变更：
//   旧：void(Connection&, std::string_view)   一次给完
//   新：void(Connection&, Buffer&)             业务自己决定消费多少
//   原因：协议解析需要"看到不完整数据时不消费，等下次拼起来"

#pragma once

#include <netinet/in.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "buffer.h"

namespace epoll_proj {

class Connection {
public:
    enum class State {
        kConnected,
        kDisconnected,
    };

    using MessageCallback = std::function<void(Connection& conn, Buffer& input)>;
    using CloseCallback   = std::function<void(Connection& conn)>;
    // 通知 Server 修改本连接在 epoll 上关注的事件位（如开关 EPOLLOUT）
    using UpdateEventsCallback = std::function<void(Connection& conn, uint32_t events)>;

    Connection(int fd, const sockaddr_in& peer_addr);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int fd() const { return fd_; }
    State state() const { return state_; }
    bool connected() const { return state_ == State::kConnected; }
    const std::string& peer() const { return peer_; }

    // 业务读缓冲（也可由业务直接操作）
    Buffer& input_buffer() { return input_buffer_; }
    const Buffer& output_buffer() const { return output_buffer_; }

    void set_message_callback(MessageCallback cb)        { message_cb_ = std::move(cb); }
    void set_close_callback(CloseCallback cb)            { close_cb_ = std::move(cb); }
    void set_update_events_callback(UpdateEventsCallback cb) { update_events_cb_ = std::move(cb); }

    // EPOLLIN：循环 read 到 input_buffer_，再回调给业务
    void handle_read();

    // EPOLLOUT：把 output_buffer_ 中的数据继续 write 出去
    void handle_write();

    // 业务侧主动发数据
    // 行为：
    //   1) 若 output_buffer_ 是空且未关注 EPOLLOUT，先尝试直接 write
    //   2) write 不完 / EAGAIN：把剩余追加到 output_buffer_，并开启 EPOLLOUT
    //   3) 若 output_buffer_ 非空：直接追加到尾部，等 handle_write 处理
    void send(std::string_view data);

    // 主动关闭（也会被 read=0 / 出错时内部调用）
    void close();

private:
    void enable_write_events();    // 开启 EPOLLOUT 关注
    void disable_write_events();   // 关闭 EPOLLOUT 关注

    int fd_;
    std::string peer_;
    State state_;

    // 当前在 epoll 上关注的事件位（默认 EPOLLIN | EPOLLET）
    uint32_t events_;

    Buffer input_buffer_;
    Buffer output_buffer_;

    MessageCallback message_cb_;
    CloseCallback close_cb_;
    UpdateEventsCallback update_events_cb_;
};

}  // namespace epoll_proj
