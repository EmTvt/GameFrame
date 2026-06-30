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

#include "util/buffer.h"

namespace epoll_proj {

class EventLoop;
class Channel;
class Connection;

// 统一别名：所有跨函数边界传递 Connection 的地方都用这个，
// 让"谁可能在异步路径里持有连接"在类型上一眼可见
using ConnectionPtr = std::shared_ptr<Connection>;

// 让 Connection 能调 shared_from_this()，把"自己"以 shared_ptr 形式
// 传给业务回调和异步 functor —— 任何一个还活着的 shared_ptr 都能延后 Connection 析构。
// 这是为了支撑：
//   1) 业务回调在异步链里持有 Connection（协程、定时器、跨 loop 任务）
//   2) close() 流程中防止"close_cb 中途 Connection 被析构 → 自我 UAF"
class Connection : public std::enable_shared_from_this<Connection> {
public:
    enum class State {
        kConnected,
        kDisconnecting,   // 半关状态：写端已 shutdown 或 pending
        kDisconnected,
    };

    // 回调签名升级：传 const ConnectionPtr& 而不是 Connection&
    //   - 业务可以把它存到 session map / 协程 frame / 定时器里，安全延长生命周期
    //   - const& 避免每次调用多一次 atomic 引用计数操作
    using MessageCallback = std::function<void(const ConnectionPtr& conn, Buffer& input)>;
    using CloseCallback   = std::function<void(const ConnectionPtr& conn)>;

    // ---- 背压（back-pressure）相关回调 ----
    //
    // HighWaterMark：output_buffer_ 大小"从低于阈值跨过阈值"的瞬间触发一次
    //   - 边沿触发，不是水平触发：避免业务在过载窗口内被回调淹没
    //   - 业务收到后可选择：暂停发送 / 切到丢弃模式 / 关闭连接
    //
    // WriteComplete：output_buffer_ 排空（从非空 → 空）的瞬间触发一次
    //   - 与 HighWaterMark 配对，业务靠这个信号知道"可以恢复发送了"
    //
    // 两个回调都在 conn 归属的 loop 线程里、通过 queue_in_loop 异步触发，
    // 不会在 send() / handle_write() 内部同步执行 —— 防止业务回调再次 send/close
    // 导致状态机递归。
    using HighWaterMarkCallback = std::function<void(const ConnectionPtr& conn)>;
    using WriteCompleteCallback = std::function<void(const ConnectionPtr& conn)>;

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

    // 设置高水位阈值 + 回调。mark == 0 表示禁用（默认）。
    // 必须在 conn 投入使用之前设置 —— 不打算支持运行期热改，
    // 否则需要把 mark / cb 写到 loop 线程才安全
    void set_high_water_mark_callback(size_t mark, HighWaterMarkCallback cb) {
        high_water_mark_ = mark;
        high_water_cb_ = std::move(cb);
    }

    // output_buffer_ 排空时的通知。同样要求在投入使用前设置。
    void set_write_complete_callback(WriteCompleteCallback cb) {
        write_complete_cb_ = std::move(cb);
    }

    // ---------- 业务上下文挂载 ----------
    // 每条连接附挂任意业务对象（idle timer id、HTTP 解析器、会话状态等）。
    //
    // 为什么用 shared_ptr<void> 而不是 std::any：
    //   - any 每次取值都要 any_cast + RTTI 检查；shared_ptr<void> 是纯指针
    //   - 业务往往希望"持有同一份上下文"的多个引用都活着，shared_ptr 天然支持
    //
    // 约定：只有 loop 线程能 set/get（Connection 本来就这条线程的）—— 无需加锁。
    template <typename T>
    void set_context(std::shared_ptr<T> ctx) { context_ = std::move(ctx); }

    // 取上下文。调用方负责保证 T 类型正确（错了是 UB，static_pointer_cast 不做检查）。
    // 没设过 context 时返回 nullptr。
    template <typename T>
    std::shared_ptr<T> context() const {
        return std::static_pointer_cast<T>(context_);
    }

    bool has_context() const { return static_cast<bool>(context_); }

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
    // 语义：立即双向关，不管 output_buffer_ 里有没有未发完的数据。
    void close();

    // 优雅半关（shutdown 写端）：
    //   - 若 output_buffer_ 已空 → 立刻 ::shutdown(fd_, SHUT_WR)
    //   - 若 output_buffer_ 非空 → 置 shutdown_pending_ 标志，等 handle_write()
    //     把数据写完后再执行 ::shutdown(SHUT_WR)
    // 半关后：不再向对端写入，仍可读（等对端 FIN）。
    // 跨线程安全：内部走 run_in_loop。
    void shutdown();

private:
    void shutdown_in_loop();
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

    // ---- 背压回调与阈值 ----
    // high_water_mark_ == 0 表示禁用：所有跨阈值判断短路掉，零开销
    HighWaterMarkCallback high_water_cb_;
    WriteCompleteCallback write_complete_cb_;
    size_t high_water_mark_ = 0;

    // 半关标志：shutdown() 时 outbuf 非空，标记为 true；
    // handle_write() 写空 outbuf 后检查此标志，再补 ::shutdown(SHUT_WR)
    bool shutdown_pending_ = false;

    // 业务上下文 —— 类型擦除，由业务通过 set_context<T>/context<T>() 使用
    std::shared_ptr<void> context_;
};

}  // namespace epoll_proj
