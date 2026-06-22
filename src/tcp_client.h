// TcpClient: 主动连出去的 TCP 客户端（非阻塞 connect + 指数退避重连）
//
// 与 TcpServer 的对称性：
//   TcpServer 在 listen_fd 上 accept 出新 conn 后构造 Connection；
//   TcpClient 在自己创建的 socket 上非阻塞 connect，握手成功后构造 Connection。
//   一旦 Connection 起来，后续的 I/O / close / send / 背压回调 完全复用现成的那一套。
//
// 重连策略（muduo 一致）：
//   - 失败/对端关闭/出错 → 关 fd → run_after(backoff_) 再来一次
//   - backoff_：500ms → 1s → 2s → ... → 30s 封顶
//   - 任意一次成功 → backoff_ 复位回 500ms
//   - disconnect() 显式断开：retry_enabled_ = false，定时器即使到了也不会重新拨号
//
// 线程模型（与 EventLoop 一致）：
//   - 构造 + connect/disconnect 必须在 loop 线程做（或者通过 loop->run_in_loop 派进来）
//   - connection() 返回的指针可在任意线程读，但实际 send() 要么在 loop 线程，要么由
//     上层（LogSender）通过 loop->run_in_loop 派进来 —— 跟 TcpServer 侧的约定一致
//
// 教学项目简化：
//   - 不支持运行期改 host:port，要换地址先 disconnect 再新建
//   - 不支持 IPv6（只走 AF_INET），保持和 TcpServer 同一档简化度
//   - 不做 connect 超时（一次 connect 在内核默认 ~75s 超时；要更短自己加 timer）

#pragma once

#include <netinet/in.h>

#include <cstdint>
#include <memory>
#include <string>

#include "connection.h"

namespace epoll_proj {

class EventLoop;
class Channel;

class TcpClient {
public:
    using ConnectionCallback = std::function<void(const ConnectionPtr&)>;
    using MessageCallback    = Connection::MessageCallback;

    // host 当前只支持 IPv4 点分十进制（如 "127.0.0.1"）。
    // 不做 DNS 解析：教学项目不引 getaddrinfo 的依赖，用户自己解析。
    TcpClient(EventLoop* loop, std::string host, uint16_t port);
    ~TcpClient();

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    // 业务回调：建议在 connect() 之前设置。
    // 复用 TcpServer 风格：connection 建立和断开走同一回调，业务靠 conn->connected() 区分。
    void set_connection_callback(ConnectionCallback cb) { connection_cb_ = std::move(cb); }
    void set_message_callback(MessageCallback cb)        { message_cb_ = std::move(cb); }

    // 启动：把"开始非阻塞 connect"派到 loop 线程
    // 可任意线程调；幂等（已经 kConnected 或 kConnecting 时直接返回）
    void connect();

    // 主动断开：设置 retry_enabled_=false，然后在 loop 线程里 close 当前 conn
    // 已断开则只关 retry 标志
    void disconnect();

    // 当前连接（连上才非空；连接中 / 重试间隙均为 nullptr）。
    // 业务在 connection_callback 里拿到的 ConnectionPtr 更可靠；这里是个便利接口。
    ConnectionPtr connection() const { return connection_; }

    bool connected() const { return connection_ && connection_->connected(); }

private:
    enum class State {
        kDisconnected,   // 没在连，也不重连（disconnect 后的终态）
        kConnecting,     // 已发起非阻塞 connect，等 EPOLLOUT 报告结果
        kConnected,      // 握手成功，connection_ 有效
        kRetrying,       // 上一轮挂了，定时器到点会回到 kConnecting
    };

    // 全在 loop 线程执行 —— 调用方负责 run_in_loop 派进来
    void connect_in_loop();
    void disconnect_in_loop();

    // 创建非阻塞 socket，发起 connect。
    // 三种立即返回情况：
    //   - 立刻成功（极少见，loopback 偶尔会）：直接走 on_connected
    //   - EINPROGRESS：挂上 connect_channel_ 等 EPOLLOUT
    //   - 其他 errno：关 fd，安排重试
    void start_connect();

    // EPOLLOUT 到达时：getsockopt(SO_ERROR) 看握手结果
    //   - 0：成功，构造 Connection，channel 转交（旧的 connect_channel_ 销毁）
    //   - 非 0：失败，关 fd，安排重试
    void handle_connect_writable();

    // Connection 通过 close_cb 通知我们："我死了"
    // 我们清掉 connection_，然后 schedule_retry（如果还允许重连）
    void on_connection_closed(const ConnectionPtr& conn);

    // 安排下一次重连：run_after(backoff_)，更新 backoff_（指数退避到上限）
    // retry_enabled_ == false 时不挂定时器，直接回 kDisconnected
    void schedule_retry();

    // 把 fd 设为 O_NONBLOCK。失败返回 false（调用方负责关 fd）
    static bool set_nonblocking(int fd);

    EventLoop* loop_;
    const std::string host_;
    const uint16_t port_;
    sockaddr_in server_addr_;   // 构造时解析一次，重连不再 parse

    State state_ = State::kDisconnected;
    bool retry_enabled_ = false;     // connect() 时翻 true，disconnect() 翻 false

    // 当前正在 connect 的 fd（kConnecting 时有效，其他状态 = -1）。
    // 握手成功后所有权转给 Connection，本字段清回 -1。
    int connecting_fd_ = -1;

    // 临时 channel：仅在 kConnecting 期间挂 EPOLLOUT 等握手结果
    // 握手完成（成功或失败）后立即销毁；fd 的去向另由 Connection 接管 / 自行 close
    std::unique_ptr<Channel> connect_channel_;

    // 握手成功后的连接对象。close 后清回 nullptr。
    // shared_ptr 是因为 Connection 自己要 enable_shared_from_this（同 TcpServer 侧）
    ConnectionPtr connection_;

    // 指数退避当前等待时长（毫秒）。每次失败翻倍，上限 kMaxBackoffMs。
    // 任意一次成功握手 → 复位到 kInitialBackoffMs。
    int64_t backoff_ms_ = 0;
    static constexpr int64_t kInitialBackoffMs = 500;
    static constexpr int64_t kMaxBackoffMs     = 30 * 1000;

    ConnectionCallback connection_cb_;
    MessageCallback    message_cb_;
};

}  // namespace epoll_proj
