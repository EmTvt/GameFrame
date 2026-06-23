// LogSender: 把业务线程产生的日志通过 TcpClient 发到独立的 log_server 进程
//
// 这一层做的事很少，但每一件都"位置确定"——它把已经写好的几块拼起来：
//
//   业务线程 push(string) ─┐
//                          │  (MPSCQueue<string>，util/mpsc_queue.h)
//                          ▼
//                    LogSender (本类) —— 跑在自己的 EventLoop 上
//                          │
//                          │  每 50ms 定时 drain → encode → send
//                          ▼
//                    TcpClient (src/tcp_client.h) —— 自带指数退避重连
//                          │
//                          ▼
//                       log_server
//
// 为什么独立成一个类（而不是直接在 main 里用 MPSCQueue + TcpClient 拼）：
//   - drain ↔ TcpClient.connection() 的耦合点不小：要判 connected()、要看
//     output_buffer_ 是否过阈值、要在 disconnect 期间停止 drain。
//   - LOG_DROPPED 合成消息要插在批次开头，这个逻辑跟 push 路径无关，跟 drain
//     时序强相关，归属 Sender 而非 Queue。
//   - 未来 LOG_INFO 宏要找一个稳定的入口点 push(string)，把这个入口集中起来。
//
// 线程模型：
//   - LogSender::push 可在任意业务线程调（只 push 到 MPSCQueue，零阻塞）
//   - 其它公共方法（start/stop/连接回调注册）必须在 owner 线程调；其中
//     start/stop 内部会 run_in_loop 派进 sender_loop_，调用方不用再包一层
//   - sender_loop_ 是外部传进来的：一般是 EventLoopThread::start_loop() 的返回值，
//     由调用方持有，与 TcpClient 风格一致（让"loop 怎么来"由上层决定）
//
// 断连期间的行为：
//   - TcpClient 自己负责重连（500ms→30s 指数退避），LogSender 不掺和重连决策
//   - 断连期间 push 仍然有效：消息正常入队；满了由 MPSCQueue 自己丢最新 + dropped_++
//   - drain tick 期间若发现没有可用 connection，直接跳过这一轮（不 take_dropped、
//     不消耗任何队列元素），把数据继续留在队列里给重连后发
//   - 重连成功后下一次 tick 自然恢复 drain；dropped_ 不会因此丢失，因为它在
//     MPSCQueue 内部以 atomic<uint64_t> 累加，take_dropped() 调用前一直累计
//
// 背压协作：
//   - 给 Connection 设了 high_water_mark：output_buffer_ 跨过这个阈值时
//     HighWaterMarkCallback 触发，Sender 进入 paused_ 状态（drain tick 直接跳过）
//   - WriteCompleteCallback 触发后恢复 paused_=false，下次 tick 正常 drain
//   - 没有这个机制的话：log_server 慢消费 → TcpClient 这边 output_buffer_ 无限增长
//     → 进程内存爆掉。muduo 也是同样的玩法
//
// 教学项目简化：
//   - 不做"等剩余消息发完才退出"的精确语义。stop() 会做一次最后 drain + send，
//     然后调 disconnect()。带 timeout 是想留接口位，第一版直接 best-effort。
//   - 不暴露 metrics（queue_size / dropped_total 当前周期等）；要观测打开 stderr
//     诊断输出（[DIAG log_sender] 前缀）即可。

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "src/event_loop.h"
#include "src/tcp_client.h"
#include "util/mpsc_queue.h"

namespace epoll_proj {

class LogSender {
public:
    struct Options {
        // MPSCQueue 容量。日志一条按平均 200B 估算，10000 条 ≈ 2 MiB 内存，
        // 足以扛住 log_server 短暂抖动（几秒到十几秒）。
        size_t queue_capacity = 10000;

        // 每次 tick 最多 drain 多少条 —— 防止一次性把所有日志拼成超大批次
        // 把 Connection::output_buffer_ 瞬间撑大。
        size_t max_batch_per_tick = 1000;

        // tick 周期：50ms。低吞吐场景下日志最多延迟 50ms 落到 server，对调试
        // 完全够用；高吞吐场景下边沿唤醒 + 批量 drain 会自动摊薄。
        int64_t tick_interval_ms = 50;

        // Connection output_buffer_ 高水位。跨过这个阈值 Sender 暂停 drain，
        // 直到 WriteComplete 排空再恢复。4 MiB 是经验值：足够大不至于在正常
        // 网络抖动下频繁触发，又不至于把本进程内存吃太多。
        size_t high_water_mark = 4 * 1024 * 1024;
    };

    // sender_loop 必须是一个已经在专属线程上跑起来的 EventLoop（由调用方持有，
    // 一般是 EventLoopThread::start_loop() 的返回值）。
    // host/port 直接转给内部 TcpClient（不支持 DNS，IPv4 点分十进制）。
    // 两个重载：把"用默认 Options"和"显式传 Options"分开声明，回避
    // "类内默认实参里引用本类嵌套类型默认成员初始化"的 C++ 解析限制
    LogSender(EventLoop* sender_loop, std::string host, uint16_t port);
    LogSender(EventLoop*  sender_loop,
              std::string host,
              uint16_t    port,
              Options     opts);
    ~LogSender();

    LogSender(const LogSender&) = delete;
    LogSender& operator=(const LogSender&) = delete;

    // 启动：派任务到 sender_loop_ 上挂第一次 tick + 调 TcpClient.connect()
    // 幂等：重复调忽略
    void start();

    // 停止：派任务到 sender_loop_ 上：取消 tick → 最后一次 drain + send →
    //   tcp_client_.disconnect()。同步等待 sender_loop_ 完成这套动作。
    //
    // 注意：本函数会阻塞当前线程直到 sender_loop_ 处理完。不要在 sender_loop_
    //   线程内部调（会死锁）。一般在主线程析构前调一次即可。
    void stop();

    // 业务侧入口：任意线程可调，零阻塞。
    //   - 满则丢最新，内部计入 MPSCQueue::dropped_
    //   - 返回值：是否真的入队成功；返回 false 表示这条消息已被丢弃
    //   - 不抛异常（push 路径要绝对稳）
    bool push(std::string msg);

    // 仅供测试 / 监控用。
    size_t queue_size() const { return queue_.size(); }

private:
    // 全在 sender_loop_ 线程跑
    void start_in_loop();
    void stop_in_loop();
    void schedule_next_tick();    // 把 on_tick 挂到 tick_interval_ms 后
    void on_tick();               // 定时器到点：drain → encode → send

    // TcpClient 状态变化时回调（在 sender_loop_ 上跑）
    void on_connection(const ConnectionPtr& conn);

    // 背压回调（也在 sender_loop_ 上跑）
    void on_high_water(const ConnectionPtr& conn);
    void on_write_complete(const ConnectionPtr& conn);

    // 把一批字符串编码后通过当前 connection_ 发出。
    // 调用方负责确认 connection_ 非空且 connected==true 且 paused_=false。
    // 把 LOG_DROPPED 合成消息插在批次开头由 do_drain_and_send 统一处理。
    void send_batch(const std::vector<std::string>& batch);

    // 真正干活的核心：
    //   1) take_dropped()，若 >0 合成 "[LOG_DROPPED] N messages lost" 入批
    //   2) drain(out, max_batch_per_tick)
    //   3) send_batch(out)
    // 调用方负责 connection 检查、paused 检查 —— 本函数不再判
    void do_drain_and_send();

    EventLoop* const sender_loop_;
    const Options    opts_;

    // TcpClient 由 LogSender 持有 + 拥有：构造时 new、析构时 delete
    // 用 unique_ptr 不是因为它会被多处共享，而是为了让构造时序更清晰
    // （TcpClient 构造需要 sender_loop_，自然先构造 sender_loop 再构造它）
    std::unique_ptr<TcpClient> tcp_client_;

    // 业务侧 push 的队列。move-only string，符合 MPSCQueue<T> 要求
    MPSCQueue<std::string> queue_;

    // 当前持有的 connection。on_connection 里更新；只在 sender_loop_ 线程读写
    ConnectionPtr connection_;

    // 背压暂停标志：高水位回调里翻 true，WriteComplete 翻 false
    // 只在 sender_loop_ 线程读写，无需 atomic
    bool paused_ = false;

    // 周期 tick 的 TimerId。stop_in_loop 里 cancel
    // EventLoop::TimerId 是个 uint64_t，0 当"无定时器"哨兵 —— 这与 TimerQueue
    // 内部从 1 开始派号相吻合（详见 timer_queue.cpp）
    EventLoop::TimerId tick_timer_id_ = 0;

    // 启动 / 停止状态。push 不看这俩，只看 queue 本身；start/stop 自己幂等。
    bool started_ = false;
    bool stopped_ = false;
};

}  // namespace epoll_proj
