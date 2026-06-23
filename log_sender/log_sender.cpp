#include "log_sender/log_sender.h"

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <utility>
#include <vector>

#include "src/connection.h"
#include "util/length_prefixed_codec.h"

namespace epoll_proj {

LogSender::LogSender(EventLoop* sender_loop, std::string host, uint16_t port)
    : LogSender(sender_loop, std::move(host), port, Options{}) {}

LogSender::LogSender(EventLoop*  sender_loop,
                     std::string host,
                     uint16_t    port,
                     Options     opts)
    : sender_loop_(sender_loop),
      opts_(opts),
      tcp_client_(std::make_unique<TcpClient>(sender_loop, std::move(host), port)),
      queue_(opts.queue_capacity) {
    // 注册到 TcpClient 上的回调一律 capture this：TcpClient 的寿命由本对象保证，
    //   ~LogSender 之前会先 stop()，stop 内部 disconnect 会让 TcpClient 不再回调
    tcp_client_->set_connection_callback(
        [this](const ConnectionPtr& conn) { on_connection(conn); });
    // 不挂 message_cb：log_server 不会回写任何内容。挂个空回调也无妨，
    //   但为了让"我们不期待收到任何数据"在代码上可见，留空。
}

LogSender::~LogSender() {
    // 防御性兜底：用户没显式 stop() 就析构 → 这里再来一次。
    // stop() 内部会 assert "不能在 sender_loop_ 线程里调" —— 析构必须发生在
    //   sender_loop_ 之外（一般是 main 线程）才安全。
    if (started_ && !stopped_) {
        stop();
    }
}

// ---------------- start / stop ----------------

void LogSender::start() {
    // 跨线程调安全：start_in_loop 内部判幂等
    sender_loop_->run_in_loop([this]() { start_in_loop(); });
}

void LogSender::start_in_loop() {
    sender_loop_->assert_in_loop_thread();
    if (started_) return;
    started_ = true;

    tcp_client_->connect();    // TcpClient 自己负责非阻塞 connect + 重连
    schedule_next_tick();      // 先把第一次 tick 排上
}

void LogSender::stop() {
    if (sender_loop_->in_loop_thread()) {
        // 死锁防护：sender_loop_ 自己 quit 后再回来调 stop() 才会走这里。
        // 这种情况下直接同步调 stop_in_loop —— 反正 loop 也已经停了
        stop_in_loop();
        return;
    }

    // 跨线程：派任务 + 同步等。用一个简单的 mutex+cv 等 stop_in_loop 跑完。
    // 不用 future/promise 是想让本类不依赖 <future>，编译更轻
    std::mutex              mu;
    std::condition_variable cv;
    bool                    done = false;

    sender_loop_->run_in_loop([this, &mu, &cv, &done]() {
        stop_in_loop();
        {
            std::lock_guard<std::mutex> lk(mu);
            done = true;
        }
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&done]() { return done; });
}

void LogSender::stop_in_loop() {
    sender_loop_->assert_in_loop_thread();
    if (stopped_) return;
    stopped_ = true;

    // 1) 关 tick：cancel 对未触发 ID 有效，对已触发 ID 是 no-op
    if (tick_timer_id_ != 0) {
        sender_loop_->cancel_timer(tick_timer_id_);
        tick_timer_id_ = 0;
    }

    // 2) 最后一次 drain + send：尽量把队列里的东西在断开前送出去
    //    paused_ 状态下也强行试一把：调用方主动 stop，剩余消息留在 output_buffer_
    //    交给 Connection 自己排空（disconnect 不会瞬间断开，会等 close 流程走完）
    //    没连上则跳过，没法兜
    if (connection_ && connection_->connected()) {
        do_drain_and_send();
    }

    // 3) 断开 TcpClient（这一步会让 retry_enabled_ = false，不会再自动重连）
    //    然后**在 loop 线程里**销毁 tcp_client_ —— TcpClient::~TcpClient 第一行
    //    会 assert_in_loop_thread，必须在这里跑，不能等 LogSender 在 main 线程析构
    //    时再 reset
    tcp_client_->disconnect();
    connection_.reset();    // disconnect 之后 close_cb 已经清过一次，这里幂等
    tcp_client_.reset();    // ←关键：在 sender_loop_ 线程销毁 TcpClient
}

// ---------------- push ----------------

bool LogSender::push(std::string msg) {
    auto r = queue_.push(std::move(msg));
    // 边沿唤醒：业务侧第一条到达时戳一下 sender_loop_，让它马上跑一次 tick
    //   而不是等下一个 50ms 周期。低吞吐时延迟从 50ms 降到 ~微秒级
    //   高吞吐时 wake_up 只有 false→true 那一次为 true，不会高频 wakeup
    //
    // 我们派一个 functor 进去 —— EventLoop::queue_in_loop 会 wakeup epoll_wait。
    // 这里不挂"额外的提前 tick"，只是借用 wakeup 让 sender_loop_ 醒来；它醒来之后
    // 如果有别的事情要处理（比如已有 tick functor 在排队）会一并跑掉。
    //
    // 注意：我们不在 push 路径里直接同步 do_drain_and_send —— push 必须保持
    // 最短临界区，drain 涉及 send / encode / buffer 操作，开销大且要触 loop 线程
    // 资源，不能在业务线程里干
    if (r.wake_up) {
        sender_loop_->queue_in_loop([this]() {
            // 这是个"借用 wakeup 顺手把 tick 提前跑一遍"的快路径。
            // started_ 还没置位时（极早期 push）：跳过，等正式 tick 起来再发
            // stopped_ 之后：跳过，stop_in_loop 已经做了最后一次 drain
            if (!started_ || stopped_) return;
            on_tick();
        });
    }
    return r.ok;
}

// ---------------- tick ----------------

void LogSender::schedule_next_tick() {
    sender_loop_->assert_in_loop_thread();
    if (stopped_) return;

    tick_timer_id_ = sender_loop_->run_after(opts_.tick_interval_ms, [this]() {
        // 到点后 timer 已被消费（run_after 是一次性的），id 失效
        tick_timer_id_ = 0;
        on_tick();
    });
}

void LogSender::on_tick() {
    sender_loop_->assert_in_loop_thread();
    if (stopped_) return;

    // 三道闸门：
    //   1) connection_ 是否非空（重连间隙为 nullptr）
    //   2) Connection 是否真处于 kConnected
    //   3) 背压有没有把我们暂停（output_buffer_ 还没排空）
    // 任何一道不过：跳过本轮 drain，让数据继续待在 MPSCQueue 里
    if (connection_ && connection_->connected() && !paused_) {
        do_drain_and_send();
    }

    // 不论是否真的 drain，都要把下一轮 tick 排上 —— 否则一旦因为 paused 跳过
    // 就再也没人帮我们排队下次 tick 了
    schedule_next_tick();
}

void LogSender::do_drain_and_send() {
    sender_loop_->assert_in_loop_thread();
    // 进入这里时已经满足：connection_ 非空 + connected + !paused_

    // 1) 先拿 dropped 计数：只有 >0 才合成一条暴露消息
    //    顺序上要在 drain 之前 take：take 完了 push 路径上新累计的 dropped
    //    会在下一轮 tick 暴露 —— 不会丢，只是延迟一轮
    uint64_t dropped = queue_.take_dropped();

    std::vector<std::string> batch;
    if (dropped > 0) {
        // 不挤占 max_batch_per_tick 的额度 —— 这只是一条诊断消息
        batch.reserve(opts_.max_batch_per_tick + 1);
        batch.emplace_back("[LOG_DROPPED] " + std::to_string(dropped) +
                           " messages lost");
    } else {
        batch.reserve(opts_.max_batch_per_tick);
    }

    size_t took = queue_.drain(batch, opts_.max_batch_per_tick);
    if (took == 0 && dropped == 0) {
        // 真没东西：直接返回
        return;
    }

    send_batch(batch);
}

void LogSender::send_batch(const std::vector<std::string>& batch) {
    // 调用前已确认 connection_ 非空 + connected
    // 编码 + send 都在 sender_loop_ 线程里跑，与 Connection 所在 loop 一致，
    //   走 Connection::send 的快路径（无 run_in_loop 派发开销）
    for (const auto& payload : batch) {
        connection_->send(LengthPrefixedCodec::encode(payload));
    }
}

// ---------------- TcpClient 回调 ----------------

void LogSender::on_connection(const ConnectionPtr& conn) {
    sender_loop_->assert_in_loop_thread();

    if (conn->connected()) {
        // 新连接：挂背压回调（必须在 conn 投入使用 ≈ start() 之前设置；
        // TcpClient 是先调 connection_cb 再 conn->start()，时机正好）
        if (opts_.high_water_mark > 0) {
            conn->set_high_water_mark_callback(
                opts_.high_water_mark,
                [this](const ConnectionPtr& c) { on_high_water(c); });
            conn->set_write_complete_callback(
                [this](const ConnectionPtr& c) { on_write_complete(c); });
        }

        connection_ = conn;
        paused_     = false;
        // 不在这里立刻 drain：on_tick 50ms 之后会做，避免在 connection_cb
        //   同步路径里又同步 send 一大批东西，让握手刚成功的栈太深
    } else {
        // 断了：清掉，下次 tick 自然跳过；TcpClient 会自己安排重连
        connection_.reset();
        paused_ = false;
    }
}

void LogSender::on_high_water(const ConnectionPtr& /*conn*/) {
    sender_loop_->assert_in_loop_thread();
    paused_ = true;
    std::cerr << "[log_sender] paused: output buffer over high water mark ("
              << opts_.high_water_mark << " bytes)" << std::endl;
}

void LogSender::on_write_complete(const ConnectionPtr& /*conn*/) {
    sender_loop_->assert_in_loop_thread();
    if (paused_) {
        paused_ = false;
        std::cerr << "[log_sender] resumed: output buffer drained" << std::endl;
    }
}

}  // namespace epoll_proj
