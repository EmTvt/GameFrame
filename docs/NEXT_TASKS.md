# Next Tasks

> 接下来要做什么，按"投入产出比 + 当前痛点驱动"排序。**不要让 AI 自己猜下一步**。

## 最终目标架构（业务进程的日志接入）

```
业务线程  LOG_INFO("xxx")  ──→  MPSCQueue (lock-free, 有界, 满则丢最旧)
                                       │
                                       ▼
                       LogSender 线程 (独立 EventLoop)
                                       │ 定时 drain + length-prefix 编码
                                       ▼
                                   TcpClient
                                       │ 断线指数退避重连
                                       ▼
                                  ─── TCP ───
                                       │
                                       ▼
                              LogServer 进程
                              (单 EventLoop, 已实现)
                              拆帧 → LogFile 落盘
```

**核心设计决策**：
- LogServer 独立进程：挂了不拖慢业务，业务通过指数退避重连恢复
- 业务侧 `LOG_INFO` 零阻塞：纯内存 push 到 MPSCQueue
- LogSender 独立线程：drain 队列后通过 TcpClient 发出
- MPSC：多业务线程写日志 → 一个 Sender 线程消费

---

## 短期（log_server 闭环 / 按实现顺序）

### Task 1. `LengthPrefixedCodec` — 编解码共享组件

**文件**：`src/length_prefixed_codec.h`（纯头文件，无状态）

**职责**：把 `log_server/main.cpp` lambda 里的拆帧逻辑抽成 client/server 共享组件。

**接口草案**：
```cpp
namespace epoll_proj {
struct LengthPrefixedCodec {
    // 4 字节大端 length + payload；返回可直接 conn->send 的字符串
    static std::string encode(std::string_view payload);

    // 从 input 中循环拆帧，把完整消息追加到 out
    // 帧长度上限校验（防异常 length 吃爆内存）
    // 返回 false 表示遇到非法帧（业务通常应关闭连接）
    static bool decode(Buffer& input, std::vector<std::string>& out,
                       uint32_t max_frame_len = 16 * 1024 * 1024);
};
}
```

**要做的事**：
- 写头文件 + 内联实现
- `log_server/main.cpp` 切到用 `LengthPrefixedCodec::decode`
- 加单元测试或 smoke 验证

### Task 2. `MPSCQueue<T>` — 无锁有界队列

**文件**：`src/mpsc_queue.h`

**核心特性**：
- **有界**：容量上限（如 10000 条 / 10 MB），满时丢弃**最旧**日志（防 OOM）
- **多生产者**：任意业务线程并发 push（先 mutex 跑通，再考虑 lock-free 优化）
- **单消费者**：只有 LogSender 线程 drain
- **批量 drain**：`drain(vector<T>& out, size_t max_batch)` 一次取一批

**唤醒策略**：
- push 后设 `atomic<bool> has_pending_ = true`
- LogSender 的 EventLoop 上挂短周期定时器（50ms）定时检查 + drain
- 只在 `false → true` 翻转时额外 wakeup（边沿通知，避免高频 wakeup）
- 高吞吐 → 自动批量；低吞吐 → 延迟最多 50ms（日志场景可接受）

### Task 3. `LogSender` — 日志发送器

**文件**：`log_sender/log_sender.{h,cpp}`（新目录）

**职责**：组装 MPSCQueue + TcpClient + 独立 EventLoop。

- 持有 `EventLoopThread`（自己跑一个 loop 线程）
- 持有 `TcpClient`（连 LogServer）
- 持有 `MPSCQueue<string>`（接业务日志）
- 定时 drain → `LengthPrefixedCodec::encode` → `tcp_client.send`
- LogServer 断线期间：日志继续入队（不阻塞业务），队列满则丢最旧

**生命周期**：
- 全局单例（或 main 中创建后传给 LOG 宏）
- 进程退出：stop() → drain 剩余 → 等发送完成（带超时） → 断开连接

### Task 4. `LOG_INFO` 宏 — 业务侧接口

**文件**：`log_sender/logging.h`

**设计**：
```cpp
#define LOG_INFO(fmt, ...) \
    do { \
        std::string msg = epoll_proj::format_log( \
            epoll_proj::LogLevel::INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        epoll_proj::LogSender::instance().push(std::move(msg)); \
    } while(0)
```

- 格式化在调用线程完成（避免跨线程传 va_list）
- push 到 MPSCQueue 是纯内存操作，纳秒级
- 支持 `LOG_DEBUG / LOG_INFO / LOG_WARN / LOG_ERROR`
- 编译期宏开关关闭低级别日志（零开销）

---

## 中期（网络库补完）

- [ ] **`shutdown(SHUT_WR)` 半关支持**：`Connection::close()` 现在直接关，没区分"我还有 outbuf 没发完"。muduo 的 `shutdown()` 在 outbuf 排空后才真关。
- [ ] **`Connection::force_close_with_delay(ms)`**：HTTP 之类协议 close 前要给客户端最后写一点数据。
- [ ] **集成测试**：补一个"TcpServer 多线程压测 + Connection 析构顺序"的脚本/可执行。
- [ ] **Acceptor 抽出**：listen channel/handle_accept 从 `TcpServer` 独立成 `Acceptor`（mirrors muduo），目前不是痛点。

## 长期（看情况）

- [ ] log_server 加**多 IO 线程**：accept 用 mainLoop，conn 分给 subLoop；每个 subLoop 一个 LogFile（按线程分片）或统一 LogFile（加锁/MPSC）。
- [ ] **HttpServer demo**：HTTP/1.1 解析作为第二个真实业务，验证 `Connection::set_context<T>` 挂解析器状态机。
- [ ] **压力对比**：跟 muduo / asio echo 同硬件 throughput / p99 对比。
