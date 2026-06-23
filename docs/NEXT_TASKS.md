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

### Task 1. `LengthPrefixedCodec` — 编解码共享组件 ✅ Done (2026-06-18)

**文件**：`util/length_prefixed_codec.h`（纯头文件，无状态）

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

### Task 2. `MPSCQueue<T>` — 多生产者单消费者有界队列 ✅ Done (2026-06-18)

**文件**：`util/mpsc_queue.h`（拆分到 util/ 见 DECISIONS.md 2026-06-18 "util/ 目录拆分"条）

**核心特性**：
- **有界**：容量上限（如 10000 条 / 10 MB），满时**丢最新**（push 直接返回 false，不动队列）—— 与 muduo / spdlog 一致，理由见 `DECISIONS.md` 2026-06-18 条
- **dropped 计数**：push 失败时累加 `atomic<uint64_t> dropped_`；LogSender drain 时若 `dropped_ > 0`，在该批日志前合成一条 `[LOG_DROPPED] N messages lost` 并清零，避免静默丢失
- **多生产者**：任意业务线程并发 push（先 mutex 跑通，再考虑 lock-free 优化）
- **单消费者**：只有 LogSender 线程 drain
- **批量 drain**：`drain(vector<T>& out, size_t max_batch)` 一次取一批

**接口草案**：
```cpp
template <class T>
class MPSCQueue {
public:
    explicit MPSCQueue(size_t capacity);
    bool   push(T v);                              // 满则返回 false，dropped_ + 1
    size_t drain(std::vector<T>& out, size_t max_batch);
    uint64_t take_dropped();                       // 原子读出并清零（LogSender 用）
    size_t size() const;
};
```

**唤醒策略**：
- push 后设 `atomic<bool> has_pending_ = true`
- LogSender 的 EventLoop 上挂短周期定时器（50ms）定时检查 + drain
- 只在 `false → true` 翻转时额外 wakeup（边沿通知，避免高频 wakeup）
- 高吞吐 → 自动批量；低吞吐 → 延迟最多 50ms（日志场景可接受）

### Task 3. `LogSender` — 日志发送器 ✅ Done (2026-06-22)

**文件**：`log_sender/log_sender.{h,cpp}`

**实现要点**（写下来给后续会话定位，别再"猜"细节）：
- 构造签名 `LogSender(EventLoop* sender_loop, host, port, Options)`：sender_loop 由调用方持有（一般是 `EventLoopThread::start_loop()` 的返回值），与 TcpClient 风格一致。提供两个构造重载，避开"类内默认实参里引用嵌套类型默认成员初始化"的 C++ 解析限制。
- 三个内部状态：`connection_`（kConnected 时非空，重连间隙为 nullptr）、`paused_`（HighWater→true，WriteComplete→false）、`tick_timer_id_`（0 当哨兵；TimerQueue 从 1 开始派号，安全）
- tick 自递归：`on_tick` 跑完 drain 后再 `schedule_next_tick()`，避免一次 `run_every`-类语义带来的状态泄漏（cancel 一致性更好查）
- push 边沿唤醒：`MPSCQueue::push` 返回 `wake_up=true` 那次走 `queue_in_loop` 戳醒 sender_loop_，让首条消息能以微秒级延迟落地（不必等下一个 50ms tick）。`on_tick` 自己幂等：无 connection / paused 时直接跳过
- LOG_DROPPED 合成：`do_drain_and_send` 开头调 `take_dropped()`，>0 时在批次最前插一条 `[LOG_DROPPED] N messages lost`
- 析构顺序坑：`~TcpClient` 第一行 `assert_in_loop_thread`。**必须在 stop_in_loop 里 `tcp_client_.reset()`**（在 sender_loop_ 线程销毁），让 LogSender 在 main 线程析构时 unique_ptr 已是 nullptr
- stop 同步等：用 mutex+cv 而不是 future/promise，省一个标准库头依赖

**测试**：`test_log_sender`（4 producer × 10000 条，cap=5000 队列），落盘行数 = ok push + 1 条 LOG_DROPPED 合成，对账精确。

### Task 4. `LOG_*` 宏 — 业务侧接口 ✅ Done (2026-06-23)

**文件**：`log_sender/logging.h`

**最终实现**（与下方早期草案有出入，以此为准）：
```cpp
#define LOG_INFO(fmt, ...) \
    EPOLL_LOG_IMPL(::epoll_proj::LogLevel::INFO, 1, fmt __VA_OPT__(,) __VA_ARGS__)
// EPOLL_LOG_IMPL: do{ if(level<LOG_COMPILE_LEVEL) break;
//                     auto* s=LogSender::global(); if(!s) break;
//                     s->push(format_log(level, __FILE__, __LINE__, fmt, ...)); }while(0)
```

**落地要点**（给后续会话定位）：
- 四级别 `LOG_DEBUG/INFO/WARN/ERROR`，都经 `EPOLL_LOG_IMPL` 统一展开
- `format_log` 在**调用线程**用两遍 `vsnprintf` 格式化，带 `__attribute__((format(printf,4,5)))` 编译期校验；输出 `LEVEL file:line | body`，**不打时间戳**（log_server 的 append_line 统一加，避免两段时间）
- **全局访问点**用 `LogSender::set_global(LogSender*)/global()`，**不是自构造单例**：实例仍由调用方（main）构造拥有，静态成员只存裸指针。决策与理由见 `DECISIONS.md` 2026-06-23 条（草案里 `LogSender::instance()` 的写法已废弃）
- 编译期级别门控 `if ((level_num) < LOG_COMPILE_LEVEL) break;`：两端常量，关掉的级别整段为死代码被消除（零开销）。release 用 `-DLOG_COMPILE_LEVEL=2` 只留 WARN/ERROR
- 用 C++20 `__VA_OPT__(,)` 而非 GNU `##__VA_ARGS__`，符合工程 `-std=c++20`（`CMAKE_CXX_EXTENSIONS OFF`）
- nullptr 安全：`global()` 为空（未注册 / 已注销）时静默跳过，不崩

**业务接入**：`main.cpp`（echo demo）已接 LogSender —— 连接/断开 → LOG_INFO、收包 → LOG_DEBUG、空闲踢人 → LOG_WARN；新增 SIGINT/SIGTERM 优雅退出（quit → `set_global(nullptr)` → `sender.stop()`）。`CMakeLists.txt` 的 `epoll_proj` 目标补了 `${LOG_SENDER_SOURCES}`。

**测试**：`test_logging`（自包含，进程内 mini sink，无需外部 log_server）。四级别各 400 条端到端计数精确匹配（other=0），并覆盖 set_global 前 / set_global(nullptr) 后的 no-op；`./build/test_logging` 返回码即结论。

---

### Task 4 早期草案（保留存档；以上方"最终实现"为准）

最初草案用的是自构造单例 `LogSender::instance().push(...)`：
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
