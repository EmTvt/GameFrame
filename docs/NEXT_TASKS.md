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

## 下一阶段：按依赖顺序逐个实现

> 这几项有明确的依赖关系（半关 → 是 HTTP / 协程正确收尾的前提；HTTP 是协程的最佳首个业务）。
> 建议按 Task 编号顺序做。每张卡给出 文件 / 职责 / 要点 / 验收，实现时不必再猜。

### Task 5. `Connection` 半关 `shutdown(SHUT_WR)` ⬜ 推荐先做

**文件**：`src/connection.{h,cpp}`

**为什么先做**：当前 `Connection::close()` 是「直接关」，不管 `output_buffer_` 里还有没有没发完的数据。这导致一个**已知 bug**：`LogSender::stop()` 做完最后一次 drain+send 后调 `disconnect()`，若 output_buffer 还没排空，数据会随 close 一起丢（现在只能 stop 前 sleep 2s best-effort）。半关做完，这个 bug 才能真正修掉；同时 HTTP「写完响应再关」、协程 `co_await write` 的正确收尾都依赖它。

**要做的事**：
- 新增 `Connection::shutdown()`（对应 muduo 的 `shutdownInLoop`）：
  - 若 `output_buffer_` **已空** → 立刻 `::shutdown(fd_, SHUT_WR)`，进入半关状态（不再写，等对端 FIN）。
  - 若 `output_buffer_` **非空** → 只置一个 `shutdown_pending_ = true` 标志，**不立刻关**；等 `handle_write()` 把 outbuf 写空后，再在那里执行 `::shutdown(SHUT_WR)`。
- `handle_write()` 末尾（outbuf 排空分支）检查 `shutdown_pending_`，是则补一次 `::shutdown(SHUT_WR)`。
- 跨线程安全：`shutdown()` 走 `loop_->run_in_loop(...)`（与 send/close 一致）。
- `close()` 与 `shutdown()` 的语义区分写进头注释：close = 立即双向关；shutdown = 优雅半关（发完再关写端）。

**改完要说明调用链流向**（CLAUDE.md 要求）。

**验收**：
- LogSender 的 `stop()` 改用 `shutdown()` 代替 `disconnect()` 的硬关（或在 disconnect 前先 shutdown），去掉 test 里的 sleep 摊薄后，落盘行数仍对账精确（无尾部丢失）。
- 补一个能复现「outbuf 未排空就关会丢」的小场景，验证半关后不丢。

---

### Task 6. `Connection::force_close_with_delay(ms)` ⬜（小，可选）

**文件**：`src/connection.{h,cpp}`

**职责**：与 Task 5 互补的另一端 —— 有些协议（HTTP 错误响应、限流踢人）要「再写最后一点数据，给对端 N 毫秒读，然后强制关」。

**要点**：
- `run_after(ms, [weak_this]{ if(auto c=weak.lock()) c->close(); })`，用 `weak_ptr<Connection>` 避免延寿。
- 与 `shutdown()` 区别：force_close 是「到点无论如何都关」，shutdown 是「发完才关、可能一直等」。

**验收**：定时强制关路径被走到一次即可（可放进 HTTP demo 一起验）。

---

### Task 7. `HttpServer` demo（HTTP/1.1 解析）⬜

**文件**：`http/`（新目录）：`http_request.{h,cpp}`、`http_response.{h,cpp}`、`http_context.{h,cpp}`（解析状态机）、`http_server.{h,cpp}`、`http_demo_main.cpp`

**职责**：作为**第二个真实业务**，验证两件现有能力：
1. `Connection::set_context<T>()` 挂一个 HTTP 解析器状态机（每连接一份），跨多次 EPOLLIN 续解析（半包）。
2. Task 5 的半关：响应写完后优雅关（`Connection: close`）或保持（`keep-alive`）。

**要点**：
- 解析状态机：`kExpectRequestLine → kExpectHeaders → kExpectBody → kGotAll`，喂 `Buffer&`，半包返回「还要更多」。
- 复用现有 `Buffer`、`message_cb`；HttpServer 内部包一层 TcpServer。
- 先只支持 GET + 固定路由（`/` `/hello`），body/分块传输后续再加。
- keep-alive：解析到 `Connection: keep-alive` 不关，复位状态机继续等下一个请求。

**验收**：`curl http://127.0.0.1:PORT/hello` 拿到正确响应；`curl` 默认 keep-alive 下连发两个请求复用同一连接；业务事件照常经 LOG_* 落盘。

---

### Task 8. 协程层（C++20 coroutine，**与回调层并存，不替换**）⬜ 大工程，拆子步

**前置认知**：epoll / `EventLoop::loop()` / Channel / Buffer / TimerQueue **全部保留**；协程只替换「最上层业务写法」（回调 → 线性 `co_await`）。调度直接复用 `EventLoop::queue_in_loop`（把 `coroutine_handle::resume()` 包成 Functor 投递）。生命周期靠 `Connection` 已有的 `enable_shared_from_this`（见 connection.h 注释，当初就是为协程 frame 留的）。决策记录见 `DECISIONS.md`（实现前补一条）。

按以下子步推进，每步可独立验证：

- **8.1 `coro/task.h`**：`Task<T>` + `promise_type`（detached task 用于 fire-and-forget；带 final-suspend 的用于可 join）。单测：`co_await` 一个立即完成的值能拿到结果。
- **8.2 `co_await sleep_for(ms)`**：包 `EventLoop::run_after`，到点 resume。**这是协程接入 EventLoop 的最小验证点**，做完就摸清了全部接合机制。单测：挂起→定时器 resume→在 loop 线程继续执行。
- **8.3 `ReadAwaiter` / `WriteAwaiter`**：给 `Connection` 加 `read()` / `write()` 协程接口，内部存 `std::coroutine_handle<> waiting_reader_`。`handle_read()`：有挂起 reader 则 resume，否则走旧 message_cb（**两模式共存**）。`WriteAwaiter` 用 `write_complete_cb_` 做「等可写再 resume」，把背压的 `paused_` 标志语义吸收掉。
  - **坑（务必处理）**：① 挂起的协程 frame 必须按值持有 `ConnectionPtr`（防 co_await 期间 conn 析构 → resume 时 UAF）；② `close()` 要 resume 所有挂起 awaiter 并让其感知「连接已死」（返回空/抛异常），否则 frame 泄漏；③ 同一 fd 同时只允许一个 reader 挂起，加 assert。
- **8.4 协程版 echo demo**：与现有 `main.cpp`（回调版）并列，直观对比两种写法。把 echo + 10s idle 超时写成一条直线（`co_await (read() | timeout(10s))`）。
- **8.5（可选）协程版 HTTP**：Task 7 的状态机用协程重写，体会有状态协议在协程下的简化。

**验收**：每个子步有最小可运行验证；协程 echo 与回调 echo 行为一致（含 idle 踢人），且 LOG_* 落盘正常。

---

## 其它（按痛点，未排期）

- [ ] **集成测试**：补一个"TcpServer 多线程压测 + Connection 析构顺序"的脚本/可执行（当前并发析构路径无专门覆盖）。
- [ ] **`LengthPrefixedCodec` 单元测试**：半包 / 粘包 / length=0 / 超 max_frame_len / 边界长度的专用用例（现在只被 smoke 覆盖）。
- [ ] **Acceptor 抽出**：listen channel/handle_accept 从 `TcpServer` 独立成 `Acceptor`（mirrors muduo），当前不是痛点。
- [ ] **log_server 多 IO 线程**：accept 用 mainLoop，conn 分给 subLoop；每个 subLoop 一个 LogFile（按线程分片）或统一 LogFile（加锁/MPSC）。当前 `num_threads=0`，写盘是阻塞 fwrite，流量上来会成瓶颈。
- [ ] **压力对比**：跟 muduo / asio echo 同硬件 throughput / p99 对比。
