# Design Decisions

> 与 AI 反复讨论后确定下来的设计结论。**只追加，不重写**（推翻一个决策时，新增一条说明 supersedes）。

---

## 2026-06-15: log_server 不抽 static lib，直接复用 src/ 源文件

**Reason**
当前规模下只有 `epoll_proj` 和 `log_server` 两个可执行，再加几个 test，抽成 lib 是过度设计；CMake 里直接把 `src/*.cpp` 列到 log_server 的 `add_executable` 里就行。

**Impact**
- 加新 src 文件需要同步加进 4 个 `add_executable`（epoll_proj / test_event_loop / test_tcp_client / log_server）。
- 等真有第三个业务方再统一抽 `epoll_net` 静态库。

---

## 2026-06-15: Channel 不持有 fd 生命周期

**Reason**
Channel 只代表"对这个 fd 在 epoll 上感兴趣的事件 + 回调"。fd 的 close 由真正的资源持有者负责（Connection 持有连接 fd，TcpServer 持有 listen fd，TcpClient 在 connecting 阶段持有中间 fd）。混在一起会导致关闭顺序混乱、weak_ptr<Channel> 没法准确语义。

**Impact**
- 关闭连接时**先**调 `Channel::disable_all` + 从 EventLoop 注销，**再**关 fd、释放 Connection。
- `Channel::tie()` 用 `weak_ptr<void>` 而不是 `weak_ptr<Connection>`，让 Acceptor / TcpClient 也能复用同一份 Channel 实现。

---

## 2026-06-15: Connection 一律用 `shared_ptr` 管理

**Reason**
- 定时器、跨线程异步回调、`set_context<T>` 业务对象都可能"延后访问 Connection"，裸引用太脆弱。
- 跟 muduo 风格保持一致。

**Impact**
- 异步回调持有 Connection 一律 `ConnectionPtr` 拷贝（lambda 捕获）让其延寿。
- "外部资源"（如定时器、业务 session 表）持 Connection 一律 `weak_ptr<Connection>`，避免拖尾析构。
- 公开类型别名 `using ConnectionPtr = std::shared_ptr<Connection>;`。

---

## 2026-06-15: 背压回调走 `queue_in_loop` 异步触发

**Reason**
`HighWaterMarkCallback` / `WriteCompleteCallback` 如果在 `Connection::send` 同步路径里直接调，业务在回调里再调 `send` 会触发自递归，状态机会爆。

**Impact**
- 这两个回调用 `loop_->queue_in_loop(...)` 派出去。
- 业务可以放心在回调里再调 send，不会立即重入。

---

## 2026-06-15: EventLoop 不可重入

**Reason**
`loop()` 用 `quit_` 单标志退出，递归进入会让退出语义不可控。

**Impact**
- 不要从 `EventLoop::loop()` 内部再调 `loop()`。
- 一个线程一个 EventLoop。

---

## 2026-06-15: epoll_wait 不加 SA_RESTART

**Reason**
信号要能立刻打断 `epoll_wait`，让 mainLoop 检测到 `quit_` 并优雅退出（SIGINT/SIGTERM 路径必须）。

**Impact**
- 信号处理器只设标志位 / 调 `loop->quit()`，不做实际工作。
- 不用 `sigaction(..., SA_RESTART)`。

---

## 2026-06-15: EOF 路径也派一次 message_cb

**Reason**
内核会把数据 + FIN 在同一次 EPOLLIN 里送达；`Connection::handle_read` 拿到 `n == 0` 时缓冲区里通常还有未派给业务的数据。直接 close 会丢最后一帧。

**Impact**
- `handle_read` 的 EOF 分支要先 `message_cb_(conn, input_buffer_)` 再 `handle_close()`。
- 这是 muduo `handleRead` 的标准做法，**保留**。

---

## 2026-06-15: 头文件以"动机"为主写中文注释

**Reason**
教学/学习驱动的项目，**为什么这样做**比"代码做了什么"重要。
描述代码本身的注释只会和代码一起腐烂。

**Impact**
- 新写的类，头文件开头放一段中文注释：这个类为什么存在 / 演进到这一步是为了解决什么问题。
- 函数/字段的注释解释动机，而不是描述实现。
- 临时调试输出统一用 `[DIAG ...]` 前缀，便于事后 grep 清理。

---

## 2026-06-16: TcpClient 与 TcpServer 对称设计，复用 Connection

**Reason**
TcpServer accept 后会 new 一个 Connection；TcpClient connect 成功后也应该走同一条路径，否则 I/O 路径会被复制两份。

**Impact**
- TcpClient 在 connecting 阶段自管 fd + 临时 `connect_channel_`；连接成功后 `new_connection(fd)` 把 fd 转交给 Connection，临时 channel 销毁。
- 重连时机：Connection close 回调里检查 `retry_` 标志，启动 `run_after(retry_delay_ms_, retry)`，退避翻倍上限 30s。
- 业务一律通过 `set_connection_callback` / `set_message_callback` 接入，与 TcpServer 一致。

---

## 2026-06-16: log_server 不嵌入业务进程，独立部署

**Reason**
- 业务进程的可靠性不应被日志写盘拖累
- 日志服务挂了，业务可以靠 LogSender 的指数退避重连恢复
- 留出未来"集中式日志收集"的演进路径

**Impact**
- 业务进程通过 `MPSCQueue + LogSender + TcpClient` 把日志发到独立的 log_server 进程
- 业务侧 `LOG_INFO` 必须零阻塞（纯内存 push），不允许任何 I/O
- log_server 短暂断开期间日志在 MPSCQueue 中堆积，溢出时丢**最旧**日志（保新弃旧）
