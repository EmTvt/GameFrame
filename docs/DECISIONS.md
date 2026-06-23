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

---

## 2026-06-18: MPSCQueue 满策略改为"丢最新"（supersedes 2026-06-16 末尾"保新弃旧"）

**Reason**
- 与 muduo / spdlog / glog 等主流实现保持一致，降低维护者认知成本。
- "丢最旧"实现要在 push 路径上推进 front_ 指针并析构旧元素，多生产者并发下要么扩大临界区、要么搞复杂的 head/tail CAS；"丢最新"在满时直接 `return false`，临界区最短，未来切 lock-free 也最容易（SPSC 经典 ring buffer 直接可用，多生产者只要在 tail 的 CAS 上排队）。
- 故障现场的"最近日志最有价值"在本场景不成立：触发队列满的前提是 LogServer 已断开 / Sender 跟不上，此时**前段**日志通常包含"问题刚发生时的现场"，反而是更值钱的；后到的多半是同一故障的洪泛刷屏，丢掉影响小。
- 配合 LogSender：满时业务侧静默丢弃，但要在 Sender 里维护一个 `dropped_count_`，下次有空间时 push 一条 "[LOG_DROPPED] N messages lost" 进队列，让运维能在日志里看到丢了多少（avoids silent loss）。

**Impact**
- `MPSCQueue::push(T)` 满时返回 `false`，**不**覆盖任何已有元素。
- `MPSCQueue` 暴露 `dropped()` 计数（`atomic<uint64_t>`），由 push 失败路径自增。
- LogSender 在每次 drain 拿到非空批次前，检查 `dropped_count` 是否大于 0，是则在批次开头插一条"丢了 N 条"的合成日志并清零计数。
- `docs/NEXT_TASKS.md` 中 Task 2 的描述同步改为"丢最新"。

---

## 2026-06-18: 拆出 `util/` 目录，存放与 net 无强耦合的通用件

**Reason**
- `Buffer` 只是字节缓冲区，`MPSCQueue<T>` 是纯模板容器：两者都不 `#include` 任何 net 头文件，放在 `src/`（网络库本体）里会让"src 内部是否能被业务直接依赖"这条边界变模糊。
- 后续 LogSender / 业务进程要直接用 `MPSCQueue`，但不应该（也不需要）拽进整个 `src/` 网络库做编译依赖。给它一个独立目录，依赖方向单向清晰。
- 目录是文档：看到 `util/` 就知道"这里的东西可以被任何上层放心 include，不会反过来拖进 epoll/Channel/EventLoop"。

**Impact**
- `src/buffer.h` → `util/buffer.h`；`src/mpsc_queue.h` → `util/mpsc_queue.h`。
- 全工程 include 统一为 `#include "util/buffer.h"` / `#include "util/mpsc_queue.h"`（含 `src/connection.h`、`src/length_prefixed_codec.h`、`main.cpp`、`log_server/main.cpp`、`test_mpsc_queue.cpp`）。
- CMake 不需要新增 source（util/ 全是头文件），只要 `target_include_directories` 已经包含 workspace root 就能找到。所有现有 target 都满足。
- 依赖方向硬约束：**`util/` 不准 `#include "src/*"`**。如果将来某个 util 件需要依赖 net，那它就不该在 util/。
- `LengthPrefixedCodec` 暂留在 `src/`：它依赖 Buffer 但更偏"协议件"而非"容器件"，跟具体应用层语义有关。如果后续发现也想被业务方直接复用，再考虑迁。

---

## 2026-06-18 (晚): `LengthPrefixedCodec` 也迁入 `util/`（supersedes 同日上一条末尾"暂留 src/"）

**Reason**
- 重新看了一下 codec 的依赖：除了 `util/buffer.h` 它什么都没引（不依赖 Channel/EventLoop/Connection 任何 net 头）。所谓"协议件"只是语义分类，从**依赖方向**上看它和 Buffer / MPSCQueue 完全是一类。
- 即将要写的 LogSender 会同时用 `MPSCQueue<string>` 和 `LengthPrefixedCodec::encode`。如果 codec 留在 `src/`，LogSender 不论放在哪个目录都得同时 include `src/` 和 `util/`，"util 是底层"这条边界就被破坏一次。
- "暂留"是基于"它跟应用层语义有关"这种感觉性判断，没有具体的依赖事实支撑。该判断不成立。

**Impact**
- `src/length_prefixed_codec.h` → `util/length_prefixed_codec.h`（`git mv`）。
- `log_server/main.cpp` 的 include 从 `"src/length_prefixed_codec.h"` 改为 `"util/length_prefixed_codec.h"`。这是全工程唯一一处真实 include 点。
- `util/` 现在共 3 个头文件：Buffer / MPSCQueue / LengthPrefixedCodec。依赖图 `util/length_prefixed_codec.h → util/buffer.h`，仍在 util 内部，不破坏"util/ 不准 #include src/*"的约束。
- 文档：`PROJECT_CONTEXT.md` 目录树、`ARCHITECTURE.md` util 层、`CURRENT_STATE.md` util 节都同步加 codec。

---

## 2026-06-23: LOG_* 宏用"全局访问点"而非"自构造单例"（supersedes NEXT_TASKS Task4 早期 `instance()` 草案）

**Reason**
日志是横切关注点，`LOG_INFO` 要能在任意文件/线程零参数调用，所以需要一个全局可达的入口。但 Meyers 自构造单例（`LogSender::instance()`）在本工程有三个硬冲突：
- **构造依赖拿不到**：`LogSender` 构造需要 `EventLoop*/host/port`，且 `sender_loop_` 必须是已经在专属线程跑起来的 loop；自构造单例只能 hardcode 或再加 init，单例的"零配置"优势消失。
- **与既有风格打架**：TcpClient / LogSender 都刻意让"loop 由上层持有"。塞一个自管 loop 的单例进去是架构倒退。
- **静态析构顺序坑**：`stop()` 必须在 `sender_loop_` 线程、loop 还活着时跑完（`stop_in_loop` 里 `tcp_client_.reset()` 依赖 in_loop_thread）。交给静态析构则时机不可控，会重蹈已修复的 `~TcpClient` 跨线程 assert。

**Impact**
- `LogSender` 加一对静态入口 `static void set_global(LogSender*)` / `static LogSender* global()`，后端是 `static inline LogSender* g_instance_ = nullptr`。**只存裸指针，不接管所有权**。
- 实例仍由调用方（一般 main）构造 + 拥有。约定用法：`start()` → `set_global(&s)`（业务线程起来前）→ … → `set_global(nullptr)`（业务线程 join 后、stop 前）→ `s.stop()`。
- 宏侧 `global()` 拿到 nullptr 时静默 no-op（日志系统未就绪 / 已注销），不崩。
- 线程约定：`set_global` 只在"业务线程尚未启动"或"已全部 join"时调，与并发的 `global()` 读不重叠，因此用普通指针即可，不引 atomic。
- 宏用 do-while(0) + 编译期级别门控（`if (level_num < LOG_COMPILE_LEVEL) break;`，关掉的级别为死代码消除）+ C++20 `__VA_OPT__(,)`（非 GNU `##__VA_ARGS__`，符合 `CMAKE_CXX_EXTENSIONS OFF`）。
- 时间戳归属：`format_log` **不打时间戳**，由 log_server 落盘时 `append_line` 统一加，避免两段时间。
