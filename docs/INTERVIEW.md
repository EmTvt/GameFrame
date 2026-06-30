# 项目面试介绍指南（epoll_proj）

> 用途：面试时完整、有层次地讲清这个项目，**重点是难点的「问题 → 方案 → 效果 → 可追问」**，而不是功能罗列。
> 读法：先背第 0 节电梯陈述；第 4 节难点专题是重头戏；第 7 节是面试官追问清单，提前想好答案。

---

## 0. 30 秒电梯陈述（先抛结论）

> 「我从零手写了一个 Linux C++ 高性能网络库，参照 muduo 的 Reactor 模型，自底向上实现了 epoll 封装、Channel 事件分发、主从多线程 Reactor、基于 timerfd 的定时器、带背压的连接管理；并在它之上做了一个**生产形态的异步日志系统**——业务线程零阻塞写日志，经无锁化 MPSC 队列交给独立线程，通过 TCP 发往独立的日志收集进程落盘。整个项目的核心是**单线程事件循环 + 跨线程任务投递**这套并发模型，难点集中在**对象生命周期管理、跨线程安全、写缓冲与背压**这几块。」

一句话记忆：**「epoll Reactor 网络库 + 在其上搭的异步日志系统，主线是 one-loop-per-thread 的并发模型」**。

---

## 1. 项目定位

- **是什么**：从零手写的 Linux epoll C++ 网络库（muduo 风格），逐步演进出主从 Reactor、定时器、跨线程投递、背压；并用它搭了独立的日志收集服务 `log_server` + 业务侧日志发送 `LogSender` + `LOG_*` 宏作为真实业务验证。
- **技术栈**：C++20、epoll / timerfd / eventfd、pthread、CMake。
- **演进式开发**：每一步都对应一个能讲清动机的「为什么」，而不是一次到位——这一点本身就是亮点，面试时强调「我是怎么一步步逼出每个设计的」。

---

## 2. 整体架构

### 分层（依赖单向向下）

```
业务层    main.cpp(echo+idle)  /  LOG_* 宏  /  log_server 进程
            │                       │
应用组件   LogSender(复用Loop+TcpClient+MPSC)
            │
网络库     TcpServer / TcpClient ── Connection ── Channel ── EventLoop ── TimerQueue
src/                                                  │
通用件     util/: Buffer / MPSCQueue / LengthPrefixedCodec（不依赖 net，单向被依赖）
```

### 两条端到端数据流（讲项目时画出来）

**网络收发**：
```
client ──TCP──> epoll_wait 醒 → Channel::handle_event → Connection::handle_read
   → 拆帧(Codec) → message_cb → 业务处理 → conn->send → (写不完)output_buffer + EPOLLOUT 回写
```

**异步日志**：
```
业务线程 LOG_INFO → format_log(调用线程) → LogSender::push → MPSCQueue(零阻塞)
   → (边沿唤醒) sender_loop 50ms tick → drain 一批 → encode → TcpClient 连接的 Connection::send
   → TCP → log_server 拆帧 → LogFile 落盘(fwrite→每秒fflush，不fsync)
```

---

## 3. 模块详解（每个：是什么 / 为什么 / 关键实现）

### 3.1 EventLoop + 线程模型 + 跨线程投递 ★核心

- **是什么**：事件循环。`epoll_wait` 拿就绪事件 → 派给 Channel → 执行 pending functors → 处理定时器。
- **线程模型**：**one loop per thread**。EventLoop 构造时记录 `owner_tid_`（构造它的线程即它的 loop 线程），所有改状态的操作都 `assert_in_loop_thread()`。这条铁律让**绝大多数数据结构无需加锁**。
- **跨线程投递**：`run_in_loop(f)`：
  - 调用者就是 loop 线程 → 直接执行（零开销快路径）；
  - 否则 → 入队 `pending_functors_`（加锁）+ `wakeup()`。
- **wakeup 机制**：用一个 `eventfd` 注册成 Channel 挂在自己的 epoll 上。别的线程往它写 8 字节，`epoll_wait` 立刻醒来 → 跑 pending functors。**为什么需要**：epoll_wait 平时阻塞，没这个机制跨线程投递的任务要等到下次有 I/O 才被执行。
- **pending_functors 的 swap 技巧**：执行前把队列 `swap` 到局部变量再解锁执行——**缩短临界区** + 允许 functor 内部再投递（不会自锁）。

### 3.2 Channel — fd 在 epoll 视角的「代言人」

- **是什么**：封装「一个 fd + 它关注的事件 + 4 类回调（read/write/close/error）」。`epoll_event.data.ptr` 直接存 `Channel*`，事件来了 `handle_event(revents)` 自己分发到对应回调——**零查表**（不再维护 `map<fd, cb>`）。
- **关键**：`enable_reading()/enable_writing()` 改的就是「我关注什么事件」，再 `update_channel` 同步到 epoll。
- **难点见 D1（tie 生命周期保护）**。

### 3.3 TimerQueue — timerfd + std::set

- **是什么**：基于 `timerfd` + `std::set<Entry>`（按到期时间排序）实现 O(logN) 增删的定时器。
- **为什么 timerfd**：把「时间」也变成一个 fd 事件，统一进 epoll，和 I/O 一视同仁——不用给 epoll_wait 算超时参数。
- **关键**：set 的队首就是最近要触发的，`timerfd_settime` 设到队首时间；到点读 timerfd → 取出所有已过期的回调执行。`cancel` 用 TimerId，0 当哨兵（TimerQueue 从 1 派号）。

### 3.4 Buffer — 三段式

- `prependable | readable | writable` 三段。读写指针滑动，避免频繁挪数据；空间不够先内部腾挪再扩容。`readv` + 栈上额外缓冲，一次系统调用尽量读干净。

### 3.5 Connection — 单条连接的抽象 + 生命周期 ★核心

- **是什么**：持有 fd + Channel + input/output buffer + 4 类业务回调 + `set_context<T>` 业务上下文。
- `enable_shared_from_this`：把自己以 `shared_ptr` 交给异步回调/定时器/将来的协程 frame，**任何一个存活的 shared_ptr 都能延后析构**。
- **难点见 D3（生命周期）、D4（写缓冲+背压）、D5（EOF）**。

### 3.6 TcpServer — 主从 Reactor

- mainLoop 只负责 `accept`；accept 出的 Connection 用 `EventLoopThreadPool` round-robin 派给某个 subLoop，之后这条连接的全部 I/O 都在那个 subLoop 线程。
- **`connections_` map 只在 mainLoop 线程访问** → 无需加锁。
- **难点见 D3（close 跨线程 erase）**。

### 3.7 TcpClient — 主动连接 + 重连

- 非阻塞 `connect`：`EINPROGRESS` → 挂临时 Channel 等 `EPOLLOUT` → `getsockopt(SO_ERROR)` 判握手结果。
- 与 TcpServer **对称**：握手成功后构造 Connection，后续 I/O 完全复用同一套。
- 指数退避重连：500ms→30s 封顶，成功复位。
- **难点见 D6**。

### 3.8 MPSCQueue — 多生产者单消费者有界队列 ★核心

- mutex + ring buffer，构造时一次性分配，push 路径不再 new。
- **满则丢最新**（与 muduo/spdlog 一致）+ `dropped_` 原子计数。
- **边沿唤醒**：`push` 返回 `{ok, wake_up}`，只有「空→非空」那次 `wake_up=true`，避免高频 wakeup。
- **难点见 D7**。

### 3.9 LengthPrefixedCodec — 4B 大端 length + payload

- 纯静态、无状态（解码状态都在传入的 Buffer 上）。处理**半包/粘包**：头不够 4B 或 body 不够就返回 true 等下次；length=0 或超上限返回 false（协议错误，断连）。

### 3.10 log_server — 独立日志收集进程

- 单 EventLoop（accept + I/O + 落盘同线程，简单够用）。
- LogFile 按「日 + 大小」滚动；启动扫目录复用当天最大序号文件。
- **fflush vs fsync**：每秒 fflush 把用户态缓冲推进内核 page cache，**不 fsync**（不吃 IOPS）。难点见 D9。

### 3.11 LogSender — 复用 Loop 的日志发送器 ★核心

- **组装**：外部传入的 `EventLoop`（独立线程）+ 自己持有的 `TcpClient` + `MPSCQueue<string>`。
- `push` 任意线程零阻塞；50ms tick `drain` 一批 → encode → `connection_->send`。
- 边沿唤醒让首条日志微秒级落地，高吞吐时 50ms 批处理摊薄。
- 断连期间消息留队列等重连；满则丢最新 + 合成 `[LOG_DROPPED] N` 暴露。
- **难点见 D7、D8**。

### 3.12 LOG_* 宏 — 业务侧接口

- `format_log` 在**调用线程**格式化（避免跨线程传 va_list）；`do-while(0)`；编译期级别门控；C++20 `__VA_OPT__`；nullptr 安全。
- **全局访问点 set_global/global 而非自构造单例**——难点见 D10。

---

## 4. 难点专题（重头戏：问题 → 方案 → 效果 → 可追问）

> 面试时每个难点用「我遇到了什么问题 → 为什么会发生 → 怎么解决 → 效果」讲，最能体现深度。

### D1. 一条连接在处理自己的事件时把自己关了 → Channel::tie()

- **问题**：`epoll_wait` 返回一批就绪事件，正在执行某 Channel 的 `handle_event` 时，回调内部（或同批的其他事件）把这个 Connection 释放了。`handle_event` 后续再访问 Channel/Connection 成员 → **use-after-free**。
- **方案**：`Channel::tie(weak_ptr<void>)` 在 Connection start 时把自己绑上；`handle_event` 入口先 `tie_.lock()` 提升成 shared_ptr，**在本次事件处理期间锁住对象存活**，处理完再释放。
- **效果**：彻底消除「事件处理中途对象被析构」的 UAF。
- **可追问**：为什么用 `weak_ptr<void>` 而不是 `weak_ptr<Connection>`？→ 让 Acceptor/TcpClient 等也能复用 Channel，不绑死类型。

### D2. 跨线程把任务塞进别的 EventLoop → eventfd + pending_functors

- **问题**：subLoop 阻塞在 `epoll_wait`，主线程/别的线程想让它执行一段代码（如优雅关闭、派发连接），怎么「叫醒」它？
- **方案**：每个 EventLoop 有个 `eventfd` 注册成 Channel。`queue_in_loop` 入队后 `write(eventfd)` → epoll_wait 立刻醒 → `do_pending_functors`（先 swap 出队再执行）。
- **效果**：任意线程可安全地把闭包投递到目标 loop 线程执行，且执行时不持锁、可重入投递。
- **可追问**：`calling_pending_functors_` 标志干嘛的？→ 正在执行 functors 时若又有投递，要确保再 wakeup 一次，否则新任务可能漏到下一轮才跑。

### D3. Connection 的生命周期 / 从连接表里「自我删除」→ shared_ptr + close_cb 派回 mainLoop

- **问题**：连接关闭由 subLoop 触发（读到 0 / 出错），但持有它的 `connections_` map 在 **mainLoop**。subLoop 直接去 erase 跨线程访问 map → 数据竞争。而且 erase 会析构 Connection，可能正是当前栈上的对象。
- **方案**：
  1. Connection 一律 `shared_ptr` 管理；
  2. close 流程：subLoop 先做本地清理（Channel 注销、关 fd、置 kDisconnected、回调业务），再通过 `close_cb` **`queue_in_loop` 派回 mainLoop** 去 `connections_.erase`；
  3. 派任务时 functor **按值捕获 ConnectionPtr**，让对象活到 erase 真正执行完才析构（「安全析构」）。
- **效果**：跨线程无锁 + 不会自我析构 UAF。
- **可追问**：为什么外部资源（定时器、session 表）持 Connection 要用 `weak_ptr`？→ 避免「timer 持强引用 → 连接关了还得等 timer 到点才析构」的拖尾。

### D4. 数据一次写不完怎么办 → output_buffer + EPOLLOUT 回写 + 背压（异步回调防递归）

- **问题**：`write()` 在内核写缓冲满时只写了一部分（短写）。剩下的不能丢、也不能阻塞等。
- **方案**：
  1. `send` 先尝试直接写；没写完的塞 `output_buffer_`，并 `enable_writing()` 关注 EPOLLOUT；
  2. 可写时 `handle_write` 继续把 output_buffer 写出；写空了 `disable_writing()`（**电平触发下必须关，否则 EPOLLOUT 空转刷爆 CPU**）。
- **背压**：output_buffer 跨过 `high_water_mark` 触发 HighWaterMark 回调，排空触发 WriteComplete。**两个回调都用 `queue_in_loop` 异步触发**——因为业务在回调里很可能再 `send`/`close`，同步触发会**状态机自递归**。
- **效果**：慢消费端不会把本进程内存撑爆（LogSender 就靠这个 paused 暂停 drain）。
- **可追问**：为什么是边沿触发的 HighWaterMark（跨过阈值才一次），不是水平？→ 避免过载窗口里被回调淹没。

### D5. EOF 丢掉最后一帧

- **问题**：`handle_read` 读到 `n==0`（对端 FIN）就直接 close，但内核常把「最后一段数据 + FIN」在同一次 EPOLLIN 送达，缓冲里还有没派给业务的数据 → 丢帧。
- **方案**：EOF 分支**先 `message_cb(conn, input)` 派一次再 close**（muduo 标准做法）。
- **效果**：连接关闭前的最后数据不丢。这是个很能体现「踩过坑」的细节。

### D6. 非阻塞 connect + 重连退避 + 悬空 this

- **问题1**：非阻塞 connect 立即返回 `EINPROGRESS`，怎么知道连上没？→ 挂临时 Channel 等 EPOLLOUT，再 `getsockopt(SO_ERROR)` 取真实结果（成功失败都报可写）。
- **问题2**：重连定时器到点时 TcpClient 可能已析构 → 回调里 `this` 悬空。→ 约定 TcpClient 生命周期必须长于 loop，析构前先 `disconnect()` 关掉 retry 标志；定时器到点先查状态再拨号。
- **可追问**：握手成功后临时 Channel 怎么处理？→ 必须先拆掉临时 Channel 再让 Connection 用同一 fd 注册，否则**同一 fd 双重注册到 epoll**。

### D7. 业务日志绝不能拖慢主循环 → MPSC + 复用独立 Loop + 丢最新 + LOG_DROPPED ★

- **问题**：业务线程（可能就是网络 epoll 线程）打日志，如果同步写盘/发网络，会直接拖垮 QPS。
- **方案（多层）**：
  1. `LOG_*` 只做「调用线程格式化 + push 到 MPSCQueue」，**纯内存、零 I/O**；
  2. MPSCQueue **满则丢最新**（push 临界区最短，直接 return false），不阻塞生产者；
  3. 真正的 drain→encode→send 在 **LogSender 独立线程的 EventLoop** 上做（复用网络库的 Loop！）；
  4. **边沿唤醒**：只有「空→非空」那次戳醒 sender_loop，低吞吐低延迟、高吞吐不刷 wakeup；
  5. 丢了多少用 `dropped_` 原子累加，下次 drain 合成一条 `[LOG_DROPPED] N messages lost`，**避免静默丢失**。
- **效果**：业务侧打日志是纳秒级内存操作；日志系统自身的抖动/断连完全不反压业务。
- **可追问**：为什么丢最新不是丢最旧？→ 触发队列满的前提是下游已经跟不上，**前段日志通常是「问题刚发生时的现场」更值钱**，后到的多是同一故障的刷屏；且丢最新 push 直接 return false，临界区最短，未来切 lock-free 最容易。

### D8. LogSender 析构触发 ~TcpClient 跨线程 assert（真实踩过的坑）★

- **问题**：第一版 LogSender 在 main 线程析构，连带 `unique_ptr<TcpClient>` 一起析构。但 `~TcpClient` 第一行 `assert_in_loop_thread()`——它要求在 sender_loop 线程析构。main 线程析构直接 assert 致命。
- **方案**：`stop()` 跨线程派任务到 sender_loop，在 **sender_loop 线程内 `tcp_client_.reset()`**；等 main 线程 unique_ptr 析构时已是 nullptr。`stop()` 用 mutex+cv 同步等这套动作跑完。
- **效果**：析构顺序安全。这个坑非常能讲——**「谁拥有、在哪个线程析构」是 Reactor 模型最容易出错的地方**。
- **可追问**：stop() 为什么不能在 sender_loop 线程自己调？→ 会和「派任务+cv 等待」死锁；代码里专门判了 `in_loop_thread()` 走同步分支。

### D9. 落盘可靠性：fflush vs fsync 的取舍

- **问题**：写日志要不要每条 fsync 保证断电不丢？
- **方案**：`fwrite`（进 libc 用户态缓冲）→ 每秒 `fflush`（进内核 page cache）→ **不 fsync**。优雅退出（SIGINT/SIGTERM）时析构 `fflush+fclose` 兜底用户态缓冲。
- **取舍**：被 kill -9 最坏丢 1s 内未 flush 的；断电会丢 page cache 里未落盘的。**用「断电可能丢几秒」换吞吐和 IOPS**——和 Obsidian 等本地软件、数据库 WAL 之外的数据文件是同一类工程取舍。
- **可追问**：要做到断电不丢怎么办？→ 学数据库 WAL，攒批 + 定期 fsync（每 N 条 / 每 M 毫秒），而不是每条 fsync。

### D10. 日志宏：全局访问点 vs 自构造单例

- **问题**：`LOG_*` 要全局零参数可调，但 LogSender 构造需要 loop/host/port，且关闭时机敏感。
- **方案**：**显式构造 + 全局访问点**（`set_global/global` 存裸指针），不用 Meyers 单例。
- **为什么不用单例**（三条）：① **可测试性**——`test_logging` 要把日志重定向到进程内 mini sink，单例写死 host 就做不到；② **关闭时机**——`stop()` 必须在 loop 线程、loop 还活着时跑完，交给静态析构时机不可控（重蹈 D8）；③ 配置灵活性（host/port/队列容量按场景调）。
- **诚实点**：单例**技术上可行**（magic statics 能在 instance() 里建线程），只是牺牲上面三点；主流库（spdlog registry、muduo 全局默认输出函数）也都走「实例可注入 + 全局访问点」。

---

## 5. 关键设计权衡一览（速记表）

| 抉择 | 选了什么 | 为什么 |
|---|---|---|
| 并发模型 | one-loop-per-thread + 跨线程投递 | 大部分结构无锁，临界区极小 |
| 事件触发 | 电平触发(LT) | 写空必须 disable EPOLLOUT，逻辑直白；ET 易漏读 |
| 对象管理 | Connection 用 shared_ptr，外部资源用 weak_ptr | 安全析构 + 防拖尾 |
| 背压回调 | queue_in_loop 异步触发 | 防业务在回调里 send/close 自递归 |
| MPSC 满策略 | 丢最新 + dropped 计数 | 临界区最短，保前段现场，不静默丢 |
| 日志落盘 | fflush 周期，不 fsync | 换吞吐/IOPS，断电丢几秒可接受 |
| 日志入口 | 全局访问点，非单例 | 可测试 + 关闭可控 + 可配置 |
| 定时器 | timerfd + set | 时间也变 fd 事件，统一进 epoll |

---

## 6. 自测数据 / 亮点（讲的时候有数据更可信）

- `test_log_sender`：4 生产者 × 10000 条并发 push，push 侧总耗时约 12ms（纯内存）；cap=5000 队列下落盘行数 = ok push + 1 条合成 LOG_DROPPED，**对账精确**。
- `test_logging`：自包含端到端，四级别各 400 条计数精确匹配，覆盖 nullptr 安全。
- 测试体系：event_loop / tcp_client / mpsc_queue（含 TSan 版）/ log_sender / logging，关键并发路径有覆盖。
- 工程亮点：演进式开发 + 每步动机文档化（docs/ 里 DECISIONS 追加日志、DEBUGGING 踩坑记录）。

---

## 7. 面试官可能的深挖问题（提前备好）

1. **ET 还是 LT？区别？** → 本项目 LT；ET 必须一次读/写干净否则丢事件，LT 更稳、配合 output_buffer 逻辑直白。
2. **epoll 为什么比 select/poll 快？** → 红黑树管理 fd + 就绪链表，O(1) 拿就绪集；无需每次传全量 fd、内核遍历。
3. **惊群？** → 单 listen fd 单 mainLoop accept，不涉及多线程抢同一 fd；多进程场景才需 EPOLLEXCLUSIVE / SO_REUSEPORT。
4. **为什么一个 loop 一个线程？多个线程共享一个 epoll 行不行？** → 共享要给所有结构加锁，且唤醒/派发复杂；one-loop-per-thread 让单 loop 内全程无锁。
5. **shared_ptr 循环引用怎么避免？** → 外部资源持 weak_ptr；Channel::tie 用 weak。
6. **send 在非 loop 线程调用安全吗？** → 不安全，要 run_in_loop 派进去；Connection 的 I/O 必须在它归属的 loop 线程。
7. **MPSC 为什么不用 lock-free？** → 先 mutex+ring 跑通，临界区只 move 一次；profile 后是瓶颈再换 Vyukov MPMC。已留好切换空间（丢最新策略对 lock-free 最友好）。
8. **惊醒 epoll 的 eventfd 和 timerfd 有什么共性？** → 都是「把非 I/O 事件 fd 化」，统一纳入 epoll，避免 epoll_wait 之外的等待通道。
9. **日志丢了怎么排查？** → 看落盘里的 `[LOG_DROPPED] N`；N>0 说明下游跟不上或断连过。
10. **怎么优雅退出不丢日志？** → 信号转 loop->quit → set_global(nullptr) → sender.stop()（最后 drain）→ log_server 析构 fflush+fclose。

---

## 8. 难点一句话背诵版

- D1 tie：事件处理期间用 weak_ptr 提升锁住对象，防 handle_event 中途 UAF。
- D2 wakeup：eventfd 注册成 Channel，跨线程投递后写它叫醒 epoll_wait。
- D3 生命周期：Connection 全程 shared_ptr，close 派回 mainLoop erase，functor 按值捕获延寿。
- D4 写缓冲：短写进 output_buffer + 关注 EPOLLOUT 回写，写空 disable；背压回调异步触发防递归。
- D5 EOF：读到 0 先派一次 message_cb 再 close，防丢最后一帧。
- D6 connect：EINPROGRESS 挂临时 Channel 等 EPOLLOUT + getsockopt 判结果；退避重连防悬空 this。
- D7 日志：LOG_* 纯内存 push 到丢最新 MPSC，独立 Loop 线程 drain 发送，边沿唤醒，dropped 合成暴露。
- D8 析构：TcpClient 必须在 loop 线程 reset，stop() 跨线程同步等。
- D9 落盘：fflush 进内核不 fsync，换吞吐；优雅退出兜底。
- D10 日志入口：全局访问点非单例，为可测试 + 关闭可控。
