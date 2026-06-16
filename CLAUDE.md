# CLAUDE.md

> 给未来的 AI pair-programmer（包括下一轮上下文里的我自己）看的项目地图。
> 目标是：进入这个仓库后**30 秒**知道在做什么、代码长什么样、下一步要做什么、哪些坑已经踩过。

---

## 1. 这是个什么项目

一个**从零手写的 Linux epoll C++ 网络库**，参照 muduo 的设计逐步演进；目前已经把"主从 Reactor + 定时器 + 跨线程任务投递 + 背压"都跑通了，正在用它搭建一个**独立的日志收集服务器 `log_server`** 作为真实业务验证。

不是工业级框架，是教学/学习驱动：每加一个组件都对应一个能讲清楚动机的"步"，**演进顺序优先于一次到位**。

### 顶层产物（三个可执行）

| target | 文件 | 用途 |
|---|---|---|
| `epoll_proj`       | `main.cpp`              | echo demo + idle timeout（10s 不发数据踢人）。验证 TcpServer + TimerQueue + Connection::set_context 整条链路。 |
| `test_event_loop`  | `test_event_loop.cpp`   | 最小测试：跨线程 `run_in_loop` 投递任务能落到 loop 线程执行。 |
| `log_server`       | `log_server/main.cpp`   | 独立进程，监听 TCP，按 `len|payload` 协议收日志，按"日 + 大小"滚动落盘。 |

构建：

```bash
cmake -B build
cmake --build build -j
```

C++20、`-Wall -Wextra`、默认 Debug、生成 `compile_commands.json` 给 clangd。

---

## 2. 目录结构与模块职责

```
epoll_proj/
├── CMakeLists.txt
├── main.cpp                  # epoll_proj demo 入口（echo + idle）
├── test_event_loop.cpp       # EventLoop 跨线程能力测试
├── src/                      # 网络库本体
│   ├── buffer.h              # readable / writable / prependable 三段式 Buffer
│   ├── channel.{h,cpp}       # fd 在 epoll 视角的"代言人"，含 tie() 生命周期保护
│   ├── connection.{h,cpp}    # 单条 TCP 连接：input/output buffer + 4 类回调 + 背压 + context<T>
│   ├── event_loop.{h,cpp}    # epoll_wait + Channel 派发 + wakeup_fd + pending_functors + TimerQueue
│   ├── event_loop_thread.{h,cpp}        # 单线程跑一个 EventLoop
│   ├── event_loop_thread_pool.{h,cpp}   # N 个 sub-loop，round-robin 分配
│   ├── server.{h,cpp}        # TcpServer：listen + accept + 把 conn 分给 subLoop
│   └── timer_queue.{h,cpp}   # 基于 timerfd 的 set<Entry>，O(logN) 增删
└── log_server/               # 业务层验证：日志收集服务
    ├── main.cpp              # 协议拆帧 + 信号处理 + 周期 flush
    ├── log_file.{h,cpp}      # 按日 + 序号滚动的文件落盘
    ├── smoke_client.py       # 简易客户端：发一条/多条 length-prefixed 消息
    └── roll_stress.py        # 压测脚本：往里灌 >10MiB 验证滚动
```

CMake 故意让 `log_server` **复用 `src/` 的源文件**而不是抽 static lib —— 当前规模下抽 lib 是过度设计，等真有第二个业务方再说。

---

## 3. 已经做到哪一步

按演进时间线（注释里到处能看见"第 N 步"，跟下面对得上）：

| # | 主题 | 状态 | 关键产出 |
|--:|---|---|---|
| 1 | 单 fd epoll wrapper | ✅ | （早期合并进了 EventLoop，已不存在独立类） |
| 2 | Buffer 三段式 | ✅ | `buffer.h` |
| 3 | TcpServer + Connection 双方向 I/O | ✅ | `server.*` / `connection.*` |
| 4 | output_buffer + send 半阻塞 / EPOLLOUT 回写 | ✅ | `Connection::send / handle_write` |
| 5 | EventLoop 从 TcpServer 抽离 | ✅ | `event_loop.*` |
| 6 | 跨线程：owner tid + wakeup_fd + pending_functors + `run_in_loop` | ✅ | `EventLoop::run_in_loop / queue_in_loop` |
| 7 | Channel 抽象 + epoll data.ptr | ✅ | `channel.*`；epoll 派发零查表 |
| 7.5 | Channel::tie() 生命周期保护 | ✅ | `Channel::tie / handle_event_with_guard` |
| 8 | 主从 Reactor：`EventLoopThread(Pool)` + 多 IO 线程 | ✅ | `event_loop_thread*.{h,cpp}` |
| 9 | TimerQueue（timerfd + std::set + cancel） | ✅ | `timer_queue.*`、`EventLoop::run_after / cancel_timer` |
| 10 | Connection::set_context<T>（业务挂载） | ✅ | `connection.h` 模板部分 |
| 11 | 背压：HighWaterMark + WriteComplete | ✅ | `Connection::set_high_water_mark_callback / set_write_complete_callback` |
| 12 | **log_server 第一阶段**：单连接 length-prefix 落盘 | ✅ | `log_server/*` |
| 12.1 | LogFile 按"日 + 序号"滚动（10 MiB/file） | ✅ | `log_file.*` |

### log_server 现状细节

- 协议：`uint32_t len(big-endian) | payload`，payload 直接写文件（追加 `\n` 方便 `tail -f`）
- 单 EventLoop（`num_threads=0`），网络 + 落盘同线程，**写盘是阻塞 fwrite**
- `flush()` 由 mainLoop 上的定时器每秒触发一次（自递归挂下一发）
- 文件名：`basedir/basename.YYYYMMDD.log.N`，N 从 1 起
- 滚动触发：跨日 → 重置 N=1；当前文件 size ≥ roll_size → N+1；启动时扫目录复用最大序号文件（未满则续写）
- 信号：SIGINT/SIGTERM → `loop->quit()` → 优雅退出 → LogFile 析构 fflush+fclose（0 丢失）；SIGPIPE → 忽略

### 在主干上的临时改动（**重要**：稳定后要清理）

`src/connection.cpp::Connection::handle_read` 里目前有一坨 `[DIAG conn ...]` stderr 诊断（入口、每次 read 返回、EOF、drain），用来定位"smoke client 发完数据立刻 shutdown(SHUT_WR) 但 server 没派 message_cb"那个 bug。

**真 bug 修在哪了**：EOF 分支（`n == 0`）原本直接 `close()`，导致同一轮 EPOLLIN 里 read 拿到的数据还没派给 message_cb 就被丢掉。现在 EOF 路径也会先派一次 `message_cb` 再 close。这是 muduo `handleRead` 的标准做法，**保留**。

待办：等下一轮稳定测试通过，把那些 `[DIAG ...]` printf 删掉。

---

## 4. 下一步要做什么

> 顺序按"投入产出比 + 当前痛点驱动"排列；不一定全做，但写在这里防止下次又凭感觉走。

### 短期（log_server 闭环）
- [ ] **清理诊断**：删 `connection.cpp::handle_read` 里的 `[DIAG ...]` 和 `log_file.cpp` 里 `LogFile::append` 每次的 stderr（保留 rolled-to 那一行）
- [ ] **客户端侧 `AsyncLogger`**：在 `epoll_proj` 进程里加一个"业务线程写、IO 线程发"的双缓冲异步日志（muduo `AsyncLogging` 风格），通过 TcpClient（见下）把日志发给 log_server
- [ ] **抽 `LengthPrefixedCodec`**：现在 server 的拆帧逻辑直接写在 `log_server/main.cpp` 的 lambda 里，client 还没写。第二步抽出来共享
- [ ] **`TcpClient`**：与 `TcpServer` 对称的主动连接侧，带断线自动重连（指数退避）。AsyncLogger 用它

### 中期（网络库补完）
- [ ] **`shutdown(SHUT_WR)` 半关支持**：现在 `Connection::close()` 是直接关，没区分"我还有 outbuf 没发完"。muduo 的 `shutdown()` 在 outbuf 排空后才真关
- [ ] **`Connection::force_close_with_delay(ms)`**：HTTP 之类的协议 close 前要给客户端最后写一点数据再关，需要延迟关闭
- [ ] **集成测试**：现在只有 `test_event_loop`。至少补一个"TcpServer 多线程压测 + Connection 析构顺序"的脚本/可执行
- [ ] **Acceptor 抽出**：现在 listen socket 的 channel/handle_accept 直接挂在 `TcpServer` 里，独立成 `Acceptor` 类会更对称（mirrors muduo），但当前不是痛点

### 长期（如果还往下做）
- [ ] log_server 加**多 IO 线程**：accept 用 mainLoop，conn 分给 subLoop；每个 subLoop 一个 LogFile（按线程分片）或统一一个 LogFile（加锁/MPSC 队列）
- [ ] **HttpServer demo**：跑通 HTTP/1.1 解析作为第二个真实业务，验证 `Connection::set_context<T>` 挂解析器状态机的设计
- [ ] **压力对比**：跟 muduo / asio echo 在同硬件做 throughput / p99 对比，找出瓶颈

---

## 5. 关键设计约定（动手前必读）

读代码 / 改代码前请认这些，否则会跟现有风格打架：

1. **线程归属**
   - 每个 `EventLoop` 绑定到构造它的线程（`owner_tid_`）
   - 所有写 `EventLoop` / `Channel` / `Connection` 的操作**必须在它归属的 loop 线程**做
   - 跨线程调用走 `run_in_loop(f)`：当前线程是 loop 线程就直接调，否则 `queue_in_loop` + `wakeup`
   - 不确定就调 `assert_in_loop_thread()` 自检

2. **生命周期**
   - `Connection` 都用 `ConnectionPtr = std::shared_ptr<Connection>` 传，禁止裸 `Connection&` 跨函数边界
   - 异步回调要持有 conn 的，**捕获 ConnectionPtr 拷贝**（让 lambda 延寿）
   - 定时器等"外部资源"持 conn 一律用 `weak_ptr<Connection>`，避免拖尾析构
   - Channel 的事件派发期生命周期靠 `tie()` 锁定（防 close 中途自我析构）

3. **回调风格**
   - 业务对外：`ConnectionCallback(conn)` 一个回调覆盖建立/断开两种语义，用 `conn->connected()` 区分（muduo 风格）
   - 业务对外：`MessageCallback(conn, Buffer& input)`；拆帧由业务做，Buffer 给的是 readable 视图
   - 不在同步回调里再调可能触发回调的方法（避免状态机自递归）——`HighWaterMark` / `WriteComplete` 故意走 `queue_in_loop` 异步触发

4. **风格**
   - 头文件**开头有一大段中文注释**讲"这个类为什么存在 / 演进到这一步为了解决什么"。新写的类请保持这个习惯
   - 注释要解释**动机**（"为什么这么做"），而不是描述代码（"这里调用 xxx"）
   - 临时诊断用 `[DIAG ...]` 前缀，方便事后 `grep -n` 一把清掉

5. **不要做的事**
   - 别在 `src/` 里 include `log_server/` ——单向依赖
   - 别给 `TcpServer` 加业务字段（解析器、session 表等），那些挂在 `Connection::context_` 上
   - 别把 `EventLoop::loop()` 改成可重入 —— 当前用 `quit_` 单标志，递归会爆

---

## 6. 一些反复踩过的坑（避免重复挖）

- **telnet 不适合测**：会注入 IAC 协商字节，把 length-prefixed 协议解析出天文数字 → "bad frame length=..."。压测一律用 `smoke_client.py` / `roll_stress.py`
- **client `sendall` 紧跟 `shutdown(SHUT_WR)`**：内核可能把数据 + FIN 在同一次 EPOLLIN 里送达，server 的 `handle_read` 必须在 EOF 分支也派 message_cb，否则丢最后一帧。**已修**
- **roll_size 默认值**：log_server 当前默认 10 MiB；命令行第 4 个参数可以传 MiB 整数覆盖（压测时调小到 1 MiB 验证翻号）
- **`LogFile::open_file` 用 `file_size()` 校准 `written_bytes_`**：续写场景必须，否则进程重启会立刻翻号 / 永不翻号
- **`epoll_wait` 不要加 `SA_RESTART`**：信号要能打断它，立刻处理 `quit_`
- **`Channel::tie` 的 weak_ptr<void>**：故意不绑 Connection 类型，否则 Acceptor 没法用同一份 Channel 实现

---

## 7. 常用调试 / 验证命令

```bash
# 构建
cmake -B build && cmake --build build -j

# 跑 echo demo（10s idle 踢人）
./build/epoll_proj
# 另一终端 telnet 127.0.0.1 8888

# 跑 log_server，10 MiB 翻号
./build/log_server 9099 ./res/log
# 压测：往里灌 12 MiB，应该看到 .log.1 写满 → .log.2 出现
python3 log_server/roll_stress.py --port 9099 --total 12M

# 验证翻号更细的节奏（1 MiB 一翻）
./build/log_server 9099 ./res/log 1
python3 log_server/roll_stress.py --port 9099 --total 5M
ls -la res/log/      # 期望 .log.1..5

# EventLoop 跨线程能力自测
./build/test_event_loop
```

---

## 8. 维护这份文档的原则

- **每完成一个"步"**：在第 3 节表里勾掉、把简短产出写一行；第 4 节挪一下顺序
- **每发现一个新坑**：写进第 6 节，附"已修/未修"
- **每改一个核心约定**：第 5 节同步更新
- 不要把它写成代码文档（那是头文件注释的活）；这里只放**地图、决策、待办**
