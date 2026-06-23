# Project Context

> 项目本身是什么、为什么存在、产物有哪些。**稳定信息**，很少变动。

## 1. 项目定位

一个**从零手写的 Linux epoll C++ 网络库**，参照 muduo 的设计逐步演进；目前已经把"主从 Reactor + 定时器 + 跨线程任务投递 + 背压"都跑通了，正在用它搭建一个**独立的日志收集服务器 `log_server`** 作为真实业务验证。

不是工业级框架，是教学/学习驱动：每加一个组件都对应一个能讲清楚动机的"步"，**演进顺序优先于一次到位**。

## 2. 顶层产物（可执行）

| target | 入口文件 | 用途 |
|---|---|---|
| `epoll_proj`        | `main.cpp`              | echo demo + idle timeout（10s 不发数据踢人）+ **业务侧 LOG_* 接入**（连接/收包/踢人落盘到 log_server）。验证 TcpServer + TimerQueue + Connection::set_context + LogSender 整条链路。 |
| `test_event_loop`   | `test_event_loop.cpp`   | 最小测试：跨线程 `run_in_loop` 投递任务能落到 loop 线程执行。 |
| `test_tcp_client`   | `test_tcp_client.cpp`   | TcpClient 验证：连接 echo server → 主动 close → 自动重连 → 再次 echo。 |
| `test_mpsc_queue`   | `test_mpsc_queue.cpp`   | MPSCQueue 单/多线程语义：FIFO / 丢最新 / dropped 计数 / 边沿唤醒。 |
| `test_log_sender`   | `test_log_sender.cpp`   | LogSender 多生产者并发 push + 真发到外部 log_server（需先起 log_server）。 |
| `test_logging`      | `test_logging.cpp`      | LOG_* 宏端到端：进程内自建 mini sink，无需外部进程，计数精确比对 + nullptr 安全。 |
| `log_server`        | `log_server/main.cpp`   | 独立进程，监听 TCP，按 `len|payload` 协议收日志，按"日 + 大小"滚动落盘。 |

## 3. 目录结构与模块职责

```
epoll_proj/
├── CMakeLists.txt
├── CLAUDE.md                  # 新会话入口（短）
├── docs/                      # 项目文档（本目录）
├── .claude/                   # 规则与 skill
├── main.cpp                   # epoll_proj demo 入口（echo + idle + LOG_* 接入）
├── test_event_loop.cpp        # EventLoop 跨线程能力测试
├── test_tcp_client.cpp        # TcpClient 连接/重连测试
├── test_mpsc_queue.cpp        # MPSCQueue 单/多线程语义测试
├── test_log_sender.cpp        # LogSender 并发 push + 真发到外部 log_server
├── test_logging.cpp           # LOG_* 宏端到端（进程内 mini sink，自包含）
├── util/                      # 与网络层无强耦合的通用件（纯头文件）
│   ├── buffer.h               # readable / writable / prependable 三段式 Buffer
│   ├── length_prefixed_codec.h# 4B 大端 length + payload 帧编解码（仅依赖 Buffer）
│   └── mpsc_queue.h           # 多生产者单消费者有界队列（mutex+ring，丢最新）
├── src/                       # 网络库本体（依赖 util/）
│   ├── channel.{h,cpp}        # fd 在 epoll 视角的"代言人"，含 tie() 生命周期保护
│   ├── connection.{h,cpp}     # 单条 TCP 连接：input/output buffer + 4 类回调 + 背压 + context<T>
│   ├── event_loop.{h,cpp}     # epoll_wait + Channel 派发 + wakeup_fd + pending_functors + TimerQueue
│   ├── event_loop_thread.{h,cpp}        # 单线程跑一个 EventLoop
│   ├── event_loop_thread_pool.{h,cpp}   # N 个 sub-loop，round-robin 分配
│   ├── server.{h,cpp}         # TcpServer：listen + accept + 把 conn 分给 subLoop
│   ├── tcp_client.{h,cpp}     # 主动连接 + 指数退避重连
│   └── timer_queue.{h,cpp}    # 基于 timerfd 的 set<Entry>，O(logN) 增删
├── log_sender/                # 业务侧日志发送（net 之上的应用层，依赖 src/ + util/）
│   ├── log_sender.{h,cpp}     # MPSCQueue + TcpClient 组装：drain→encode→send；含 set_global/global
│   └── logging.h              # LOG_DEBUG/INFO/WARN/ERROR 宏 + format_log + 编译期级别开关
└── log_server/                # 业务层验证：日志收集服务
    ├── main.cpp               # 协议拆帧 + 信号处理 + 周期 flush
    ├── log_file.{h,cpp}       # 按日 + 序号滚动的文件落盘
    ├── smoke_client.py        # 简易客户端：发一条/多条 length-prefixed 消息
    └── roll_stress.py         # 压测脚本：往里灌 >10MiB 验证滚动
```

**依赖方向**：`log_sender / log_server / main.cpp / test_*  →  src/  →  util/`。`util/` 自身不 `#include "src/*"`，是单向依赖底层；`log_sender/` 在 net 之上（`log_sender → src → util`）。

## 4. 构建/运行

```bash
# 构建（默认 Debug + compile_commands.json）
cmake -B build && cmake --build build -j

# 跑 echo demo（10s idle 踢人）
./build/epoll_proj
# 另一终端：python3 log_server/smoke_client.py 或自写客户端

# 跑 log_server（默认 10 MiB 翻号）
./build/log_server 9099 ./res/log
python3 log_server/roll_stress.py --port 9099 --total 12M

# 验证细一点的翻号节奏（1 MiB 一翻）
./build/log_server 9099 ./res/log 1
python3 log_server/roll_stress.py --port 9099 --total 5M
ls -la res/log/      # 期望 .log.1..5

# EventLoop 跨线程能力自测
./build/test_event_loop

# TcpClient 连接 + 重连自测
./build/test_tcp_client

# LOG_* 宏端到端自测（自包含，无需外部 log_server；返回码即结论）
./build/test_logging
```

## 5. 工具链约束

- C++20、`-Wall -Wextra`、默认 Debug、生成 `compile_commands.json` 给 clangd
- 仅 Linux（依赖 `epoll` / `timerfd` / `eventfd`）
- 链接 `pthread`
- 测试客户端用 Python3（不依赖第三方库）

## 6. 命名空间

所有库代码在 `namespace epoll_proj { ... }` 内。`log_server/` 也用同一命名空间。
