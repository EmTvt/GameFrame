# Project Context

> 项目本身是什么、为什么存在、产物有哪些。**稳定信息**，很少变动。

## 1. 项目定位

一个**从零手写的 Linux epoll C++ 网络库**，参照 muduo 的设计逐步演进；目前已经把"主从 Reactor + 定时器 + 跨线程任务投递 + 背压"都跑通了，正在用它搭建一个**独立的日志收集服务器 `log_server`** 作为真实业务验证。

不是工业级框架，是教学/学习驱动：每加一个组件都对应一个能讲清楚动机的"步"，**演进顺序优先于一次到位**。

## 2. 顶层产物（四个可执行）

| target | 入口文件 | 用途 |
|---|---|---|
| `epoll_proj`        | `main.cpp`              | echo demo + idle timeout（10s 不发数据踢人）。验证 TcpServer + TimerQueue + Connection::set_context 整条链路。 |
| `test_event_loop`   | `test_event_loop.cpp`   | 最小测试：跨线程 `run_in_loop` 投递任务能落到 loop 线程执行。 |
| `test_tcp_client`   | `test_tcp_client.cpp`   | TcpClient 验证：连接 echo server → 主动 close → 自动重连 → 再次 echo。 |
| `log_server`        | `log_server/main.cpp`   | 独立进程，监听 TCP，按 `len|payload` 协议收日志，按"日 + 大小"滚动落盘。 |

## 3. 目录结构与模块职责

```
epoll_proj/
├── CMakeLists.txt
├── CLAUDE.md                  # 新会话入口（短）
├── docs/                      # 项目文档（本目录）
├── .claude/                   # 规则与 skill
├── main.cpp                   # epoll_proj demo 入口（echo + idle）
├── test_event_loop.cpp        # EventLoop 跨线程能力测试
├── test_tcp_client.cpp        # TcpClient 连接/重连测试
├── src/                       # 网络库本体
│   ├── buffer.h               # readable / writable / prependable 三段式 Buffer
│   ├── channel.{h,cpp}        # fd 在 epoll 视角的"代言人"，含 tie() 生命周期保护
│   ├── connection.{h,cpp}     # 单条 TCP 连接：input/output buffer + 4 类回调 + 背压 + context<T>
│   ├── event_loop.{h,cpp}     # epoll_wait + Channel 派发 + wakeup_fd + pending_functors + TimerQueue
│   ├── event_loop_thread.{h,cpp}        # 单线程跑一个 EventLoop
│   ├── event_loop_thread_pool.{h,cpp}   # N 个 sub-loop，round-robin 分配
│   ├── server.{h,cpp}         # TcpServer：listen + accept + 把 conn 分给 subLoop
│   ├── tcp_client.{h,cpp}     # 主动连接 + 指数退避重连
│   └── timer_queue.{h,cpp}    # 基于 timerfd 的 set<Entry>，O(logN) 增删
└── log_server/                # 业务层验证：日志收集服务
    ├── main.cpp               # 协议拆帧 + 信号处理 + 周期 flush
    ├── log_file.{h,cpp}       # 按日 + 序号滚动的文件落盘
    ├── smoke_client.py        # 简易客户端：发一条/多条 length-prefixed 消息
    └── roll_stress.py         # 压测脚本：往里灌 >10MiB 验证滚动
```

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
```

## 5. 工具链约束

- C++20、`-Wall -Wextra`、默认 Debug、生成 `compile_commands.json` 给 clangd
- 仅 Linux（依赖 `epoll` / `timerfd` / `eventfd`）
- 链接 `pthread`
- 测试客户端用 Python3（不依赖第三方库）

## 6. 命名空间

所有库代码在 `namespace epoll_proj { ... }` 内。`log_server/` 也用同一命名空间。
