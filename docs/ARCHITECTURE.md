# Architecture

> 当前架构（不是愿景）。模块边界、所有权关系、调用流向。

## 1. 网络库分层

```
┌─────────────────────────────────────────────────────────────┐
│  业务层： main.cpp / log_server / test_*                     │
│  通过回调与库交互： ConnectionCallback / MessageCallback     │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│  会话层： TcpServer / TcpClient / Connection                 │
│  - TcpServer  ：被动 accept                                  │
│  - TcpClient  ：主动 connect + 指数退避重连                  │
│  - Connection ：一条 TCP 连接（input/output buffer + 回调）  │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│  事件层： EventLoop / Channel / TimerQueue                   │
│  - EventLoop  ：epoll_wait + 派发 + pending functors + timer │
│  - Channel    ：fd 在 epoll 视角的"代言人"，挂回调           │
│  - TimerQueue ：timerfd + set<Entry>                         │
│  - 跨线程    ： wakeup_fd (eventfd) + pending_functors_      │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│  数据层： Buffer                                             │
│  prependable / readable / writable 三段式                    │
└─────────────────────────────────────────────────────────────┘
```

## 2. 主要调用流（accept 路径）

```
main
  -> TcpServer::run()
  -> EventLoop::loop()                  // mainLoop
       └─ epoll_wait
       └─ accept_channel.handle_event()
            └─ TcpServer::handle_accept()
                 └─ EventLoopThreadPool::get_next_loop()    // round-robin 选 subLoop
                 └─ Connection 在 subLoop 上构造
                 └─ subLoop->run_in_loop(Connection::start)  // 跨线程投递
                      └─ Channel 注册到 subLoop 的 epoll
                      └─ ConnectionCallback(conn)            // 业务感知到新连接
```

## 3. 主要调用流（数据路径）

```
subLoop epoll_wait
  -> Channel::handle_event_with_guard   // tie() 锁定 Connection 生命周期
       └─ Connection::handle_read
            ├─ readv 到 input_buffer_
            ├─ MessageCallback(conn, input_buffer_)   // 业务拆帧
            └─ EOF → MessageCallback 派一次再 close   // 防丢最后一帧

业务调用 conn->send(data)
  -> Connection::send_in_loop（跨线程则 run_in_loop）
       ├─ 优先直接 write(fd)
       ├─ 剩余写不完 → append 到 output_buffer_ + 注册 EPOLLOUT
       ├─ output_buffer_ 越过 high_water_mark → HighWaterMarkCallback (异步)
       └─ EPOLLOUT 触发 → handle_write → 排空 output_buffer_
            └─ 排空后 → WriteCompleteCallback (异步)
```

## 4. 主要调用流（TcpClient 主动连接）

```
TcpClient::connect()
  -> connect_in_loop()                  // 强制在 loop 线程
       ├─ 非阻塞 socket
       ├─ ::connect → 通常返回 EINPROGRESS
       ├─ connect_channel_ 监听 EPOLLOUT
       └─ EPOLLOUT 触发
            └─ handle_write
                 ├─ getsockopt(SO_ERROR)
                 ├─ 成功 → new_connection(fd)         // 转交给 Connection
                 └─ 失败 → handle_error → retry()     // 指数退避
```

## 5. 所有权（Ownership）

| 资源 | 拥有者 | 说明 |
|---|---|---|
| EventLoop | 创建它的线程 / 外部 | 整个 loop 生命周期内只能在 owner_tid 上操作 |
| Channel | Connection / TcpServer / TcpClient / TimerQueue | Channel 不拥有 fd，只代表事件兴趣 |
| fd | Connection（连接 fd） / TcpServer（listen fd） / TcpClient connecting 阶段 | fd 关闭由 owner 负责 |
| Connection | `shared_ptr<Connection>`（ConnectionPtr） | 跨线程 / 异步回调一律拷贝 ConnectionPtr 延寿 |
| Buffer | Connection（每条连接一对：input/output） | Buffer 只管字节，不感知 epoll/fd/线程 |
| Timer | TimerQueue 内部 set | 取消用 TimerId（带 weak 校验防 ABA） |

**关键约束**：
- **Channel 不拥有 fd 生命周期**。Connection / Acceptor / TcpClient 各自负责自己 fd 的 close。
- **Connection 关闭顺序**：先把 Channel 从 EventLoop 注销 (`disable_all` + `remove`)，再走业务关闭回调，最后才让 ConnectionPtr 引用降为 0。

## 6. 线程模型

### 主从 Reactor

```
┌───────────────┐     accept      ┌───────────────┐
│   mainLoop    │ ───────────────►│   subLoop 0   │
│ (TcpServer    │                 │ (Connection 处│
│  独占)        │ ───────────────►│  理 I/O)      │
└───────────────┘                 ├───────────────┤
                                  │   subLoop 1   │
                                  └───────────────┘
                                  ...
```

- mainLoop 只做 accept；subLoop 个数由 `TcpServer(port, num_threads)` 指定（0 表示单线程模式：accept + I/O 共用）
- 一条 Connection 一旦分给某个 subLoop，**生命周期内不会迁移**

### 跨线程投递

任何线程要让 EventLoop 干活，都走：
```cpp
loop->run_in_loop(functor);   // 当前 = loop 线程：直接执行；否则 queue + wakeup
loop->queue_in_loop(functor); // 强制入队 + wakeup（eventfd）
loop->run_after(ms, fn);      // 定时器（timerfd）
```

## 7. log_server 业务架构

当前形态（**单线程**，第 12 步）：
```
client → TCP → log_server (单 EventLoop)
                  ├─ 拆帧（length-prefix）
                  ├─ LogFile::append（同线程阻塞 fwrite）
                  └─ run_after(1000, flush)  // 每秒 fflush
```

最终目标形态（业务进程接日志：见 NEXT_TASKS）：
```
业务线程 LOG_INFO → MPSCQueue → LogSender 线程 (TcpClient) → log_server
```
