# Current State

> 当前做到哪一步。新会话首先看这份。

Last updated: 2026-06-22

## Working（已稳定，可用）

### util（util/，纯头文件，不依赖 net）
- **Buffer**：三段式（prependable / readable / writable），动态扩容
- **LengthPrefixedCodec**：4B 大端 length + payload；半包 / 粘包 / 非法长度（含 0 与超 max_frame_len）都覆盖；log_server 拆帧已切到 `LengthPrefixedCodec::decode`
- **MPSCQueue\<T\>**：多生产者单消费者有界队列；mutex + ring buffer；满则丢最新 + `dropped_` 计数；`push` 返回 `{ok, wake_up}` 做边沿唤醒；4 producer × 5w 条并发计数总账通过

### 网络库（src/）
- **Channel**：fd 事件代言人；`tie()` 生命周期保护已生效
- **EventLoop**：epoll_wait + Channel 派发 + wakeup_fd（eventfd）+ pending_functors + TimerQueue
- **跨线程能力**：`run_in_loop` / `queue_in_loop` 实现 + 测试覆盖（test_event_loop）
- **TimerQueue**：基于 timerfd + `std::set<Entry>`，O(logN) 增删，支持 cancel
- **Connection**：双向 I/O + 半阻塞 send（EPOLLOUT 回写）+ 4 类回调 + `set_context<T>`
- **背压**：HighWaterMark + WriteComplete（**异步触发，避免回调内自递归**）
- **TcpServer**：listen + accept + 把 conn 分发给 subLoop
- **EventLoopThread / Pool**：N 个 sub-loop，round-robin 分配
- **TcpClient**：非阻塞 connect + 指数退避重连（500ms → 30s 上限）+ test_tcp_client 通过

### log_server
- 单 EventLoop（accept + I/O + 落盘同线程）
- 协议：`uint32_t len(big-endian) | payload`（拆帧已切到 `LengthPrefixedCodec::decode`）
- LogFile 按"日 + 序号"滚动（默认 10 MiB / file，可命令行覆盖）
- 启动时扫目录复用最大序号文件（未满则续写）
- mainLoop 上每秒触发一次 fflush（自递归挂下一发）
- SIGINT/SIGTERM → 优雅退出（fflush + fclose，0 丢失）；SIGPIPE → 忽略

### log_sender（log_sender/，新增）
- **LogSender**：组装 `EventLoop`（外部传入）+ `TcpClient` + `MPSCQueue<string>` 的应用层组件
- 50ms 周期 tick：drain（max_batch_per_tick=1000）→ `LengthPrefixedCodec::encode` → `conn->send`
- push 路径边沿唤醒（false→true 那次额外 `queue_in_loop` 戳醒 sender_loop），低吞吐 → 微秒级落地；高吞吐 → 50ms 摊薄批处理
- 断连期间 push 仍有效：消息正常入队，drain tick 看到 `connection_` 为空 / 未 connected / paused_ 三道闸门全跳过
- 重连成功后 `on_connection` 挂背压回调（HighWater→paused_=true, WriteComplete→恢复）
- `take_dropped()` 在每轮 drain 前调，>0 时合成 `[LOG_DROPPED] N messages lost` 插批次开头
- `stop()`：跨线程派任务 + 同步等：cancel tick → 最后一次 drain+send → `tcp_client_->disconnect()` → **在 sender_loop_ 线程内 reset(tcp_client_)**（TcpClient 析构要求 in_loop_thread）
- test_log_sender：4 producer × 10000 条 push 总耗时 ~12ms；在 cap=5000 队列下，落盘 = ok push + 1 条合成 LOG_DROPPED，对账精确

## In Progress

下一步：**Task 4 `LOG_INFO` 宏** —— 业务侧零阻塞接口，把 `format_log(level, file, line, fmt, ...)` 的结果 push 到全局 LogSender 单例。
LogSender 已就绪，宏只剩"如何选择全局单例 vs 显式构造 + 传递"这一点决策待敲定。

## Known Problems / 待验证

- **`Connection::close()` 不等 outbuf 排空**：LogSender stop 时如果 output_buffer_ 里还压着数据，会随 close 一起丢。当前 best-effort（stop 前 sleep 2s 摊薄），后续做 `shutdown(SHUT_WR)` 半关后才能严格保证 stop 不丢。
- **半关支持缺失**：`Connection::close()` 是直接关，未做"等 outbuf 排空再关"。muduo 的 `shutdown(SHUT_WR)` 行为还没实现。
- **`Connection::force_close_with_delay(ms)` 未实现**：HTTP 之类要求 close 前给客户端最后写一点数据再关的协议会受影响。
- **集成测试稀疏**：现在只有 test_event_loop / test_tcp_client / test_log_sender，缺"TcpServer 多线程压测 + Connection 析构顺序"的覆盖。
- **codec 单元测试缺失**：LengthPrefixedCodec 目前只被 log_server smoke 覆盖，没有针对半包 / 粘包 / 长度=0 / 超 max_frame_len / 边界长度的专用测试。优先级低。
- **log_server 多线程方案未启用**：当前 `num_threads=0`，写盘是阻塞 fwrite。在网络流量上来后会成为瓶颈。
- **Acceptor 没抽出**：listen socket 的 channel/handle_accept 直接挂在 `TcpServer` 里，没独立成 `Acceptor` 类。当前不是痛点。

## 已修复的重要 bug（留档）

- **EOF 丢帧**：`Connection::handle_read` 的 EOF 分支（`n == 0`）原本直接 close，导致最后一轮 read 拿到的数据没派给 message_cb 就被丢掉。**已修**：EOF 路径也会先派一次 `message_cb` 再 close（muduo 标准做法，保留）。
- **诊断输出 `[DIAG ...]`**：connection.cpp / log_file.cpp / log_sender.cpp 中临时诊断 stderr 输出已清理完毕。
- **LogSender 析构跨线程**：第一版 LogSender 在 main 线程析构时会带 TcpClient 一起析构，触发 `~TcpClient` 的 `assert_in_loop_thread` 致命错误。**已修**：`stop_in_loop` 内在 sender_loop_ 线程里 `tcp_client_.reset()`，main 线程的 unique_ptr 析构时已是 nullptr。

## 演进步序（历史快照，给新会话定位）

| # | 主题 | 状态 |
|--:|---|---|
| 1 | 单 fd epoll wrapper | ✅（早期合并进 EventLoop） |
| 2 | Buffer 三段式 | ✅ |
| 3 | TcpServer + Connection 双方向 I/O | ✅ |
| 4 | output_buffer + send 半阻塞 + EPOLLOUT 回写 | ✅ |
| 5 | EventLoop 抽离 | ✅ |
| 6 | 跨线程投递（owner_tid + wakeup + pending_functors） | ✅ |
| 7 | Channel 抽象 + epoll data.ptr 零查表 | ✅ |
| 7.5 | Channel::tie() 生命周期保护 | ✅ |
| 8 | 主从 Reactor（EventLoopThread/Pool） | ✅ |
| 9 | TimerQueue（timerfd + set + cancel） | ✅ |
| 10 | Connection::set_context\<T\> | ✅ |
| 11 | 背压（HighWaterMark + WriteComplete） | ✅ |
| 12 | log_server 第一阶段（单连接 length-prefix 落盘） | ✅ |
| 12.1 | LogFile 按日+序号滚动 | ✅ |
| 12.2 | 清理诊断输出 | ✅ |
| 13 | TcpClient（非阻塞 connect + 指数退避重连） | ✅ |
| 14 | LengthPrefixedCodec（共享编解码） | ✅ |
| 15 | MPSCQueue\<T\>（业务 → Sender 桥梁，丢最新策略） | ✅ |
| 15.1 | util/ 目录拆出（Buffer + MPSCQueue 移出 src/） | ✅ |
| 15.2 | LengthPrefixedCodec 也迁入 util/（见 DECISIONS 2026-06-18 晚条） | ✅ |
| 16 | LogSender（独立线程 + TcpClient 发日志） | ✅ |
| 17 | LOG_INFO 宏（业务侧接口） | ⬜ 下一个 |
