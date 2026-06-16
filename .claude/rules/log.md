# Rule: Logging Code

> 改 `log_server/`、未来的 `log_sender/`、以及任何与 LogFile / LOG_INFO 宏 / MPSCQueue 相关的代码时**必须**遵守。

## 1. 两边职责边界（必读）

```
业务进程                              日志进程
─────────────────                    ─────────────────
LOG_INFO 宏       ──┐
                    │ 纯内存
   MPSCQueue<str>  ◄┘
        │  drain
        ▼
   LogSender 线程
   (TcpClient)     ──── TCP ────►   log_server (TcpServer)
                                          │ 拆帧
                                          ▼
                                       LogFile 落盘
```

- **业务侧（LOG_INFO / MPSCQueue / LogSender）**：零阻塞、纯内存优先。**绝对不允许**在 `LOG_INFO` 调用栈上做 I/O（write / fsync / send / mutex 长持有都不行）。
- **日志侧（log_server / LogFile）**：允许阻塞 fwrite，但落盘节奏要可控（见下面 fflush 规则）。

## 2. log_server / LogFile 规则

### 2.1 文件命名与滚动
- 文件名固定格式：`<basedir>/<basename>.YYYYMMDD.log.<N>`，N 从 1 起
- 滚动触发：
  - **跨日**：直接重置 N=1（新的一天从 1 开始）
  - **当前文件 size ≥ roll_size**：N+1，新建文件
- 启动时扫 `basedir`，复用当天最大序号文件；若该文件未满 `roll_size` 则**追加续写**
- `LogFile::open_file` 必须用 `std::filesystem::file_size()` 校准 `written_bytes_`，否则续写场景会立刻翻号或永不翻号（已修过的 bug）

### 2.2 写盘策略
- 写用 `fwrite`（or `fwrite_unlocked`）+ 行尾 `\n` 方便 `tail -f`
- `written_bytes_` 在每次 append 后累加，用于触发滚动判断
- **不要**在每条日志后都 `fflush`：log_server 当前由 mainLoop 定时器**每秒** `fflush` 一次（自递归挂下一发）。改这个节奏要在 DECISIONS.md 留记录。
- 是否 `fsync`：当前**不做** fsync。理由：fflush 已经把数据交给内核，进程崩溃不丢；机器掉电场景接受最多 1 秒丢失。改这个决策要在 DECISIONS.md 留记录。

### 2.3 信号处理
- `SIGINT` / `SIGTERM` → `loop->quit()` → 优雅退出 → LogFile 析构 fflush + fclose（**0 丢失**）
- `SIGPIPE` 必须 `signal(SIGPIPE, SIG_IGN)`：连接被对端 reset 时不要让进程崩
- 信号处理器只设标志位 / 调 `loop->quit()`，**不**在信号上下文里做 I/O
- `SIGKILL` 救不了，部分日志会丢——告诉用户用 SIGTERM/SIGINT

### 2.4 协议
- `uint32_t len(big-endian) | payload`
- `len` 必须做上限校验（默认 16 MiB），防异常 length 吃爆内存
- 拆帧逻辑请用 `LengthPrefixedCodec`（NEXT_TASKS Task 1 完成后），不要在 lambda 里再写一遍

## 3. 业务侧（LogSender / LOG_INFO / MPSCQueue）规则

### 3.1 LOG_INFO 不允许做的事
- ❌ 任何 I/O（write / send / fsync / fopen）
- ❌ 长持锁（>1µs 的 mutex）
- ❌ 异常抛出（日志路径必须 noexcept）
- ❌ 调用可能阻塞的标准库函数（`std::cerr` / `printf` 在锁竞争下也算）

### 3.2 LOG_INFO 必须做的事
- ✅ 在调用线程做格式化（`format_log` 返回 `std::string`），避免跨线程传 va_list
- ✅ push 到 `MPSCQueue` 是纯内存操作（mutex 短临界区或 lock-free CAS）
- ✅ 失败（队列满）时**丢弃最旧**，不阻塞调用方
- ✅ 编译期可通过宏开关关闭低级别日志（零运行时开销）

### 3.3 MPSCQueue 设计约束
- **有界**：容量上限明确（条数或字节数），满则丢最旧
- **多生产者**：业务线程并发 push（先 mutex 跑通，lock-free 再说）
- **单消费者**：只有 LogSender 线程 drain，**不要**在多个地方 drain
- **批量 drain**：一次取一批，减少唤醒次数
- 唤醒 LogSender 的方式：边沿通知（`atomic<bool> has_pending_` `false→true` 时才 wakeup），避免每条日志一次 wakeup

### 3.4 LogSender 设计约束
- 持有独立 `EventLoopThread`（自己跑一个 loop）
- 持有一个 `TcpClient` 连 log_server，**启用断线重连**（默认就启用）
- log_server 断线期间日志继续入 MPSCQueue，**不允许**反过来阻塞业务线程
- 进程退出：`stop()` → drain 剩余 → 等发送完成（带超时，比如 2s）→ disconnect

## 4. 不允许做的事（综合）

- ❌ 在 `src/` 网络库里 `#include "log_server/..."` 或 `#include "log_sender/..."`（单向依赖）
- ❌ 在 LOG_INFO 调用栈上访问 LogFile / 文件系统
- ❌ 业务进程把日志直接写本地文件（绕过 LogSender → log_server 链路）。除非是**LogSender 自己**写一个本地 fallback 文件，且需在 DECISIONS.md 立项
- ❌ 日志协议帧不带长度前缀（`tail -f` 友好换行不算协议）

## 5. 风格

- 临时调试输出统一用 `[DIAG ...]` 前缀，事后 `grep -n '\[DIAG' log_server src log_sender` 一把清掉
- LogFile / LogSender / MPSCQueue 头文件顶部都要有"为什么存在 / 解决什么问题"中文注释
- 不要在日志路径上加新依赖（jsoncpp / fmtlib 等）；标准库够用

## 6. 改完之后必须做

- log_server 改动 → 跑：
  ```bash
  ./build/log_server 9099 ./res/log 1
  python3 log_server/roll_stress.py --port 9099 --total 5M
  ls -la res/log/   # 期望 .log.1..5
  ```
- LogSender 改动 → 至少要有：业务侧线程并发 push + 断网期间不丢失（在容量内）+ 重连后能续上的 smoke 测试
- 在回答里说明：日志路径上有没有引入新的阻塞点
