# Debugging Notes

> 反复踩过的坑、调试技巧、常见现象的根因。**新坑请追加**。

---

## 测试客户端的选择

### ❌ 不要用 `telnet` 测 length-prefixed 协议
telnet 会注入 IAC 协商字节，把 length-prefixed 协议解析出天文数字 → "bad frame length=..."。

### ✅ 用 Python 脚本
- `log_server/smoke_client.py`：发一条/多条 length-prefixed 消息
- `log_server/roll_stress.py`：压测脚本，往里灌 >10 MiB 验证文件滚动

### ✅ echo demo 可以 telnet
`epoll_proj` 的 echo demo（`main.cpp`）没有协议层，telnet 测试 OK：
```bash
./build/epoll_proj
# 另一终端
telnet 127.0.0.1 8888
```

---

## 已修复的关键 bug

### EOF 丢帧（client `sendall` 紧跟 `shutdown(SHUT_WR)`）
**现象**：客户端发完最后一帧立刻 `shutdown(SHUT_WR)`，server 收不到这一帧。

**根因**：内核会把数据 + FIN 在同一次 EPOLLIN 里送达。`Connection::handle_read` 收到 `n == 0` 时直接 close，但缓冲区里其实还有未派的完整数据。

**修复**：EOF 分支也派一次 `message_cb_(conn, input_buffer_)` 再 `handle_close()`。这是 muduo `handleRead` 的标准做法，**保留**。

### LogFile 续写时进程重启会立刻翻号 / 永不翻号
**根因**：`open_file` 没有用 `file_size()` 校准 `written_bytes_`，启动时把它当 0 算。

**修复**：扫到旧文件后用 `std::filesystem::file_size()` 初始化 `written_bytes_`。

### 诊断输出污染
之前 `connection.cpp` / `log_file.cpp` 里挂了一堆 `[DIAG ...]` stderr 输出，已清理。新加临时调试一律用 `[DIAG ...]` 前缀，便于事后 `grep -n '\[DIAG' src log_server | xargs -...` 一把清掉。

---

## 常见配置注意

### `roll_size` 默认值
log_server 当前默认 10 MiB；命令行第 4 个参数可以传 MiB 整数覆盖。压测时调小到 1 MiB 验证翻号：
```bash
./build/log_server 9099 ./res/log 1
python3 log_server/roll_stress.py --port 9099 --total 5M
ls -la res/log/   # 期望 .log.1..5
```

### `epoll_wait` 不加 `SA_RESTART`
信号要能立刻打断它，否则 SIGINT/SIGTERM 退出会卡到下次 I/O。详见 DECISIONS.md。

### `Channel::tie` 的 `weak_ptr<void>`
故意不绑 Connection 类型，否则 Acceptor / TcpClient 没法用同一份 Channel 实现。

---

## 排查清单（出问题时按这个走）

### 程序行为不符合预期
1. 是不是跨线程了？检查所有写 EventLoop / Channel / Connection 的地方都在 owner 线程
   - 怀疑时调 `EventLoop::assert_in_loop_thread()` 自检
2. 是不是 lambda 没捕 ConnectionPtr？看是不是裸引用了已析构的 Connection
3. 是不是同步回调里又调了触发回调的方法？背压相关的 callback 应该走 `queue_in_loop`

### 收不到完整数据 / 丢帧
1. 是不是客户端 close 太快？检查 server 的 `handle_read` EOF 分支有没有派 `message_cb`
2. Buffer 里 readable 区域是否真的有数据：`fprintf(stderr, "[DIAG] readable=%zu\n", buf.readable_bytes())`
3. 协议层拆帧是否正确：用 `xxd` / `od -c` 看落盘内容比对

### 连接断了不重连
1. 检查 `TcpClient::set_retry(true)`（默认就是 true，但是 `disconnect()` 后会把 `user_disconnect_` 置位）
2. 看 `retry_delay_ms_` 是不是已经爬到 30s 上限，等够时间
3. server 是不是根本没起来：`netstat -tlnp | grep <port>`

### 文件没落盘 / 落盘延迟
1. log_server 是不是没收到？`tcpdump -i lo port 9099 -A` 看流量
2. fflush 定时器有没有跑：log_server 启动后等 >1s
3. SIGKILL（kill -9）会跳过析构 → fclose 没执行 → 部分数据丢失。优雅退出请用 SIGTERM/SIGINT

---

## 快速验证命令片段

```bash
# 重新构建（干净）
rm -rf build && cmake -B build && cmake --build build -j

# 看 epoll 系统调用是否如预期
strace -e trace=epoll_create1,epoll_ctl,epoll_wait,read,write,close,connect ./build/test_tcp_client 2>&1 | head -200

# 看进程的 fd 表
ls -la /proc/$(pgrep log_server | head -1)/fd/

# 看是不是真的多线程在跑
ps -T -p $(pgrep epoll_proj | head -1)
```
