# SKILL: 排查运行时问题

## 症状 → 怀疑点 速查

| 症状 | 最可能的原因 | 排查方向 |
|---|---|---|
| 客户端连上后没响应 | 业务回调没注册 / 回调里没调 `conn.send` | 看 stdout，是否有 `recv N bytes` 日志 |
| 客户端发的数据被截断 / 乱拼 | 协议拆包写错了，丢了半包 | 检查 `retrieve` 是否多消费了；半包是否被 `retrieve_all` 清空 |
| 客户端断开后服务端进程还有 fd 泄漏 | `Connection::close` 没被调用 | `ls -l /proc/$(pidof epoll_proj)/fd \| wc -l` 观察 |
| `send` 大量数据时内存疯涨 | `output_buffer_` 没水位线 | `top` 看 RSS；ss 看 Send-Q；考虑加水位线 |
| 程序卡死在某个连接上 | 业务回调里有阻塞调用（DB / sleep） | gdb attach 看主线程在哪儿 |
| 偶发 `epoll_ctl: Bad file descriptor` | 双重 close（fd 复用 bug） | 检查是否在 close 后还操作了 fd |
| 客户端 `send` 卡住 | 服务端没读，TCP 反压（**正常现象**） | `ss -tn` 看 Recv-Q，**这不是 bug** |

---

## 必备工具

```bash
# 看进程 fd 数量（连接数 + 3 个标准流 + epfd）
ls /proc/$(pidof epoll_proj)/fd | wc -l

# 看每个 fd 是什么
ls -l /proc/$(pidof epoll_proj)/fd/

# 看 socket 队列堆积
ss -tnp | grep epoll_proj
ss -tn '( sport = :8888 )'   # 看 8888 端口所有连接的 Recv-Q / Send-Q

# 看进程内存
cat /proc/$(pidof epoll_proj)/status | grep -E 'VmRSS|VmSize'

# 抓包
sudo tcpdump -i lo port 8888 -A -nn

# strace 跟系统调用
strace -p $(pidof epoll_proj) -e trace=read,write,epoll_wait,epoll_ctl,close
```

---

## 典型调查流程

### 案例 A：客户端发数据无响应

1. 看服务端 stdout 有无 `recv N bytes` 日志
   - 没有 → 数据没进 `handle_read`，可能 epoll 注册有问题
   - 有 → 业务回调收到了，问题在业务里
2. `ss -tn` 看连接是否还在 `ESTABLISHED`
3. `tcpdump` 看实际网络层有没有数据来回

### 案例 B：内存持续上涨

1. `cat /proc/PID/status | grep VmRSS` 周期采样
2. `ss -tn` 看 Send-Q 是否一直堆着 → 慢消费者，`output_buffer_` 在涨
3. 定位到具体连接 → 决定是踢掉还是加限速

### 案例 C：`Bad file descriptor` 报错

最危险的 bug：**fd 已被关闭，但又被操作了**。

1. 在 `Connection::close()` 处加日志：`std::cout << "close fd=" << fd_;`
2. 在 `epoll_ctl(MOD)` 处加日志，看是否对已 close 的 fd 仍在 MOD
3. 检查 `Connection` 析构时机 vs `epoll_ctl(DEL)` 时机
4. 详见 `CLAUDE/RULES/resource-safety.md` 关于 fd 生命周期的硬规则

---

## 容易踩的坑（本项目特有）

### 1. ET 模式下的"必须读到 EAGAIN"

`handle_read()` 必须循环 read 到 EAGAIN，否则**数据残留在内核缓冲，但不会再触发 EPOLLIN**（除非来新数据）。当前 `connection.cpp` 已正确实现。

如果你修改了 read 循环，**必须保留 `errno == EAGAIN` 时 break 的逻辑**。

### 2. `EPOLLIN | EPOLLOUT` 的处理顺序

在 `server.cpp::run()` 里，先处理 EPOLLIN 再处理 EPOLLOUT。注意中间检查 `connections_.count(fd)` —— 因为业务回调可能 `conn.close()` 把自己删了，下面再访问就是 UAF。

### 3. `conn.send()` 在业务回调里被调用

完全合法，框架支持。注意 `send` 内部可能再次调 `enable_write_events()` → epoll_ctl MOD，确保 fd 此时还有效。

### 4. 慢路径：`accept` 后 epoll_ctl 失败但 fd 已 nonblock

当前代码：失败时 `::close(conn_fd); continue;` —— 正确。不要漏 close。
