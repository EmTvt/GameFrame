# RULE: 资源安全（fd / 内存 / 生命周期）

> ⚠️ **这是本项目最重要的规则**。改动 `Connection` / `TcpServer` 的资源管理逻辑前**必须**通读本文。

## 1. fd 的所有权

- **谁创建谁负责关**：
  - `accept` 返回的 fd → 立刻交给 `Connection` 持有，`Connection` 负责关
  - `socket` 创建的 listen fd → `TcpServer` 持有，`TcpServer` 析构时关
  - `epoll_create1` 返回的 epfd → 同上
- **不允许多个对象持有同一个 fd 的所有权**

## 2. `Connection` 的 fd 状态机

这是个**反直觉但故意设计的**规则，必须严格遵守：

```
state_       fd_       含义
─────────────────────────────────────────────
kConnected   有效值     正常运行，I/O 可用
kDisconnected 有效值    close() 已调过，fd 已被 ::close，但保留值给 close_cb_ 用
              -1       析构兜底已执行（仅在没调过 close() 的情况下）
```

### 关键约束

1. **`Connection::close()` 不会把 `fd_` 置 -1**
   - 原因：让 `close_cb_` 里的 Server 还能通过 `c.fd()` 在 `connections_` map 里找到自己
   - 见 `connection.cpp::close()` 的注释

2. **`Connection::~Connection()` 通过 `state_` 判断要不要 close，不通过 `fd_`**
   - 已 `close()` 过的（`state_ == kDisconnected`）→ 析构里**不再 close**
   - 没 close 过的（`state_ == kConnected`）→ 析构里兜底 close

3. **判断"连接是否还活着"用 `state_`，不用 `fd_ >= 0`**
   - `send()` 第一行：`if (state_ != kConnected) return;`
   - 这是为了防止 close 之后业务再调 send 误操作已关 fd

### 为什么不能改成"close 时立刻 fd_=-1"？

```cpp
// ❌ 错误改法
void Connection::close() {
    state_ = kDisconnected;
    ::close(fd_);
    fd_ = -1;          // ← 看起来很对
    if (close_cb_) close_cb_(*this);   // ← 但这里 c.fd() 已经是 -1 了
}
```

后果：`server.cpp` 的 close lambda 里 `remove_connection(c.fd())` 会拿到 -1，找不到对应的 connection，**导致 connections_ map 永远删不掉，内存泄漏**。

历史曾用 lambda 捕获 fd 绕开（`[this, conn_fd]{...}`），但这等于把 Connection 的状态泄漏到外部 lambda，更难维护。

## 3. fd 复用 bug（最阴险的 UB）

```
T0: Connection A close 了 fd=5
T1: 内核把 5 又分给了新 accept 的 Connection B
T2: A 的析构函数又 close 一次 fd=5
        ↓
    把 B 的连接关了！B 可能还在传数据 → 数据丢失 / 错乱
```

防御方法（已实现）：

- `close()` 之后绝不在同一对象里再 `::close(fd_)`
- 析构时用 `state_` 判断（已 close 过就跳过）

**如果要新增析构 / cleanup 路径，必须遵守这个不变量。**

## 4. `Connection` 在回调中被销毁的风险

当前代码里，**`server.cpp::run()` 处理 EPOLLIN 后会再判断一次 `connections_.count(fd)`**：

```cpp
if (ev & EPOLLIN) {
    conn.handle_read();
    if (!connections_.count(fd)) continue;   // ← 关键
}
if (ev & EPOLLOUT) {
    conn.handle_write();
}
```

原因：业务回调可能在 read 处理过程中调 `conn.close()`，触发 `remove_connection` → `connections_.erase` → `unique_ptr` 析构 → `Connection` 销毁。如果不做这个检查，下面 `conn.handle_write()` 就是 **use-after-free**。

**改这段代码时务必保留这个检查**。

## 5. epoll 与 fd 的同步

- `epoll_ctl(EPOLL_CTL_ADD)` 只在 `accept` 成功后做一次
- `epoll_ctl(EPOLL_CTL_MOD)` 在 `update_events` 里做（开关 EPOLLOUT）
- `epoll_ctl(EPOLL_CTL_DEL)` 在 `remove_connection` 里做，**且必须在 `::close(fd)` 之前**
  - 内核行为：fd 被 close 后，epoll 会自动从关注列表里移除，但显式 DEL 更稳

当前代码里 `Connection::close()` 先 `::close(fd_)` 再调 `close_cb_` → Server 的 `remove_connection` 才 `epoll_ctl(DEL)`。**这个顺序是反的**，但 Linux 内核能容忍（DEL 一个已关闭的 fd 会返回 EBADF，但不影响 epoll 内部状态）。

⚠️ 如果要严格起见，可改成：先 `epoll_ctl(DEL)` 再 `::close`。但这会要求 Connection 反向通知 Server 先 DEL，结构更绕。**当前实现是有意识的妥协**，改之前先想清楚。

## 6. 内存

- 用户态缓冲区（`input_buffer_` / `output_buffer_`）当前**没有上限**
- 慢消费者会让 `output_buffer_` 无限增长 → OOM
- 这不是 bug，是已知短板，第三步会加水位线
- 在加水位线之前，**禁止用本项目接受不可信流量**

## 7. 内存数据结构生命周期

```
TcpServer
  └── connections_ : unordered_map<int, unique_ptr<Connection>>
                                              │
                                              └── 持有 fd / Buffer / callbacks
```

- `TcpServer` 析构 → `connections_.clear()` → 所有 `unique_ptr<Connection>` 析构 → 各自析构 fd
- **不要让 `Connection` 反向持有 `TcpServer*`**（讨论过，回调方式更优）
- 想加 `Connection` 间的引用？→ 用 fd 作 key 通过 `TcpServer` 间接访问，**不要存裸指针**
