# SKILL: 扩展框架能力

> 适用任务：给框架本身加新功能（不只是写业务回调），比如水位线、定时器、Connection 上下文等。

## 改动范围速查

| 想加的能力 | 主要改的文件 | 是否破坏 API |
|---|---|---|
| WriteCompleteCallback | `connection.h/.cpp` | 否（新增可选回调） |
| 高/低水位线 | `connection.h/.cpp` | 否（新增可选回调） |
| `Connection::set_context` | `connection.h` | 否（新增方法） |
| 定时器 | 新增 `timer.h/.cpp` + `server.cpp` 接 timerfd | 否（业务可选用） |
| 多线程 EventLoop | 大改：拆分 `EventLoop`、`TcpServer` 持有 loop pool | **是（重构）** |

---

## 模板 1：加 WriteCompleteCallback

**用途**：当 `output_buffer_` 全部发出去时通知业务，业务据此决定要不要继续塞数据（典型场景：分块下大文件）。

### 改动点

`connection.h`：

```cpp
using WriteCompleteCallback = std::function<void(Connection&)>;

void set_write_complete_callback(WriteCompleteCallback cb) {
    write_complete_cb_ = std::move(cb);
}

private:
    WriteCompleteCallback write_complete_cb_;
```

`connection.cpp` 的 `handle_write()` 末尾：

```cpp
if (output_buffer_.empty()) {
    disable_write_events();
    if (write_complete_cb_) write_complete_cb_(*this);
}
```

`send()` 末尾（如果直接 write 一次性发完了）也要触发？**不要**——muduo 的设计是只在异步发完时才回调，避免业务收到回调时栈帧深度不可控。

---

## 模板 2：加高/低水位线

**用途**：output_buffer_ 涨太大时通知业务"压一下"，防止 OOM。

### 改动点

`connection.h`：

```cpp
using HighWaterMarkCallback = std::function<void(Connection&, size_t)>;

void set_high_water_mark_callback(HighWaterMarkCallback cb, size_t mark) {
    high_water_mark_cb_ = std::move(cb);
    high_water_mark_ = mark;
}

private:
    HighWaterMarkCallback high_water_mark_cb_;
    size_t high_water_mark_ = 64 * 1024 * 1024;  // 默认 64MB
```

`connection.cpp` 的 `send()` 中，**追加到 output_buffer_ 之后**：

```cpp
output_buffer_.append(ptr, remaining);
if (high_water_mark_cb_ &&
    output_buffer_.readable_bytes() >= high_water_mark_) {
    high_water_mark_cb_(*this, output_buffer_.readable_bytes());
}
enable_write_events();
```

业务侧典型应对：暂停产生数据，或直接 `conn.close()` 踢掉慢消费者。

---

## 模板 3：加 Connection 上下文（HTTP 解析器要它）

**用途**：业务想给每个 Connection 挂一个自己的状态对象（如 HTTP 解析器实例）。

### 改动点

`connection.h`：

```cpp
#include <any>

class Connection {
    // ...
    void set_context(std::any ctx) { context_ = std::move(ctx); }

    template <typename T>
    T& get_context() { return std::any_cast<T&>(context_); }

private:
    std::any context_;
};
```

业务用法：

```cpp
struct HttpContext { /* 状态机 */ };

// 新连接到来时（在 message_cb 第一次进入时初始化即可）
if (!conn.has_context()) conn.set_context(HttpContext{});

// 之后每次回调
auto& ctx = conn.get_context<HttpContext>();
ctx.parse(input);
```

⚠️ 别用 `void*` —— 会导致类型擦除后业务自己 cast 错。`std::any` 有运行时类型检查。

---

## 模板 4：加定时器

最简方案：**timerfd + epoll**。

### 改动点

新增 `src/timer.h`：

```cpp
class TimerQueue {
public:
    using TimerId = uint64_t;
    using Callback = std::function<void()>;

    explicit TimerQueue(int epfd);
    ~TimerQueue();

    TimerId add_timer(int timeout_ms, Callback cb, bool repeat = false);
    void cancel(TimerId id);

    void handle_read();  // timerfd 可读时调

    int fd() const { return timerfd_; }

private:
    int timerfd_;
    int epfd_;
    // 用最小堆或 std::set<std::pair<expire_time, id>> 维护
};
```

`server.cpp` 中：
- 构造时 `timerfd_create + epoll_add`
- `epoll_wait` 收到 timerfd 可读 → `timer_queue_.handle_read()`

---

## 重构警告：多线程 EventLoop

如果要做多线程版本：

1. 抽出 `EventLoop` 类（持有 `epfd_` 和事件循环）
2. `Connection` 持有 `EventLoop*`，所有对自己的操作都通过 `loop_->run_in_loop(...)` 投递
3. `TcpServer` 持有一个 main loop（accept）+ 一个 sub loop pool（处理已连接 fd）
4. 跨线程唤醒用 `eventfd`
5. **`Connection` 必须改成 `shared_ptr` + `enable_shared_from_this`**，否则异步回调里对象可能已被销毁

这是大重构，建议另起分支，先跑通单线程稳定版再做。

---

## 扩展框架的检查清单

- [ ] 新加的回调都是**可选**的（`if (cb_) cb_(...)`），不破坏现有业务
- [ ] 新加的字段都给了**默认值**，存量调用方不需要改
- [ ] 改完后 `main.cpp` 的 echo demo 仍然能跑（基本回归）
- [ ] 新能力在 `CLAUDE.md` 第 3 节"已完成的能力"里更新一行
- [ ] 如果改了资源管理逻辑（fd 关闭时机、生命周期），**必须先读 `CLAUDE/RULES/resource-safety.md`**
