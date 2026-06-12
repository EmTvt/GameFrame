# RULE: API 稳定性

> 修改对外接口（`set_*_callback` / `Buffer` 公开方法 / `Connection` 公开方法）前必读。

## 当前对外的核心 API

```cpp
// TcpServer
server.set_message_callback(MessageCallback);

// Connection（业务回调里用）
conn.fd();
conn.peer();
conn.connected();
conn.send(std::string_view);
conn.close();
conn.input_buffer();
conn.output_buffer();

// Buffer（业务在回调里直接操作）
buffer.readable_bytes();
buffer.peek();
buffer.readable_view();
buffer.retrieve(size_t);
buffer.retrieve_all_as_string();
buffer.append(...);
```

## 改 API 前必须回答的 3 个问题

### Q1：现有所有调用点都改了吗？

```bash
# 找一遍现有调用点
grep -rn "set_message_callback" .
grep -rn "\.send(" src/ main.cpp
grep -rn "input_buffer" src/ main.cpp
```

### Q2：回调签名变化是否兼容？

最容易踩坑的就是回调签名。比如：

- `void(Connection&, std::string_view)` → `void(Connection&, Buffer&)` 是**破坏性**变化
- 加新可选参数 → 应该用**新增一个回调名**（如 `set_xxx_callback`），不要改老的

### Q3：默认值是否合理？

新加的成员（如水位线大小）必须给默认值，确保**老代码不改也能编译运行**。

## 不能轻易动的关键约定

### 1. `MessageCallback` 签名

```cpp
using MessageCallback = std::function<void(Connection&, Buffer&)>;
```

- 拿到 `Buffer&` 而不是 `const&` —— 业务必须能 `retrieve`
- 拿到 `Connection&` —— 业务能 `send` / `close`
- **永远不要**改成传 `std::string` / `std::string_view`，会让业务无法处理半包

### 2. `Buffer` 的两段游标语义

```
| prependable | readable | writable |
0 ←→ reader  ←→  writer  ←→  size()
```

- `peek()` 返回 readable 起始指针
- `retrieve(n)` 推进 reader_index_，**不释放内存**
- `append` 写入 writable 区
- 这些行为是业务依赖的核心约定，**改了等于全员改**

### 3. `Connection::send` 的双路径行为

```cpp
void Connection::send(std::string_view data);
```

业务能依赖的行为：
- 不阻塞，立刻返回
- 数据要么进了内核（快路径），要么进了 `output_buffer_`（慢路径）
- 不会丢数据（除非连接已关）
- 可以从业务回调里调用，可以连续调用

如果以后改成异步投递（多线程版本要做），**这个语义必须保留**，否则业务全坏。

### 4. 状态查询

```cpp
bool conn.connected();
```

业务可能依赖它判断"还能不能 send"。**永远不能让它返回 true 但 send 实际无效**。

## 加新回调的标准模式

```cpp
// 1) 在 Connection 里定义类型 + 注册接口 + 字段
using NewCallback = std::function<void(Connection&)>;
void set_new_callback(NewCallback cb) { new_cb_ = std::move(cb); }
private:
    NewCallback new_cb_;

// 2) 在合适时机触发，必须判空
if (new_cb_) new_cb_(*this);

// 3) main.cpp 不需要改，业务想用就用
```

**新加回调应当是可选的**——`main.cpp` 的 echo demo 不需要注册它就能正常跑。

## 改 API 的必须流程

1. 先在分支上改
2. 改完跑一遍：`cmake --build build && ./build/epoll_proj` + telnet 测试
3. 在 `CLAUDE.md` 第 3 节"已完成的能力"里同步更新
4. 如果是破坏性变化，更新 `main.cpp` demo 演示新用法
