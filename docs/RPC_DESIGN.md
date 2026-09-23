# RPC 框架设计

> 在现有 epoll_proj 网络库之上，逐步搭一个**进程间函数调用（RPC）框架**。
> 本文档是"实现前的设计与路线"，供后续会话按卡片逐步落地，**不要凭空猜细节**。
> 风格与 `docs/NEXT_TASKS.md` 一致：每个 Task 给出 文件 / 职责 / 接口草案 / 验收。

Last updated: 2026-08-29

---

## 0. 目标与非目标

**目标**：让业务写成 `EchoRsp rsp = stub.Echo(req);`（或 `co_await stub.Echo(req)`），
底层自动完成「序列化 → 发送 → 网络 → 服务端找到函数并执行 → 返回 → 反序列化」，
把网络细节对业务透明。

**本阶段非目标（明确后置，避免过度设计）**：
- 服务注册/发现中间件（etcd/zk）——**先直连**（配置 host:port），第 8 阶段再上。
- 负载均衡、熔断限流、链路追踪——生产级特性，路线末尾。
- 多语言互通——先只保证 C++ ↔ C++。
- 幂等/自动重试——**默认不自动重试**，从源头回避幂等问题（见 §1.4）。

---

## 1. 关键设计决策（先把认知钉死）

### 1.1 传输层：TCP，不是 UDP

RPC 语义要求"请求-响应可靠对应 + 有序 + 无大小限制"。UDP 不保证可靠/有序、
且有 MTU 限制（大包要自己分片重组），在应用层重造这些等于重新发明 TCP。

**结论：直接复用现有 `TcpServer` / `TcpClient` / `Connection` 作为传输层，不引入 UDP。**
这几个类已经解决了非阻塞 I/O、半阻塞 send + EPOLLOUT 回写、背压、指数退避重连。

### 1.2 帧格式：复用 `LengthPrefixedCodec` 做外层，payload 内再套 RPC header

TCP 是字节流，必须自己划分消息边界。现有 `LengthPrefixedCodec`（4B 大端 length + payload）
已经把粘包/半包/非法长度处理干净，**外层帧直接复用它**，不重复造。

RPC 的信息（是请求还是响应、request_id、方法名、错误码）放进 payload 的**内层 header**：

```
+----------------+-------------------------------------------+
|  4B length(BE) |  payload = [RpcHeader][body(序列化后的参数)] |   <- 外层: LengthPrefixedCodec
+----------------+-------------------------------------------+
                    \__ 内层: RpcCodec 负责 header 的编解码 __/
```

详细字段见 §3。

### 1.3 序列化：先自定义二进制/JSON 跑通，再接 protobuf

- **序列化格式**（对象↔字节流）与**通信协议**（帧怎么划分、header 带什么）是两件事，别混。
- 学习项目建议：**第一版用最朴素的序列化**（方法名是字符串、body 先用 JSON 或裸字符串），
  把 request_id 匹配、跨线程唤醒、超时这些"框架骨架"跑通；**第二版再接 protobuf** 做 body。
- protobuf 在 C++ 里额外解决了"无反射"问题（见 1.5），是生产首选，但不该是第一步的门槛。

### 1.4 request_id ≠ 幂等（两个层次，别混）

- **request_id（框架层，必做）**：一条 TCP 长连接可并发多个请求，响应乱序回来，
  客户端靠 `request_id` 把响应对回当初的调用（找到对应的 promise/callback）。
  **每个请求都要有，`atomic<uint64_t>` 自增即可。目的是"响应路由"，不是幂等。**
- **幂等（业务层，本阶段不做）**：同一操作执行一次和多次结果一致。只有在"超时重试"场景才需要，
  靠业务保证（唯一订单号、DB 唯一约束、去重表）。**本框架默认不自动重试，从源头回避。**

### 1.5 C++ 无反射 → 用"注册表"模拟（对应你在腾讯看到的 map + proto）

Java/Go 有反射，运行时能凭方法名字符串直接 invoke。C++ 编译期就丢了类型信息，
运行时只有一个方法名字符串，必须**启动时手动建一张 `map<方法名 → std::function>`**，
把"字符串 → 可调用对象"显式登记。这就是"没有反射时的手动 vtable"。

protobuf 的 `service` 定义经 `protoc` 生成代码，能**自动生成这张注册表 + stub + 序列化**——
本质是"帮你补上 C++ 缺失的反射"。所以第一版我们手写这张 map，第二版用 proto 生成它。

### 1.6 同步 vs 异步 vs 协程

- **同步 call**：调用线程阻塞等结果 —— `std::promise/future` + 条件变量。
- **异步 call**：传回调，结果回来时在 loop 线程触发回调。
- **协程 call（后置到协程阶段）**：`co_await stub.Method(req)`，底层就是把"resume 协程"
  塞进 §1.4 的 pending 表，与现有 `docs/NEXT_TASKS.md` Task 8 的协程层缝合。

三者共用同一套底层（发送 + request_id + pending 表），只是"结果如何交回业务"不同。

---

## 2. 架构分层：RPC 落在哪一层

RPC 是**业务层之下、会话层之上**的一层应用协议层，不动 `src/` 网络库本体：

```
┌───────────────────────────────────────────────────────────┐
│  业务层：EchoServiceImpl / 业务调用 stub.Echo(req)          │
├───────────────────────────────────────────────────────────┤
│  RPC 层（新增 rpc/）                                        │
│   - RpcServer  ：包 TcpServer；收到帧 → 解 header → 查注册表 → dispatch │
│   - RpcClient  ：包 TcpClient；发请求 → 存 pending → 收响应匹配 → 交回业务 │
│   - RpcCodec   ：RpcHeader 编解码（套在 LengthPrefixedCodec 内层）      │
├───────────────────────────────────────────────────────────┤
│  会话层：TcpServer / TcpClient / Connection（现成，不改）   │
├───────────────────────────────────────────────────────────┤
│  事件层：EventLoop / Channel / TimerQueue（现成，超时用它） │
├───────────────────────────────────────────────────────────┤
│  util 层：Buffer / LengthPrefixedCodec / MPSCQueue（现成）  │
└───────────────────────────────────────────────────────────┘
```

**依赖方向**：`rpc/ → src/ → util/`（与 log_sender 同档）。`rpc/` 里**禁止**加进 `src/`。

---

## 3. 线协议（wire protocol）

外层帧沿用 `LengthPrefixedCodec`。内层 payload 布局（第一版，二进制 header + 字符串 body）：

```
payload:
  uint8   type        // 0 = Request, 1 = Response
  uint64  request_id  // 大端；客户端自增，服务端原样回填
  uint32  status      // 仅 Response 用：0 = OK，非 0 = 错误码（见枚举）
  uint16  method_len  // 仅 Request 用：方法名长度
  bytes   method      // 仅 Request 用：方法名（method_len 字节，如 "EchoService.Echo"）
  bytes   body        // 剩余全部：序列化后的参数/返回值（第一版可为 JSON 或裸串）
```

- `type`：区分请求/响应，决定后面字段怎么解。
- `request_id`：§1.4 的响应路由键。
- `status`：响应的错误码。建议枚举 `kOk / kMethodNotFound / kDeserializeError / kServerError / kTimeout`。
- `method`：`服务名.方法名`，服务端拿它查注册表。
- `body`：真正的载荷。第一版随便（JSON / 裸串），protobuf 阶段换成 `SerializeToString`。

> 字节序统一大端，手写 shift（与 `LengthPrefixedCodec` 的 `peek_uint32_be` 一致，不引 htonl）。
> 版本演进：如需兼容，可在 type 前加 1B `magic/version`；第一版从简不加。

---

## 4. 核心组件设计（接口草案）

> 以下签名是草案，落地时以头文件为准；注释解释**动机**，遵守 `.claude/rules/net.md`。

### 4.1 `rpc/rpc_message.h` —— RpcHeader + RpcCodec（纯头文件，无状态）

```cpp
namespace epoll_proj {

enum class RpcType : uint8_t { kRequest = 0, kResponse = 1 };

enum class RpcStatus : uint32_t {
    kOk = 0, kMethodNotFound, kDeserializeError, kServerError, kTimeout,
};

struct RpcMessage {
    RpcType     type;
    uint64_t    request_id;
    RpcStatus   status;      // 仅 response
    std::string method;      // 仅 request，形如 "EchoService.Echo"
    std::string body;        // 序列化后的参数/返回值
};

// 套在 LengthPrefixedCodec 内层：负责 RpcMessage <-> payload 字符串。
// 无状态、静态函数（拆帧状态已由外层 Buffer + LengthPrefixedCodec 持有）。
struct RpcCodec {
    static std::string encode(const RpcMessage& msg);          // -> payload（再交给 LengthPrefixedCodec::encode）
    static bool        decode(std::string_view payload, RpcMessage& out);  // 非法返回 false
};

}  // namespace epoll_proj
```

**要点**：发送侧 `LengthPrefixedCodec::encode(RpcCodec::encode(msg))` → `conn->send(...)`；
接收侧 `LengthPrefixedCodec::decode(buf, frames)` 拿到一批 payload，逐个 `RpcCodec::decode`。

### 4.2 `rpc/rpc_server.{h,cpp}` —— 服务端

```cpp
class RpcServer {
public:
    // handler 拿到 request body，返回 (status, response body)。
    // 用 std::function 是第一版的"手写反射注册表"；protobuf 阶段由生成代码填这张表。
    using Handler = std::function<RpcStatus(std::string_view req_body, std::string& rsp_body)>;

    RpcServer(uint16_t port, size_t num_threads = 0);

    // 注册一个方法。method 形如 "EchoService.Echo"。
    // 必须在 run() 前注册完（注册表只读后无锁）。
    void register_method(std::string method, Handler h);

    void run();

private:
    void on_message(const ConnectionPtr& conn, Buffer& input);   // 挂到 TcpServer::message_cb
    void dispatch(const ConnectionPtr& conn, const RpcMessage& req);

    TcpServer server_;
    std::unordered_map<std::string, Handler> handlers_;   // 方法名 -> 可调用对象（§1.5）
};
```

**dispatch 流**：外层拆帧 → `RpcCodec::decode` → 按 `method` 查 `handlers_` →
- 命中：调 handler 得到 (status, rsp_body) → 组 Response（回填同一 `request_id`）→ 编码 → `conn->send`。
- 未命中：回 `kMethodNotFound` 的 Response。
- handler 内可 `co_await` DB（协程阶段）：那时 dispatch 变成启动一个协程，完成后再回包。

### 4.3 `rpc/rpc_client.{h,cpp}` —— 客户端（request_id 匹配的核心）

```cpp
class RpcClient {
public:
    using DoneCallback = std::function<void(RpcStatus, std::string rsp_body)>;

    RpcClient(EventLoop* loop, std::string host, uint16_t port);

    void connect();     // 复用 TcpClient；建立后挂 message_cb / close_cb

    // 异步调用：结果回来时在 loop 线程触发 done。
    void async_call(std::string method, std::string req_body,
                    DoneCallback done, int64_t timeout_ms = 3000);

    // 同步调用：内部 promise/future，调用线程阻塞等。禁止在 loop 线程自身调用（会死锁）。
    RpcStatus call(std::string method, std::string req_body,
                   std::string& rsp_body, int64_t timeout_ms = 3000);

private:
    struct PendingCall {
        DoneCallback done;
        EventLoop::TimerId timeout_timer;   // 用 TimerQueue 挂的超时定时器
    };

    void send_request_in_loop(uint64_t id, std::string method, std::string body,
                              DoneCallback done, int64_t timeout_ms);
    void on_message(const ConnectionPtr& conn, Buffer& input);   // 收响应 → 按 id 找 pending → 触发
    void on_timeout(uint64_t id);                                // 定时器到点：pending 未回 → kTimeout
    void fail_all_pending(RpcStatus st);                         // 连接断开时把所有挂起请求 fail 掉

    TcpClient client_;
    EventLoop* loop_;
    std::atomic<uint64_t> next_id_{1};                     // §1.4 request_id 生成
    std::unordered_map<uint64_t, PendingCall> pending_;    // 仅 loop 线程访问，无需锁
};
```

**为什么 `pending_` 只在 loop 线程访问**：发送经 `run_in_loop` 派进 loop 线程写表，
响应在 loop 线程 `on_message` 里读表，超时回调也在 loop 线程 —— 遵守
`.claude/rules/net.md` §1「不给状态加锁绕过线程归属」。

---

## 5. 调用链流向（跨线程是关键）

### 5.1 客户端一次 async_call

```
业务线程: rpc_client.async_call("EchoService.Echo", body, done)
  ├─ id = next_id_++                          // 原子自增
  └─ loop_->run_in_loop(send_request_in_loop) // 切到 loop 线程
        │  (以下在 loop 线程)
        ├─ pending_[id] = { done, timeout_timer = loop_->run_after(timeout_ms, on_timeout(id)) }
        ├─ RpcMessage{kRequest, id, method, body}
        ├─ LengthPrefixedCodec::encode(RpcCodec::encode(msg))
        └─ conn->send(frame)                   // 走现成半阻塞 send 路径

（网络往返……）

loop 线程 epoll_wait 就绪:
  Connection::handle_read → message_cb → RpcClient::on_message
        ├─ LengthPrefixedCodec::decode(input, frames)
        ├─ RpcCodec::decode(frame, msg)        // type=kResponse, 带 request_id
        ├─ it = pending_.find(msg.request_id)  // §1.4 响应路由
        ├─ loop_->cancel_timer(it->timeout_timer)   // 取消超时
        ├─ done(msg.status, msg.body)          // 交回业务（在 loop 线程）
        └─ pending_.erase(it)
```

### 5.2 同步 call

`call()` 内部 `async_call` + `std::promise`，业务线程 `future.get()` 阻塞；
`done` 里 `promise.set_value(...)` 唤醒。**注意：不能在 loop 线程自身调 `call()`（自我死锁），加 assert。**

### 5.3 超时 / 断连的收尾（务必处理，否则 pending 泄漏 + 业务永久卡住）

- **超时**：`on_timeout(id)` 在 loop 线程触发 → 若 `pending_` 还有该 id → 用 `kTimeout`
  调 `done` 并 erase。（响应比超时晚到时 `find` 落空，直接丢弃，安全。）
- **连接断开**：`close_cb` 里 `fail_all_pending(kServerError)` —— 遍历所有 pending，
  cancel 各自定时器、用错误码调 done、清表。否则同步 call 的 `future.get()` 会永久阻塞。

### 5.4 服务端一次 dispatch

```
subLoop epoll_wait → Connection::handle_read → message_cb → RpcServer::on_message
  ├─ LengthPrefixedCodec::decode → 逐帧 RpcCodec::decode（type=kRequest）
  └─ dispatch(conn, req)
        ├─ handlers_.find(req.method)          // §1.5 手写反射表
        ├─ 命中 → status = handler(req.body, rsp_body)
        │        未命中 → status = kMethodNotFound
        ├─ RpcMessage{kResponse, req.request_id, status, rsp_body}  // 回填同一 id
        └─ conn->send(LengthPrefixedCodec::encode(RpcCodec::encode(rsp)))
```

---

## 6. 难点与坑清单（实现时对照）

| # | 难点 | 处理 |
|---|------|------|
| 1 | 粘包/半包 | 复用 `LengthPrefixedCodec::decode`（已覆盖），RPC 层不重写 |
| 2 | 响应乱序匹配 | `request_id` + `pending_` 表（§1.4、§5.1） |
| 3 | 超时 | 每请求挂 `TimerQueue` 定时器，到点 fail（§5.3） |
| 4 | 连接断开挂起请求泄漏 | `close_cb` → `fail_all_pending`（§5.3），否则同步 call 永久阻塞 |
| 5 | 跨线程安全 | 发送/收响应/超时都在 loop 线程；`pending_` 无锁（§4.3） |
| 6 | 同步 call 死锁 | 禁止在 loop 线程自身调 `call()`，加 `assert(!loop_->in_loop_thread())` |
| 7 | Connection 生命周期 | 异步路径捕获 `ConnectionPtr` 拷贝延寿（rules/net §2） |
| 8 | 半关收尾 | 依赖 `Connection::shutdown()`（NEXT_TASKS Task 5），确保 outbuf 发完再关 |
| 9 | 无反射 | 手写 `map<方法名, handler>`；protobuf 阶段由生成代码填（§1.5） |
| 10 | 大 body | 外层 length 上限 `max_frame_len` 已防爆内存；RPC 大参数注意别超 16MiB 默认上限 |

---

## 7. 分阶段实现路线（按依赖顺序，逐卡片落地）

> 前置状态（2026-09-07 核对）：`docs/NEXT_TASKS.md` 的 **Task 5（Connection 半关 shutdown）
> 与 Task 6（force_close_with_delay）已完成**，RPC 无阻塞项，可直接从 RPC-1 开工。
> **可执行的逐卡片落地清单见 `docs/RPC_NEXT_TASKS.md`**（含已核对的真实接口签名）。

### RPC-1. `RpcCodec` + `RpcMessage`（最小骨架，最先做）
- **文件**：`rpc/rpc_message.h`（纯头文件）。
- **职责**：§3 的 header 编解码，套在 `LengthPrefixedCodec` 内层。
- **验收**：单测 —— 一个 `RpcMessage` encode 后 decode 回来字段一致；非法/截断 payload decode 返回 false。

### RPC-2. `RpcServer`：方法注册 + dispatch（单方法打通）
- **文件**：`rpc/rpc_server.{h,cpp}`。
- **职责**：包 `TcpServer`，`register_method` 建注册表，`on_message` 拆帧 → dispatch → 回包。
- **验收**：注册一个 `"Echo.Echo"`，handler 把 body 原样回；`kMethodNotFound` 路径也覆盖。

### RPC-3. `RpcClient`：async_call + request_id 匹配（不含超时）
- **文件**：`rpc/rpc_client.{h,cpp}`。
- **职责**：包 `TcpClient`，`next_id_` 自增，`pending_` 表，`on_message` 按 id 匹配触发 done。
- **验收**：连 RpcServer，连发 3 个请求，3 个 done 都按各自 body 正确回来（可乱序）。

### RPC-4. 超时 + 断连收尾
- **文件**：`rpc/rpc_client.{h,cpp}`（补 `on_timeout` / `fail_all_pending`）。
- **职责**：§5.3 —— 每请求挂 `run_after` 超时；`close_cb` fail 所有 pending。
- **验收**：① 让 server 故意不回，客户端 `timeout_ms` 后拿到 `kTimeout`；
  ② 请求发出后 kill server，客户端立即拿到 `kServerError`（不是永久阻塞）。

### RPC-5. 同步 `call()` + echo demo
- **文件**：`rpc/rpc_client.{h,cpp}`；`rpc/rpc_echo_demo.cpp`（client/server 各一个可执行）。
- **职责**：`call()` 用 promise/future 包 `async_call`；写一个端到端 echo demo。
- **验收**：`stub.call("Echo.Echo", "hello", out)` 拿回 `"hello"`；在 loop 线程调 call 触发 assert。

### RPC-6. protobuf 序列化（替换 body 层，可选但推荐）
- **文件**：`rpc/proto/*.proto` + 生成代码；`rpc/rpc_client`/`rpc_server` 接 `Message` 泛型接口。
- **职责**：body 从裸串换成 `SerializeToString/ParseFromArray`；方法名可从 `service.method` 生成。
- **要点**：CMake `find_package(Protobuf)` + `protobuf_generate`；先确认环境可得（唯一新外部依赖）。
- **验收**：`Echo{string text}` proto，client 发 `EchoReq` → server 回 `EchoRsp`，跨进程一致。

### RPC-7. 协程 stub（叠加在协程阶段之上）
- **前置**：`docs/NEXT_TASKS.md` Task 8（协程层 `Task<T>` + awaiter）完成。
- **文件**：`rpc/rpc_client` 加 `Task<RpcResult> co_call(...)`。
- **职责**：把 `async_call` 的 done 换成"resume 协程"：`pending_[id]` 存 `coroutine_handle`，
  `on_message` 里 resume（在 loop 线程）。业务写成 `auto rsp = co_await stub.co_call(...)`。
- **验收**：协程版 echo 与同步版行为一致；`co_await` 期间不阻塞 loop 线程。

### RPC-8. 服务注册与发现（最后，需要时再做）
- **文件**：`rpc/registry/*`。
- **职责**：Server 启动向注册中心注册 `服务名 → ip:port`；Client 订阅拿地址列表；心跳/租约摘除。
- **选型**：etcd / ZooKeeper / Consul。**在此之前一直用直连**（配置文件写 host:port）。
- **验收**：Server 上线 Client 能发现并调用；Server 下线后 Client 自动摘除该地址。

---

## 8. 与现有代码的接合点（落地时直接用）

| 需求 | 直接复用 |
|------|----------|
| 传输 / 收发 | `TcpServer` / `TcpClient` / `Connection::send` |
| 帧边界（粘包半包） | `LengthPrefixedCodec::encode/decode` |
| 跨线程把请求切进 loop | `EventLoop::run_in_loop` / `queue_in_loop` |
| 超时 | `EventLoop::run_after` / `cancel_timer`（底层 `TimerQueue`） |
| 每连接挂会话/解析状态 | `Connection::set_context<T>()` |
| 连接建立/断开感知 | `set_connection_callback` / `set_close_callback`（`conn->connected()` 区分） |
| 优雅关（响应发完再关） | `Connection::shutdown()`（NEXT_TASKS Task 5） |
| 背压 | `set_high_water_mark_callback` / `set_write_complete_callback`（大批量调用时用） |

---

## 9. 一句话总结

**RPC = 你现有的 TCP 传输 + LengthPrefixedCodec 帧 + 一个内层 header（type/request_id/method/status）
+ 客户端 `map<request_id, pending>` 做响应路由 + 服务端 `map<方法名, handler>` 补 C++ 无反射 +
TimerQueue 做超时。** 先用最朴素的序列化把这套骨架跑通，再叠加 protobuf、协程、注册中心。
