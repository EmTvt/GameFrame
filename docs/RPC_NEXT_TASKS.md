# RPC 实现步骤（Next Tasks）

> 落地清单：在现有 epoll_proj 网络库之上，逐卡片实现 RPC 框架。
> **设计与动机看 `docs/RPC_DESIGN.md`（为什么这么做）；本文件只讲"下一步做什么、怎么验收"**。
> 风格与 `docs/NEXT_TASKS.md` 一致：每张卡给出 文件 / 职责 / 接口 / 要点 / 验收，实现时不必再猜。

Last updated: 2026-09-07

---

## 前置检查（已全部就绪，无阻塞）

RPC 依赖的底层能力已核对，签名如下（落地时直接用，别再翻源码）：

| 能力 | 现成接口 | 位置 |
|------|----------|------|
| 服务端传输 | `TcpServer(uint16_t port, size_t num_threads = 0)`；`set_message_callback` / `set_connection_callback`；`run()`；`main_loop()` | `src/server.h` |
| 客户端传输 | `TcpClient(EventLoop* loop, std::string host, uint16_t port)`；`connect()` / `disconnect()`；`set_message_callback` / `set_connection_callback`；`connection()` | `src/tcp_client.h` |
| 外层帧 | `LengthPrefixedCodec::encode(string_view) -> string`；`decode(Buffer&, vector<string>& out, max_frame_len) -> bool` | `util/length_prefixed_codec.h` |
| 收发 | `Connection::send(string_view)`；`shutdown()`（优雅半关，已实现）；`force_close_with_delay(ms)`（已实现） | `src/connection.h` |
| 每连接状态 | `Connection::set_context<T>(shared_ptr<T>)` / `context<T>()` | `src/connection.h` |
| 连接建立/断开感知 | 同一个 `connection_callback`，靠 `conn->connected()` 区分 | `src/server.h` / `src/tcp_client.h` |
| 超时 | `EventLoop::run_after(int64_t delay_ms, TimerCallback) -> TimerId`；`cancel_timer(TimerId)`（任意线程安全） | `src/event_loop.h` |
| 跨线程投递 | `EventLoop::run_in_loop(Functor)` / `queue_in_loop(Functor)`；`in_loop_thread()` / `assert_in_loop_thread()` | `src/event_loop.h` |

> `EventLoop::TimerId` 实际类型 = `uint64_t`。`PendingCall` 里存它、`cancel_timer` 取消，签名完全对得上。
> `docs/RPC_DESIGN.md` §7 里"先做 NEXT_TASKS Task 5（半关 shutdown）"的前置提醒**已过时**——Task 5/6 都已完成，RPC 可直接开工。

---

## 目录与依赖方向

新增 `rpc/` 目录，依赖方向 `rpc/ → src/ → util/`，**禁止把 RPC 代码塞回 `src/`**（与 log_sender 同档）。

```
rpc/
  rpc_message.h          # RPC-1：RpcMessage + RpcCodec（纯头文件，无状态）
  rpc_server.{h,cpp}     # RPC-2：包 TcpServer，注册表 + dispatch
  rpc_client.{h,cpp}     # RPC-3/4/5：包 TcpClient，pending 表 + 超时 + 同步 call
  rpc_echo_demo.cpp      # RPC-5：端到端 echo（client/server 两个可执行或一个带参）
```

---

## RPC-1. `rpc/rpc_message.h` — RpcMessage + RpcCodec ✅ Done (2026-09-07)

> 实现要点（与草案的出入，以此为准）：
> - 采用**定长头部 15B**（type 1 + request_id 8 + status 4 + method_len 2），method/body 变长紧随其后。
>   比"按 type 决定字段在不在"更对称好测；response 里 method_len=0，request 里 status 写 kOk。
> - `RpcCodec` 无状态、`struct` + 全 `static` 函数（**无实例、无单例**），形态对齐 `LengthPrefixedCodec`。
> - `decode` 每步校验剩余长度：短于头 / method_len 超出剩余 / type>kResponse → 返回 false。
> - 测试 `test_rpc_codec.cpp`（自包含，返回码即结论）：round-trip（含空 body / 二进制 body）+ 截断包拒收
>   + 非法 type + 与 LengthPrefixedCodec 两层叠加连续拆帧。`./build/test_rpc_codec` 通过。


**文件**：`rpc/rpc_message.h`（纯头文件，无状态，仿 `LengthPrefixedCodec` 的写法）。

**职责**：定义 RPC 内层协议，套在 `LengthPrefixedCodec` 内层。内层 payload 布局（`docs/RPC_DESIGN.md` §3）：

```
uint8   type        // 0 = Request, 1 = Response
uint64  request_id  // 大端；客户端自增，服务端原样回填
uint32  status      // 仅 Response：0 = kOk，非 0 = 错误码
uint16  method_len  // 仅 Request：方法名长度
bytes   method      // 仅 Request：方法名，如 "EchoService.Echo"
bytes   body        // 剩余全部：序列化后的参数/返回值
```

**接口草案**：
```cpp
namespace epoll_proj {

enum class RpcType : uint8_t { kRequest = 0, kResponse = 1 };

enum class RpcStatus : uint32_t {
    kOk = 0, kMethodNotFound, kDeserializeError, kServerError, kTimeout,
};

struct RpcMessage {
    RpcType     type;
    uint64_t    request_id;
    RpcStatus   status;      // 仅 response 有意义
    std::string method;      // 仅 request，形如 "EchoService.Echo"
    std::string body;        // 序列化后的参数/返回值（第一版可裸串/JSON）
};

struct RpcCodec {
    static std::string encode(const RpcMessage& msg);                     // -> 内层 payload
    static bool        decode(std::string_view payload, RpcMessage& out); // 非法/截断返回 false
};

}  // namespace epoll_proj
```

**要点**：
- 字节序统一大端，手写 shift（与 `LengthPrefixedCodec::peek_uint32_be` 一致，不引 htonl）。
- `decode` 要防越界：每读一个字段前校验剩余长度够不够，任何一步不够 → 返回 false（截断包安全丢弃）。
- 发送侧最终形态：`conn->send(LengthPrefixedCodec::encode(RpcCodec::encode(msg)))`。
- 接收侧：`LengthPrefixedCodec::decode(buf, frames)` 拿一批 payload，逐个 `RpcCodec::decode`。

**验收**：新增 `test_rpc_codec.cpp`——
- Request/Response 各造一个 `RpcMessage`，`encode` 后 `decode` 回来所有字段一致；
- 截断 payload（如只给前 3 字节、method_len 声称 100 实际只有 2 字节）`decode` 返回 false；
- 返回码即结论（与 `test_logging` 风格一致）。

---

## RPC-2. `rpc/rpc_server.{h,cpp}` — 方法注册 + dispatch ✅ Done (2026-09-11)

> 实现要点（与草案的出入，以此为准）：
> - **Handler 签名改为 `int(const std::string& req_body, std::string& rsp_body)`**（按需求：返回 int 错误码，
>   0=成功；入参 req + 出参 &res）。返回的 int 原样填进响应 status 字段透传给客户端。
> - 框架层错误（方法未注册）**不调 handler**，直接回 `RpcStatus::kMethodNotFound`；handler 只在命中时跑，
>   其返回码是业务层结果。两者共用 status 字段，约定 0=kOk。
> - `on_message`：外层 `LengthPrefixedCodec::decode` 循环拆多帧 → 逐帧 `RpcCodec::decode`；
>   外层/内层任一非法、或收到非 Request → `conn->close()`。
> - `send_response` 回填同一 request_id → 内层编码 → 外层加长度 → `conn->send`。
> - 暴露 `main_loop()` 供业务挂信号 / 测试 quit。
> - 测试 `test_rpc_server.cpp`（自包含，裸阻塞 socket 当客户端，返回码即结论）：命中 Echo.Echo +
>   业务错误码 Biz.Fail 返回 7 + 未注册 kMethodNotFound + 单连接连发 3 请求按序回。`./build/test_rpc_server` 通过。


**文件**：`rpc/rpc_server.{h,cpp}`。

**职责**：包 `TcpServer`；`register_method` 建"方法名 → handler"注册表（C++ 无反射的手写替代，见 `RPC_DESIGN.md` §1.5）；`on_message` 拆帧 → 查表 → dispatch → 回包。

**接口草案**：
```cpp
class RpcServer {
public:
    // handler 拿 request body，产出 (status, response body)。
    using Handler = std::function<RpcStatus(std::string_view req_body, std::string& rsp_body)>;

    RpcServer(uint16_t port, size_t num_threads = 0);

    // 必须在 run() 前注册完（注册表 run 后只读，无锁）。method 形如 "EchoService.Echo"。
    void register_method(std::string method, Handler h);
    void run();

private:
    void on_message(const ConnectionPtr& conn, Buffer& input);
    void dispatch(const ConnectionPtr& conn, const RpcMessage& req);

    TcpServer server_;
    std::unordered_map<std::string, Handler> handlers_;
};
```

**dispatch 流**：
- 拆帧 → `RpcCodec::decode`（type 应为 kRequest）→ `handlers_.find(req.method)`
- 命中：`status = handler(req.body, rsp_body)`
- 未命中：`status = kMethodNotFound`
- 组 `RpcMessage{kResponse, req.request_id, status, "", rsp_body}`（**回填同一 request_id**）→ 编码 → `conn->send`
- `RpcCodec::decode` 返回 false（非法帧）→ `conn->close()`（buffer 已对不上边界）

**要点**：
- `on_message` 里 `LengthPrefixedCodec::decode` 一次可能拆出多帧，**循环处理每一帧**。
- 多线程（`num_threads > 0`）时 dispatch 在各 subLoop 线程执行——`handlers_` run 后只读，安全无锁；handler 内如需共享可变状态由业务自己保证。

**验收**：注册 `"Echo.Echo"`（handler 把 body 原样写回 rsp_body 并返 kOk）；用 nc/小客户端或 RPC-3 完成后联调，命中路径与 `kMethodNotFound` 路径都覆盖。

**改完在回复里说明调用链流向**（CLAUDE.md 要求）。

---

## RPC-3. `rpc/rpc_client.{h,cpp}` — async_call + request_id 匹配 ⬜（核心难点）

**文件**：`rpc/rpc_client.{h,cpp}`（本卡先不做超时/断连，留给 RPC-4）。

**职责**：包 `TcpClient`；`next_id_` 自增生成 request_id；`pending_` 表存 "id → 回调"；`on_message` 收响应后按 id 匹配触发 done（响应可乱序回来，这就是 request_id 存在的意义，见 `RPC_DESIGN.md` §1.4）。

**接口草案**：
```cpp
class RpcClient {
public:
    using DoneCallback = std::function<void(RpcStatus, std::string rsp_body)>;

    RpcClient(EventLoop* loop, std::string host, uint16_t port);
    void connect();   // 复用 TcpClient；建立后挂 message_cb / close_cb

    void async_call(std::string method, std::string req_body,
                    DoneCallback done, int64_t timeout_ms = 3000);

private:
    struct PendingCall {
        DoneCallback         done;
        EventLoop::TimerId   timeout_timer = 0;   // RPC-4 才真正用上
    };

    void send_request_in_loop(uint64_t id, std::string method,
                              std::string body, DoneCallback done, int64_t timeout_ms);
    void on_message(const ConnectionPtr& conn, Buffer& input);

    TcpClient  client_;
    EventLoop* loop_;
    std::atomic<uint64_t>                       next_id_{1};
    std::unordered_map<uint64_t, PendingCall>   pending_;   // 仅 loop 线程访问，无锁
};
```

**调用链流向（务必守住线程归属）**：
```
业务线程: async_call(...)
  ├─ id = next_id_++                                  // 原子自增（唯一在业务线程做的写）
  └─ loop_->run_in_loop(send_request_in_loop)         // 切进 loop 线程
        │ (以下全在 loop 线程)
        ├─ pending_[id] = { done, /*timer 见 RPC-4*/ }
        ├─ RpcMessage{kRequest, id, method, body}
        └─ conn->send(LengthPrefixedCodec::encode(RpcCodec::encode(msg)))

loop 线程 epoll 就绪 → on_message:
  ├─ LengthPrefixedCodec::decode → 逐帧 RpcCodec::decode (type=kResponse)
  ├─ it = pending_.find(msg.request_id)               // 响应路由
  ├─ done(msg.status, msg.body)                        // 交回业务（在 loop 线程）
  └─ pending_.erase(it)
```

**要点**：
- `pending_` **只在 loop 线程读写**（发送经 `run_in_loop` 进 loop 线程写表，收响应在 loop 线程读表）→ 不加锁（`.claude/rules/net.md` §1：不给状态加锁绕过线程归属）。
- `send_request_in_loop` 要处理"当前未连上"：`client_.connection()` 为空 → 直接 `done(kServerError, "")` 或延后（第一版先简单 fail）。
- 连接建立时（connection_callback）把 message_cb 挂到 conn 上；`on_message` 从 `conn` 拿 `input_buffer` 拆帧。

**验收**：连上 RPC-2 的 server，连发 3 个不同 body 的请求，3 个 done 各自按正确 body 回来（允许乱序，用 request_id 对上即可）。

**改完在回复里说明调用链流向。**

---

## RPC-4. 超时 + 断连收尾 ⬜（别跳，否则同步 call 会永久阻塞）

**文件**：`rpc/rpc_client.{h,cpp}`（补 `on_timeout` / `fail_all_pending`）。

**职责**：`RPC_DESIGN.md` §5.3——
- **超时**：`send_request_in_loop` 里 `timeout_timer = loop_->run_after(timeout_ms, [this,id]{ on_timeout(id); })`；`on_message` 匹配成功后 `loop_->cancel_timer(it->timeout_timer)` 再 erase。
- `on_timeout(id)`：在 loop 线程，若 `pending_` 仍有该 id → 用 `kTimeout` 调 done 并 erase。（响应比超时晚到 → `find` 落空直接丢弃，安全。）
- **断连**：connect 时 `client_.set_connection_callback`，在 `conn->connected()==false` 分支调 `fail_all_pending(kServerError)`——遍历所有 pending，cancel 各自 timer、用错误码调 done、清表。

**要点**：
- 超时回调、断连回调、on_message 全在 loop 线程 → `pending_` 依旧无锁。
- 顺序坑：匹配成功要**先 cancel_timer 再 erase**，避免定时器回调拿到已 erase 的迭代器（其实 on_timeout 里会重新 find，也安全，但 cancel 更省一次无效唤醒）。

**验收**：
- ① server 故意不回某方法（handler 里 sleep 或干脆不注册对应回包）→ 客户端 `timeout_ms` 后拿到 `kTimeout`；
- ② 请求发出后 kill server → 客户端**立即**拿到 `kServerError`（不是等到内核 TCP 超时，也不是永久阻塞）。

---

## RPC-5. 同步 `call()` + 端到端 echo demo ⬜

**文件**：`rpc/rpc_client.{h,cpp}`（加 `call()`）；`rpc/rpc_echo_demo.cpp`。

**职责**：
- `call()` 内部用 `std::promise/future` 包 `async_call`，业务线程 `future.get()` 阻塞等结果，`done` 里 `promise.set_value(...)` 唤醒。
- 写一个端到端 echo demo：server 注册 `"Echo.Echo"`，client `call("Echo.Echo", "hello", out)` 拿回 `"hello"`。

**接口草案**：
```cpp
RpcStatus call(std::string method, std::string req_body,
               std::string& rsp_body, int64_t timeout_ms = 3000);
```

**要点**：
- **禁止在 loop 线程自身调 `call()`**（会自我死锁：future.get() 卡住 loop，done 永远跑不到）→ 首行 `assert(!loop_->in_loop_thread())`。
- demo 里 client 的 EventLoop 通常跑在独立线程（`EventLoopThread`），业务主线程调 `call()` 阻塞等——与 loop 线程分开。

**验收**：`stub.call("Echo.Echo","hello",out)` 返回 kOk 且 `out=="hello"`；在 loop 线程里调 `call()` 触发 assert（验证防死锁）。

---

## 到此为止：最小可用 RPC 框架 ✅

RPC-1~5 完成后，你有一个能跑的最小 RPC：TCP 传输 + 帧编解码 + request_id 响应路由 + 手写方法注册表 + 超时/断连收尾 + 同步/异步两种调用。**面试讲解建议同步更新 `docs/INTERVIEW.md`。**

---

## 叠加项（按需再做，无先后强依赖，除 RPC-7 需协程层）

### RPC-6. protobuf 序列化（推荐）⬜
- **文件**：`rpc/proto/*.proto` + 生成代码；client/server 接 `Message` 泛型接口。
- **职责**：body 从裸串换成 `SerializeToString` / `ParseFromArray`；方法名可从 `service.method` 生成。
- **要点**：CMake `find_package(Protobuf)` + `protobuf_generate`——**先确认环境装了 protobuf**（唯一新外部依赖）。
- **验收**：`Echo{string text}` proto，client 发 `EchoReq` → server 回 `EchoRsp`，跨进程字段一致。

### RPC-7. 协程 stub ⬜（前置：`docs/NEXT_TASKS.md` Task 8 协程层）
- **文件**：`rpc/rpc_client` 加 `Task<RpcResult> co_call(...)`。
- **职责**：把 `async_call` 的 done 换成"resume 协程"——`pending_[id]` 存 `coroutine_handle`，`on_message` 里 resume（在 loop 线程）。业务写成 `auto rsp = co_await stub.co_call(...)`。
- **验收**：协程版 echo 与同步版行为一致；`co_await` 期间不阻塞 loop 线程。

### RPC-8. 服务注册与发现 ⬜（最后，需要时再做）
- **文件**：`rpc/registry/*`。
- **职责**：Server 启动注册 `服务名 → ip:port`；Client 订阅拿地址列表；心跳/租约摘除。**在此之前一直直连**（配置写 host:port）。
- **选型**：etcd / ZooKeeper / Consul。
- **验收**：Server 上线 Client 能发现并调用；Server 下线后 Client 自动摘除该地址。

---

## 难点对照（实现时回查 `docs/RPC_DESIGN.md` §6）

| # | 难点 | 处理 | 卡片 |
|---|------|------|------|
| 1 | 粘包/半包 | 复用 `LengthPrefixedCodec::decode` | RPC-1/2/3 |
| 2 | 响应乱序匹配 | `request_id` + `pending_` 表 | RPC-3 |
| 3 | 超时 | 每请求挂 `run_after`，到点 fail | RPC-4 |
| 4 | 断连挂起请求泄漏 | `close`/断连回调 → `fail_all_pending` | RPC-4 |
| 5 | 跨线程安全 | 发送/收响应/超时都在 loop 线程，`pending_` 无锁 | RPC-3/4 |
| 6 | 同步 call 死锁 | `assert(!loop_->in_loop_thread())` | RPC-5 |
| 7 | Connection 生命周期 | 异步路径捕获 `ConnectionPtr` 拷贝延寿 | RPC-3/4 |
| 8 | 无反射 | 手写 `map<方法名, handler>` | RPC-2 |
| 9 | 大 body | 外层 `max_frame_len` 已防爆（默认 16 MiB） | RPC-1 |

---

## CMake / 构建接入（每加一个可执行时）

- 新增 `rpc/` 源码到对应 target；`test_rpc_codec`、`rpc_echo_demo` 各建一个可执行。
- 命令沿用 `CLAUDE.md`：`cmake -B build && cmake --build build -j`，然后 `./build/test_rpc_codec` 等。
