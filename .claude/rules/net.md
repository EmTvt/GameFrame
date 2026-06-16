# Rule: Networking Code

> 改 `src/` 下网络库（EventLoop / Channel / Connection / TcpServer / TcpClient / Buffer / TimerQueue）时**必须**遵守。

## 1. 线程归属（最重要）

- 每个 `EventLoop` 绑定到构造它的线程（`owner_tid_`）
- 所有写 `EventLoop` / `Channel` / `Connection` 的字段、调用其非 `const` 方法的操作，**必须在它归属的 loop 线程**做
- 跨线程调用统一走 `loop->run_in_loop(f)`：当前是 loop 线程就直接调，否则 `queue_in_loop` + `wakeup`
- 不确定时调 `assert_in_loop_thread()` 自检
- **不要**给某个状态加 mutex 来"绕过"这条规则——这会破坏整个模型的简单性

## 2. 生命周期

- `Connection` 一律用 `ConnectionPtr = std::shared_ptr<Connection>` 传递
- 禁止裸 `Connection&` / `Connection*` 跨函数边界
- 异步回调要持有 conn 的，**必须捕获 `ConnectionPtr` 拷贝**让 lambda 延寿
- 定时器、外部 session 表等"外部资源"持 conn 一律用 `weak_ptr<Connection>`，避免拖尾析构
- Channel 在事件派发期的生命周期靠 `tie()` 锁定（防 close 中途自我析构）
- **关闭顺序**：先 `Channel::disable_all` + 从 EventLoop 移除，再走业务关闭回调，最后才释放 `ConnectionPtr`

## 3. Channel 不持有 fd

- Channel 只代表"对一个 fd 在 epoll 上的事件兴趣 + 回调"
- fd close 由真正的资源持有者负责（Connection / TcpServer / TcpClient connecting 阶段）
- `Channel::tie` 用 `weak_ptr<void>`，不要绑 `weak_ptr<Connection>`（要让 Acceptor / TcpClient 也能复用）

## 4. 回调风格（保持 muduo 一致）

- `ConnectionCallback(conn)` 一个回调覆盖建立/断开两种语义，业务用 `conn->connected()` 区分
- `MessageCallback(conn, Buffer& input)`；拆帧由业务做，Buffer 给的是 readable 视图
- **不要**在同步回调里直接调可能再次触发回调的方法（避免状态机自递归）
- `HighWaterMarkCallback` / `WriteCompleteCallback` **必须** `queue_in_loop` 异步触发，**不能**同步派
- 业务对外接口尽量与 TcpServer 风格对称（TcpClient 的 `set_connection_callback` / `set_message_callback` 已经这样做）

## 5. send 路径必须保证

- `Connection::send` 可从任意线程调用，内部走 `run_in_loop(send_in_loop)`
- 优先尝试直接 `write(fd)`；写不完才 append 到 `output_buffer_` + 注册 EPOLLOUT
- `output_buffer_` 越过 `high_water_mark` 时**异步**派 `HighWaterMarkCallback`
- output_buffer 排空后**异步**派 `WriteCompleteCallback`
- 处理 `EAGAIN/EWOULDBLOCK`：不要当错误，转入 EPOLLOUT 等待下次写

## 6. read 路径必须保证

- `handle_read` 用 `readv`（input_buffer_ 内部 + 栈上备份）
- `n > 0` → 派 `message_cb`
- `n == 0`（EOF）→ **先派一次 `message_cb`**（防丢最后一帧），再 `handle_close()`
- `n < 0`：`EAGAIN/EWOULDBLOCK/EINTR` 忽略，其它走错误路径

## 7. 不允许做的事

- ❌ 在 `src/` 里 `#include "log_server/..."` ——单向依赖
- ❌ 给 `TcpServer` 加业务字段（解析器、session 表）——挂在 `Connection::context_` 上
- ❌ 把 `EventLoop::loop()` 改成可重入——`quit_` 是单标志
- ❌ `epoll_wait` 加 `SA_RESTART`——信号要能立刻打断
- ❌ 用 `select`/`poll` 替换 `epoll`——这个项目就是要练 epoll
- ❌ 偷偷把内部容器改成 `std::vector` 之外的（除非在 DECISIONS.md 写清动机）

## 8. 风格

- 头文件**开头一段中文注释**讲：这个类为什么存在 / 演进到这一步要解决什么问题
- 注释解释**动机**（为什么这么做），不描述代码（"这里调用 xxx"）
- 临时调试输出统一用 `[DIAG ...]` 前缀，方便事后 `grep -n` 一把清掉
- 公开类型别名集中在 `connection.h` / 各类 owner 头文件顶部

## 9. 改完之后必须做

- 编译过：`cmake --build build -j`
- `./build/test_event_loop` 通过
- 如果改了 connect/accept/close 路径：跑 `./build/test_tcp_client` 通过
- 如果改了 EventLoop / Channel / Connection 任一核心组件：在回答里**用文字说明改后的调用链流向**（哪些函数被加进 / 移出热点路径），便于 review
