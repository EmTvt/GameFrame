# Rule: Testing

> 加 / 改任何测试相关代码（`test_*.cpp`、Python 客户端、CMake test target）时**必须**遵守。

## 1. 当前测试矩阵

| 名字 | 入口 | 跑啥 |
|---|---|---|
| `test_event_loop`  | `test_event_loop.cpp`  | 跨线程 `run_in_loop` 投递任务能落到 loop 线程执行 |
| `test_tcp_client`  | `test_tcp_client.cpp`  | TcpClient 连 echo server → server 主动 close → 自动重连 → 再次 echo |
| `smoke_client.py`  | `log_server/smoke_client.py` | 给 log_server 发一条 / 多条 length-prefixed 消息 |
| `roll_stress.py`   | `log_server/roll_stress.py`  | 给 log_server 灌 >10 MiB 验证文件滚动 |

## 2. 加新测试用什么形式

按这个顺序选：

### a) C++ 可执行（推荐用于网络库内部行为）
- 文件放在仓库根目录：`test_<feature>.cpp`
- 在 `CMakeLists.txt` 里仿照 `test_event_loop` / `test_tcp_client` 加一个 `add_executable`
- 链接需要的 `src/*.cpp` 和 `Threads::Threads`
- **必须有超时退出**（参考 `test_tcp_client.cpp` 的 5 秒 deadline + `std::exit(1)`），CI 不能挂死
- **必须有明确的 PASS / FAIL 输出**（stderr 打 `✅ PASSED` / `❌ FAILED` + 非 0 退出码）

### b) Python smoke / 压测脚本（推荐用于 log_server 协议层）
- 放在 `log_server/` 下
- **不引入第三方库**，只用标准库（socket / struct / argparse）
- 命令行参数明确（端口、总量、并发等），跑出来要肉眼能判断成败

### c) 暂不引入测试框架
- 不引入 GoogleTest / Catch2 / doctest——当前规模手写就够用
- 真要引入，先在 DECISIONS.md 立项说明动机

## 3. 测试代码必须做的事

- **超时保护**：所有 C++ 测试都要有 deadline；不能"等连接进来"无限等
- **可重复运行**：测完资源都要释放（关 socket / quit loop / detach 线程或 join），不能两次跑互相干扰
- **不依赖外部环境**：不读环境变量、不连外网、不 hardcode 实际机器的 IP
- **端口不冲突**：每个测试用不同端口（test_tcp_client 9876 / log_server 默认 9099 / 新测试请避开这两个）
- **失败时 stderr 打清楚**：哪一步挂了、期望啥实际啥，不要只打 "FAILED"

## 4. 不允许做的事

- ❌ 用 `telnet` 测 length-prefixed 协议（会注入 IAC 协商字节，详见 docs/DEBUGGING.md）
- ❌ 用 `sleep(60)` 之类长 sleep 凑时序——用条件变量 / atomic flag + spin
- ❌ 在测试里捕获信号，让 ctrl-C 都按不掉
- ❌ 改了网络库核心却**只**改 `test_event_loop`：核心改动至少要有一条**集成路径**测试覆盖
- ❌ 把测试代码塞进 `src/`——测试入口都在仓库根目录或 `log_server/`

## 5. 改了哪些代码 → 必须跑哪些测试

| 改动范围 | 必跑 |
|---|---|
| Buffer | （目前没单元测试，至少手写个临时 `test_buffer.cpp` 或 smoke 验证）|
| EventLoop / Channel / TimerQueue | `./build/test_event_loop` |
| Connection 的 send / read / close 路径 | `./build/test_tcp_client`（覆盖了 send/echo/close/reconnect） |
| TcpServer | `./build/test_tcp_client` + `./build/epoll_proj` 手测 echo |
| TcpClient | `./build/test_tcp_client` |
| log_server / LogFile | `roll_stress.py` 跑滚动 + 启动续写场景手测 |
| 未来 LogSender / MPSCQueue | （测试待补；引入时同步加 `test_log_sender.cpp` / Python smoke） |

## 6. CI（占位）

目前没接 CI。引入 CI 时（GitHub Actions / 自建）：
- 构建：`cmake -B build && cmake --build build -j`
- 跑：依次执行上面表里所有 C++ 测试 + Python smoke
- 全部退出码 0 才算通过
- 决策记到 DECISIONS.md
