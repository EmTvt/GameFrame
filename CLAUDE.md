# CLAUDE.md

> 这是为 Claude / AI 协作者准备的项目入口文档（routing index）。
> 当 AI 进入本项目时，**先读本文件**，再根据任务类型按下方索引跳转到 `CLAUDE/` 目录下的具体文件。

---

## 1. 项目速览

- **项目名**：`epoll_proj`
- **定位**：从零搭建一个 **教学性质的 Linux epoll TCP 框架**，对标 muduo 的简化版
- **当前阶段**：第二步完成 —— 已具备读/写缓冲区、按协议解析的基础能力
- **运行模式**：单线程 + epoll(ET) + 非阻塞 I/O
- **构建**：CMake + C++20
- **入口文件**：`main.cpp`（演示一个 echo 服务）

### 一句话能力

> 能同时服务多个 TCP 客户端，业务侧通过注册回调 `void(Connection&, Buffer&)` 决定如何按协议拆消息、如何回复。框架负责连接管理、事件分发、读写缓冲。

---

## 2. 项目结构

```
epoll_proj/
├── CMakeLists.txt              # C++20 + Debug 默认 + compile_commands.json
├── main.cpp                    # 入口：演示 echo 服务的注册写法
├── src/
│   ├── buffer.h                # 字节缓冲区（reader/writer 双游标）
│   ├── connection.h / .cpp     # 单连接抽象：handle_read / handle_write / send / close
│   └── server.h / .cpp         # TcpServer：listen + epoll_wait 主循环 + 连接表
├── build/                      # CMake 构建目录（被 .gitignore 排除）
├── CLAUDE.md                   # 本文件（路由入口）
└── CLAUDE/                     # AI 上下文知识库
    ├── SKILLS/                 # 可执行的工作流（怎么做事）
    ├── RULES/                  # 必须遵守的规则（不能怎么做）
    └── MCP/                    # 外部 MCP / 工具集成说明
```

---

## 3. 当前已完成的能力（第二步）

| 模块 | 已实现 |
|---|---|
| `Buffer` | `append` / `peek` / `retrieve` / `ensure_writable`（含数据前移压缩 + 扩容） |
| `Connection` | 读循环到 EAGAIN；写快路径 + EPOLLOUT 兜底；`send` 自动判断走快路径还是入 buffer；`close` 用 `state_` 标记，`fd_` 保留给回调使用 |
| `TcpServer` | `epoll_create1` + `EPOLLET`；`accept` 循环；`update_events` / `remove_connection` 给 Connection 反向调用 |
| 业务接入 | `set_message_callback` 注册一个回调函数即可，签名 `void(Connection&, Buffer&)` |

### 还**没有**做的（关键短板，下一步候选）

- ❌ 写完成回调（`WriteCompleteCallback`）
- ❌ 高/低水位线机制（防慢消费者撑爆内存）
- ❌ Connection 上下文挂载（`set_context` —— HTTP 解析器要它）
- ❌ 定时器（空闲连接踢出 / Keep-Alive 超时）
- ❌ 多线程 EventLoop（当前单线程，单核性能上限）
- ❌ `shared_ptr<Connection>` + `enable_shared_from_this`（异步处理时连接生命周期保护）

---

## 4. 路由索引（按任务类型查阅）

> **使用方式**：根据当前要做的事，跳到下面对应的子文件继续阅读。

### 4.1 SKILLS —— 我要做某件事

| 任务 | 跳转文件 |
|---|---|
| 编译 / 运行 / 调试这个项目 | [`CLAUDE/SKILLS/build-and-run.md`](CLAUDE/SKILLS/build-and-run.md) |
| 给项目加一个新协议（HTTP / 长度前缀 / 等） | [`CLAUDE/SKILLS/add-protocol.md`](CLAUDE/SKILLS/add-protocol.md) |
| 给框架加一个新能力（如水位线、定时器） | [`CLAUDE/SKILLS/extend-framework.md`](CLAUDE/SKILLS/extend-framework.md) |
| 排查"连接卡住 / 数据丢失 / 内存涨"等运行时问题 | [`CLAUDE/SKILLS/debug-runtime.md`](CLAUDE/SKILLS/debug-runtime.md) |

### 4.2 RULES —— 我必须遵守什么

| 主题 | 跳转文件 |
|---|---|
| 代码风格、命名、注释规范 | [`CLAUDE/RULES/coding-style.md`](CLAUDE/RULES/coding-style.md) |
| 资源管理（fd、内存、生命周期）的硬约束 | [`CLAUDE/RULES/resource-safety.md`](CLAUDE/RULES/resource-safety.md) |
| 修改回调 / 接口前必须想清楚的事 | [`CLAUDE/RULES/api-stability.md`](CLAUDE/RULES/api-stability.md) |

### 4.3 MCP —— 外部工具 / 集成

| 内容 | 跳转文件 |
|---|---|
| 当前可用的 MCP 服务器与典型用法 | [`CLAUDE/MCP/available-servers.md`](CLAUDE/MCP/available-servers.md) |

---

## 5. 给 AI 协作者的 5 条核心提示

1. **本项目是教学项目**：清晰 > 性能 > 行数。代码注释要解释"为什么这么做"，不只是"做了什么"。
2. **不要引入大依赖**：除标准库外，仅允许使用 Linux 系统调用（`<sys/epoll.h>` / `<unistd.h>` 等）。**禁止引入 Boost、muduo、asio 等**。
3. **改动前先读相关 RULES**：尤其是 `resource-safety.md`（fd 的 close 时机、Connection 析构语义有非常具体的约定）。
4. **优先使用 `replace_in_file` 做局部修改**：除非用户明确要求重写，否则不要大段改写已有文件。
5. **每一步可独立运行**：本项目按"步"演进（第一步、第二步……），每一步结束都应能 `cmake --build build && ./build/epoll_proj` 跑起来。

---

## 6. 演进路线（已规划）

- ✅ **第一步**：能 accept 多个连接、能 echo（无缓冲区，read 直接转回调）
- ✅ **第二步**：引入 Buffer，回调签名改为 `(Connection&, Buffer&)`，引入 EPOLLOUT 写缓冲（**当前所在阶段**）
- ⬜ **第三步**：水位线 + WriteCompleteCallback（应对慢消费者）
- ⬜ **第四步**：定时器（timerfd + 时间轮 / 最小堆）
- ⬜ **第五步**：HTTP 协议层（在 TcpServer 之上叠 HttpServer）
- ⬜ **第六步**：多线程 EventLoop pool

---

## 7. 快速开始（验证项目能跑）

```bash
cd /data/workspace/epoll_proj
cmake -B build && cmake --build build -j
./build/epoll_proj          # 终端 1：启动服务（监听 8888）
telnet 127.0.0.1 8888       # 终端 2：随便敲字符回车，会被原样回显
```
