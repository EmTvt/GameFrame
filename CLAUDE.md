# Project Instructions

This is a C++ networking framework project (from-scratch Linux epoll, muduo-style).
The core modules are:

- **net**: epoll / Reactor / EventLoop / Channel / TcpServer / TcpClient / Connection / Buffer / TimerQueue
- **log**: log_server (独立进程，TCP 收日志落盘) + 业务侧 LogSender / MPSCQueue / LOG_INFO（实现中）

## Before making code changes

1. Read `docs/CURRENT_STATE.md` —— 当前做到哪一步、有哪些已知问题
2. Read `docs/NEXT_TASKS.md` —— 接下来要做什么（不要自己猜）
3. If touching network code (`src/`)：read `docs/ARCHITECTURE.md` 和 `.claude/rules/net.md`
4. If touching logging code (`log_server/` 或将来的 `log_sender/`)：read `.claude/rules/log.md`
5. If adding / modifying tests：read `.claude/rules/testing.md`
6. If hitting a weird symptom：check `docs/DEBUGGING.md`（很多坑已经踩过）
7. **Do not rewrite architecture unless the task explicitly asks for it.**

## Doc map（按需读，别一次全读）

- `docs/PROJECT_CONTEXT.md` — 项目是什么 / 目录结构 / 构建命令（稳定信息）
- `docs/ARCHITECTURE.md` — 当前架构、模块边界、调用流（稳定信息）
- `docs/CURRENT_STATE.md` — 当前进度（每会话更新）
- `docs/NEXT_TASKS.md` — 下一步要做什么（每会话更新）
- `docs/DECISIONS.md` — 历次设计决策的追加日志
- `docs/DEBUGGING.md` — 踩过的坑、调试技巧
- `docs/INTERVIEW.md` — 面试介绍指南：整体架构 + 难点专题（问题→方案→效果→可追问）+ 追问清单

## Build / test commands

```bash
cmake -B build && cmake --build build -j
./build/test_event_loop      # EventLoop 跨线程能力
./build/test_tcp_client      # TcpClient 连接 + 重连
./build/epoll_proj           # echo demo（10s idle 踢人）
./build/log_server 9099 ./res/log
```

## Style

- **Prefer small, focused changes.** 不必要时不要大改。
- Preserve existing class **ownership boundaries**（详见 `.claude/rules/net.md`）。
- 头文件注释解释**动机**（"为什么这么做"），不描述代码。
- 临时调试输出用 `[DIAG ...]` 前缀，方便事后 grep 清理。
- 改完 EventLoop / Channel / Connection / TcpServer / TcpClient 任一核心组件，**用文字说明改后的调用链流向**。

## Handoff

会话结束前需要更新文档时，使用 `/handoff` skill（定义在 `.claude/skills/handoff/SKILL.md`）。
该 skill 只更新 `docs/CURRENT_STATE.md` / `docs/NEXT_TASKS.md` / `docs/DECISIONS.md`，**不改源码、不改 CLAUDE.md**。
