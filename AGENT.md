# AGENT.md

> 本文件说明 `docs/` 下每一个文档的作用、什么时候读、由谁更新。
> 目的：让任何一次新会话都能**按需精准取用文档**，不用一次全读，也不会改错地方。
> 项目总规则（构建命令 / 风格 / 改代码前的阅读顺序）见根目录 `CLAUDE.md`。

## 文档速览

| 文档 | 一句话作用 | 稳定性 | 何时读 |
|------|-----------|--------|--------|
| `docs/PROJECT_CONTEXT.md` | 项目是什么、为什么存在、目录结构、构建命令、有哪些可执行产物 | 稳定，很少变 | 第一次接触项目 / 想知道"有哪些 target、怎么构建" |
| `docs/ARCHITECTURE.md` | 当前架构（不是愿景）：分层、模块边界、所有权关系、调用流向 | 稳定 | 动网络代码 `src/` 前 / 想理解模块怎么协作 |
| `docs/CURRENT_STATE.md` | 当前进度：已做完什么、进行中什么、已知问题、演进步序快照 | **每会话更新** | 新会话**第一份读**，定位"现在做到哪" |
| `docs/NEXT_TASKS.md` | 主线路线：接下来做什么（网络库 → HTTP → 协程 → 游戏后台方向），逐卡片给文件/职责/验收 | **每会话更新** | 决定"下一步做什么"时（**不要自己猜下一步**） |
| `docs/RPC_DESIGN.md` | RPC 框架的**设计与动机**（为什么这么设计）：传输选型、帧格式、request_id、无反射注册表等 | 稳定（设计定稿） | 做 RPC 相关工作、想理解"为什么这么设计"时 |
| `docs/RPC_NEXT_TASKS.md` | RPC 的**落地清单**（下一步做什么、怎么验收）：逐卡片 RPC-1~8 + 已核对的真实接口签名 | 随 RPC 进度更新 | 动手实现 RPC 时按卡片推进 |
| `docs/DECISIONS.md` | 设计决策的**追加日志**：每条记录 Reason / Impact，推翻旧决策时新增 supersedes 条 | 只追加，不重写 | 想知道"某个选择当初为什么这么定" / 做出新决策后追加 |
| `docs/DEBUGGING.md` | 踩过的坑、调试技巧、常见现象的根因（如 telnet 注入 IAC、测试客户端选择） | 追加为主 | 遇到诡异症状先来查 / 踩到新坑后追加 |
| `docs/INTERVIEW.md` | 面试介绍指南：电梯陈述 + 难点专题（问题→方案→效果→可追问）+ 追问清单 | 随项目进展更新 | 准备面试讲解 / 想从"如何对外表达"角度回顾项目 |

## 读法建议（别一次全读）

- **新会话定位**：`CURRENT_STATE.md`（做到哪了）→ `NEXT_TASKS.md`（下一步）。
- **动网络代码 `src/`**：先读 `ARCHITECTURE.md` 和 `.claude/rules/net.md`。
- **做 RPC**：`RPC_DESIGN.md`（为什么）配 `RPC_NEXT_TASKS.md`（怎么做、验收）。
- **动日志代码**（`log_server/` / `log_sender/`）：读 `.claude/rules/log.md`。
- **加/改测试**：读 `.claude/rules/testing.md`。
- **遇到怪症状**：先翻 `DEBUGGING.md`，很多坑已经踩过。
- **想知道某设计的来龙去脉**：查 `DECISIONS.md`。

## 文档职责边界（避免写错地方）

- **设计动机（为什么）** → `ARCHITECTURE.md`（结构） / `RPC_DESIGN.md`（RPC 专题） / `DECISIONS.md`（决策日志）。
- **下一步做什么（怎么做、验收）** → `NEXT_TASKS.md`（主线） / `RPC_NEXT_TASKS.md`（RPC）。
- **现状与进度** → `CURRENT_STATE.md`（唯一进度真相）。
- **调试经验** → `DEBUGGING.md`。
- **对外表达** → `INTERVIEW.md`。

## 更新约定

- 会话结束前需要更新文档时，用 `/handoff` skill（定义在 `.claude/skills/handoff/SKILL.md`）。
- 该 skill **只更新** `CURRENT_STATE.md` / `NEXT_TASKS.md` / `DECISIONS.md`，**不改源码、不改 `CLAUDE.md`**。
- `DECISIONS.md` 只追加不重写；推翻旧决策时新增一条并注明 supersedes。
