# MCP: 可用的外部服务器与典型用法

> 当前项目本身**不依赖**任何 MCP 服务器即可独立编译运行。本文档记录在协作过程中可能用到的 MCP 工具，方便 AI 在合适场景下调用。

## 当前默认可用

> 注：具体可用列表由运行环境决定，AI 在调用前应通过 `mcp_get_tool_description` 确认工具存在。

### 1. `bkm-bkte-log-query`（蓝鲸日志查询）

**用途**：本项目暂未对接，**保留位**。如果未来把服务部署到生产环境并接入蓝鲸日志，可以用它来查 `[server]` / `[conn ...]` 这类日志。

典型调用流程（参考）：
```
list_index_sets → get_index_set_fields → search_logs
```

**当前项目阶段不需要使用**——本地运行直接看 stdout 即可。

---

## 何时考虑接入 MCP

| 场景 | 适合的 MCP 类型 |
|---|---|
| 想把日志接到蓝鲸做分析 | bkm-bkte-log-query |
| 部署到云上（cloudbase / lighthouse） | 在 IDE Integration 菜单里登录对应集成 |
| 需要查内部知识库（如 muduo 源码、tRPC 文档） | RAG_search（已内置）|

---

## 不需要 MCP 也能做的事

本项目核心开发流程**完全本地化**，AI 应优先使用以下内置工具：

- `read_file` / `search_content` / `search_file` —— 看代码、找定义
- `replace_in_file` / `write_to_file` —— 改代码
- `execute_command` —— 编译、运行、查进程

**不要为了用 MCP 而用 MCP**——简单任务用基础工具更快、更可控。

---

## 在本项目中查询知识库的建议

如果遇到需要查阅外部技术资料的情况（比如确认某个 Linux 系统调用的细节、muduo 的某个设计模式），可以使用：

- `RAG_search` 工具，知识库选择 `TencentOS` / `tRPC（C++）` / `腾讯公司编程指南知识库` 等
- 但**只在确实需要**外部权威信息时使用，本地代码注释和 `man 手册`通常足够

---

## 未来扩展位

- [ ] 接入压测工具 MCP（自动化跑 benchmark）
- [ ] 接入云部署 MCP（一键发到测试环境）
- [ ] 接入 issue 跟踪 MCP（同步 TODO 到外部系统）

以上均为占位，**当前阶段不要主动接入**。
