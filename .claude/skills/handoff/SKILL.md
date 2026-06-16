---
name: handoff
description: Update project handoff documents (docs/CURRENT_STATE.md, docs/NEXT_TASKS.md, docs/DECISIONS.md) after a coding session, so the next session can resume in 30 seconds.
disable-model-invocation: true
---

# Handoff Skill

Update the project context documents for the **current** coding session so the next Claude session can pick up without reading source code.

## Inputs to gather (read-only)

Run / read these to understand what changed in this session:

1. `git status --short` — 看哪些文件动过 / 新增了
2. `git diff --stat` — 看每个文件改了多少
3. `git log --oneline -10` — 看最近 10 条提交（即使本会话没提交，也帮上下文定位）
4. 必要时 `git diff <file>` 看具体改动
5. 当前 `docs/CURRENT_STATE.md`、`docs/NEXT_TASKS.md`、`docs/DECISIONS.md` 内容（要 diff-aware 更新而不是重写）

## Files you may modify

只允许动这三份：

- `docs/CURRENT_STATE.md`
- `docs/NEXT_TASKS.md`
- `docs/DECISIONS.md`（**仅当**本会话产生了新的设计决策才追加；否则不要碰）

## Files you must NOT modify

- ❌ 任何 `src/**`、`log_server/**`、`*.cpp`、`*.h`、`CMakeLists.txt`、`*.py` —— **不改源码**
- ❌ `CLAUDE.md` —— 入口要保持短而稳定，不在 handoff 里改
- ❌ `docs/PROJECT_CONTEXT.md` / `docs/ARCHITECTURE.md` / `docs/DEBUGGING.md` —— 这些是**稳定信息**
  - 例外：架构真的变了（模块拆分、所有权迁移）→ 同步改 `ARCHITECTURE.md`
  - 例外：本会话踩到新坑 → 追加到 `DEBUGGING.md`
- ❌ `.claude/rules/**` —— 规则变更需要单独的对话明确推动

## Update rules

### docs/CURRENT_STATE.md
- 顶部 `Last updated:` 改成今天日期
- `Working` 区：把本会话新做完的能力加进去（一行一条，**点描述事实**，不写动机）
- `In Progress` 区：本会话开了头但没收尾的写在这；没有就写"无"
- `Known Problems / 待验证` 区：本会话发现的新风险加进去；本会话修掉的旧风险**删掉**
- 演进步序表：本会话完成的步骤打 ✅；下一个未完成步骤标"⬜ 下一个"

### docs/NEXT_TASKS.md
- 本会话已经完成的 task：**整段删掉**（不留删除线，不留 ~~~）
- 完成后下一个 task 自动顶上来
- 本会话冒出来的新需求：插到合适位置（短期/中期/长期），保持"按投入产出比 + 当前痛点"排序
- 已经过时的 task：直接删

### docs/DECISIONS.md
- **仅当**本会话敲定了新决策才追加一节，格式：
  ```markdown
  ## YYYY-MM-DD: <一句话决策标题>

  **Reason**
  ...

  **Impact**
  ...
  ```
- **决策不允许重写**。要推翻旧决策，就**追加新决策**并写明 `supersedes 2026-MM-DD: ...`
- 没新决策就不要碰这个文件

## Output rules

- **简洁**：不要长篇解释每个改动，技术笔记面向"30 秒接手的下一会话"
- **可操作**：写"下一步要做 X"而不是"未来可以考虑 X"
- **删过期**：旧 TODO 一定要清掉，留着会误导下一会话
- **保留语气**：不要把现有文档的中文风格改成英文，也不要把动机注释删成纯描述

## Workflow

1. 跑 `git status --short` + `git diff --stat`，列出本会话动过的文件清单
2. 读现有的三份目标文档，看哪些条目跟本会话改动相关
3. 对每个文件做**最小化** diff（只改要改的段，不全文重写）
4. 写完之后用一个简短总结告诉用户：
   - 改了哪三/二/一个文档
   - 关键变更点（3 行以内）
   - 下一会话第一步该读什么、做什么

## Self-check before finishing

- [ ] CLAUDE.md 没动
- [ ] src/ 下没动
- [ ] CURRENT_STATE.md 的"演进步序"和 NEXT_TASKS.md 的第一项**对得上**
- [ ] DECISIONS.md 只追加，没改旧条目
- [ ] 没留 "TODO: 待补充" 这种伪条目
