# demo/ 技术文档

> 多 Agent 编排框架（实现 `link.md` 设计）+ IdeaCanvas 多 Agent 协作画布应用
>
> 适用目录：`antigravity-sdk-python/demo/`
> 依赖：Python ≥ 3.10；可选运行时依赖见下文「运行方式」

---

## 目录

1. [项目概述](#1-项目概述)
2. [总体架构](#2-总体架构)
3. [目录结构](#3-目录结构)
4. [核心模块文档](#4-核心模块文档)
   - [models.py — 领域模型](#41-modelspy--领域模型)
   - [orchestrator.py — 任务编排与调度](#42-orchestratorpy--任务编排与调度)
   - [messaging.py — 消息通信与复命协议](#43-messagingpy--消息通信与复命协议)
   - [agent_runtime.py — Agent 执行层（真实 SDK）](#44-agent_runtimepy--agent-执行层真实-sdk)
   - [infrastructure.py — 全局基础设施](#45-infrastructurepy--全局基础设施)
   - [acceptance.py — 交付物验收](#46-acceptancepy--交付物验收)
   - [completion.py — 完成判定规则](#47-completionpy--完成判定规则)
   - [main.py — 端到端装配演示](#48-mainpy--端到端装配演示)
   - [a2a.py — A2A 协议层（跨网络）](#49-a2apy--a2a-协议层跨网络)
   - [a2a_antigravity_server.py — 独立 Antigravity A2A 服务](#410-a2a_antigravity_serverpy--独立-antigravity-a2a-服务)
   - [a2a_graph.py — LangGraph 多 A2A Agent 组合](#411-a2a_graphpy--langgraph-多-a2a-agent-组合)
   - [a2a_test.py — 真实 LLM 端到端测试](#412-a2a_testpy--真实-llm-端到端测试)
5. [canvas 子项目（IdeaCanvas）](#5-canvas-子项目ideacanvas)
6. [SDK 逐项对应关系](#6-sdk-逐项对应关系)
7. [运行方式](#7-运行方式)
8. [关键设计决策与注意事项](#8-关键设计决策与注意事项)

---

## 1. 项目概述

`demo/` 是 **antigravity-sdk-python** 仓库中的一个演示/参考实现，目标是把设计文档 [`link.md`](./link.md) 描述的**多 Agent 编排系统**落地为一个可运行的框架骨架。

核心原则（见 `README.md`）：

> **SDK 能用到的地方直接用，没有的地方占位。**

`google.antigravity` SDK 本身是**单 Agent** 基础设施（Agent / Conversation / Hooks / Policy / Tools）。`link.md` 要求的**多 Agent 编排**能力——任务 DAG、编排器、交付物验收、消息协议、依赖死锁检测、全局收敛——SDK 均不直接提供。因此本框架：

- **agent_runtime.py** 与 **infrastructure.py** 中大量**直接复用真实 SDK**（Agent、SubagentConfig、policy、hooks、RetryConfig、response_schema、conversation_id、otel、AskQuestionHook 等）；
- SDK 缺失的能力（任务图、编排调度、验收、复命、完成收敛等）由**纯 Python 占位实现**，并在每个模块 docstring 中标注「SDK status: 是/否/部分」。

整个 `demo/` 分为两部分：

| 部分 | 说明 | 入口 |
| --- | --- | --- |
| **框架层** | `demo/*.py`（除 canvas）——实现 link.md 的编排框架 | `python -m demo.main` |
| **应用层** | `demo/canvas/`——IdeaCanvas：一个多 Agent 协作的可视化画布 Web 应用 | `python -m demo.canvas.run` |

canvas 应用**复用框架层**的 orchestrator / models / completion / messaging / infrastructure，使 link.md 的设计在运行时真实体现。

---

## 2. 总体架构

对应 `link.md` 第四节「建议最终采用的整体架构」，分为四层。注意第四层基础设施**并非只位于底部**，而是**横跨前三层**为所有模块提供服务。

```
┌─────────────────────────────────────────────────────────────┐
│ 第一层：总指挥与任务编排                                        │
│   任务拆分 · 动态调度 · 依赖解析 · 全局完成判定                  │
│   实现：orchestrator.Orchestrator + canvas.Commander            │
│   SDK：否（SDK 无编排器）                                       │
├─────────────────────────────────────────────────────────────┤
│ 第二层：Agent 执行与协作                                        │
│   任务执行 · 消息通信 · 工具调用 · 工作目录隔离                   │
│   实现：agent_runtime.run_task + canvas.EggAgent                │
│   SDK：部分（Agent / SubagentConfig，LLM 驱动子 Agent）          │
├─────────────────────────────────────────────────────────────┤
│ 第三层：任务结果与交付闭环                                      │
│   交付物生成 · 自动验收 · 子任务复命 · 总指挥确认                 │
│   实现：acceptance.py + completion.py + messaging.REPORT/DELIVER │
│   SDK：否（无交付物/验收/复命）                                  │
├─────────────────────────────────────────────────────────────┤
│ 第四层：全局基础设施（横跨所有层）                               │
│   生命周期与状态机 · 持久化 · 重试与恢复 · 权限控制               │
│   审计日志 · 监控告警 · 人工审批                                  │
│   实现：infrastructure.py                                       │
│   SDK：生命周期/持久化/重试/权限 是；审计/监控 部分              │
└─────────────────────────────────────────────────────────────┘
```

### 完成判定公式（link.md 第五节）

整个系统的最终完成条件：

```
TaskCompleted = ExecutionFinished
               ∧ ArtifactsValid
               ∧ AcceptancePassed
               ∧ ReportSubmitted
```

即：**任务执行已结束 ∧ 要求的交付物通过验证 ∧ 验收成功 ∧ 子 Agent 已提交复命**，才判定子任务完成。总指挥还需单独执行**全局验收**（所有必需子任务通过、无未处理的失败或阻塞）才能宣布整个项目完成。

---

## 3. 目录结构

```
demo/
├── __init__.py          # sys.path 修正：让 pip 安装的 SDK 优先于仓库源码树
├── models.py            # 领域模型：Task/Artifact/Acceptance/Dependency/TaskRun/Approval/AgentInfo
├── orchestrator.py      # 任务编排：拓扑排序 + 死锁检测 + 并发调度 + 重试/超时/取消
├── messaging.py         # 消息协议：AgentMessage + MessageBus（去重/关联/确认回执）
├── agent_runtime.py     # Agent 执行层：直接使用真实 SDK 构造 AgentConfig 并运行任务
├── infrastructure.py    # 全局基础设施：状态机/持久化/恢复/权限/审批/审计/指标
├── acceptance.py        # 交付物验收：文件校验 + 结构化验收标准执行
├── completion.py        # 完成判定：单任务完成规则 + 全局收敛报告
├── main.py              # 端到端装配演示（串联所有模块）
├── a2a.py               # A2A 协议层：AgentCard/executor/server/client/dispatch（跨网络）
├── a2a_antigravity_server.py  # 独立 Antigravity A2A 服务（常驻，后端 llama.cpp）
├── a2a_graph.py         # LangGraph 组合多个远程 A2A agent（路由分发）
├── a2a_test.py          # 真实 LLM（llama.cpp 9000）端到端测试 a2a.py
├── link.md              # 设计文档（框架的目标蓝图）
├── README.md            # 框架 README
├── TECHNICAL_DOC.md     # 本技术文档
├── worktrees/           # egg 工作树目录（canvas 运行时生成）
└── canvas/              # IdeaCanvas：多 Agent 协作画布 Web 应用
    ├── __init__.py
    ├── config.py        # 后端配置：openai/litert/cloud 三选一
    ├── context.py       # 版本化共享上下文（TTL / 大小上限）
    ├── egg.py           # EggAgent：一个蛋 = 一个 SDK Agent + 独立 worktree
    ├── graph.py         # Canvas：蛋集合 + 依赖边 + 总指挥 + DAG 并发执行
    ├── reasoning.py     # 💭 推理流捕获（基于 SDK 标准流）
    ├── server.py        # FastAPI：REST + WebSocket 控制通道
    ├── run.py           # 入口：uvicorn 启动
    └── index.html       # 前端单页应用（画布 UI）
```

---

## 4. 核心模块文档

### 4.1 models.py — 领域模型

> SDK status: **否**。SDK 中不存在 Task / TaskRun / Dependency / Artifact / Acceptance 等领域类型（仅有单 Agent）。本模块是编排层的领域实体，详见 link.md 三.3「Agent 与任务不是一回事」。

本模块定义框架操作的所有实体，全部为 `@dataclass`，均提供 `to_dict()` / `from_dict()` 以支持 JSON 序列化与持久化。

#### 枚举（link.md 三.1：任务状态与交付物状态分离）

| 枚举 | 值 | 含义 |
| --- | --- | --- |
| `TaskStatus` | `planned / running / waiting_approval / blocked / success / failed / canceled` | 任务生命周期状态 |
| `ArtifactStatus` | `not_generated / generated / under_review / accepted / rejected` | 交付物生命周期（未生成/已生成/验收中/验收通过/验收失败） |
| `ExecutionStatus` | `pending / running / finished / error / canceled / timeout / abnormal_exit / process_lost` | 单次执行（TaskRun）状态，区分正常失败/超时/异常退出/主动取消/进程失联 |
| `AgentRunStatus` | `online / offline / busy` | Agent 运行状态（Agent 在线 ≠ 其任务在执行） |
| `ApprovalStatus` | `pending / approved / rejected / timed_out` | 审批生命周期 |

> **关键设计**：任务状态与交付物状态**分开建模**。任务执行成功但交付物验收失败时，任务不能算最终完成。

#### 实体

| 实体 | 关键字段 | 说明 |
| --- | --- | --- |
| `AgentInfo` | `agent_id, name, capabilities, permissions, status` | Agent 的身份/能力/权限/运行状态 |
| `Artifact` | `artifact_id, task_id, path, kind, status, version, metadata` | 任务产生的交付物；`kind ∈ {file, dir, message, result}`；带版本号 |
| `Acceptance` | `task_id, criteria, passed, evidence` | 验收规则/结果/证据；`criteria` 可为字符串/字典/可调用对象 |
| `Dependency` | `task_id, depends_on, condition` | 依赖条件；`condition ∈ {finished, accepted, reported}`（completed 由 completion 判定） |
| `TaskRun` | `run_id, task_id, agent_id, status, start_time, end_time, error` | 某次具体执行记录（TaskRun ≠ Task） |
| `Approval` | `approval_id, task_id, requested_by, reason, status, decided_by, deadline` | 审批是**独立实体**而非状态标签 |
| `Task` | `task_id, title, description, owner_agent_id, priority, required, status, dependencies, artifacts, acceptance, runs, report_submitted, confirmed` | 工作单元；`report_submitted` 跟踪「子任务已复命」，仅在验收通过后置位；`confirmed` 跟踪「总指挥已确认」 |

`Task.priority` 范围 1（最高）~ 10（最低）。`Task.required` 决定可选任务失败是否阻塞全局收敛。

---

### 4.2 orchestrator.py — 任务编排与调度

> SDK status: **否**。SDK 无编排器/DAG/调度器/依赖解析/死锁检测。其唯一的多 Agent 原语是 LLM 驱动的 `start_subagent`（types.py:351），由外部 harness 协调，非 SDK 编排框架。

实现第一层「任务编排与调度管理」（link.md 一.1、二.7）。

#### 类 `Orchestrator`

**构造参数**：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `max_concurrency` | 4 | 并发上限（≥1） |
| `dispatch` | None | 任务执行回调（同步用 `run()`，异步用 `arun()`）；接 `agent_runtime.run_task` 或 canvas 的 egg.run |
| `on_event` | None | 事件观察者（started/finished/failed/retry/timeout/canceled/appended） |
| `task_timeout` | None | 单任务超时（秒） |
| `max_retries` | 0 | 最大重试次数 |
| `retry_backoff` | 0.5 | 重试退避基数（`backoff * attempt`） |

**任务注册/分解**：

- `register(task)` / `unregister(task_id)` / `get(task_id)` / `all()`
- `add_dependency(task_id, depends_on, condition="finished")`
- `decompose(parent, subtasks)`：注册子任务并让它们依赖父任务完成
- `append(task)`：**动态追加**（运行时发现新任务）

**图算法**：

- `detect_deadlock() -> list[list[str]]`：DFS 三色（白/灰/黑）检测循环依赖，返回环列表。A→B→C→A 会被识别。
- `topo_order() -> list[str]`：拓扑排序；有环抛 `DeadlockError`。使用**优先级堆**，数字小者（高优先级）先出。

**依赖条件** `dependency_satisfied(dep)`：

| condition | 判定 |
| --- | --- |
| `finished` | 上游处于终态（success/failed/canceled）或 blocked/waiting_approval（已“尘埃落定”，下游不再无限等待）；上游回到 PLANNED（重试中）时**不满足** |
| `accepted` | 上游 `acceptance.passed is True` |
| `reported` | 上游 `report_submitted` |
| `completed` | 委托 `completion.evaluate_task_completed(upstream).complete` |

**调度核心**：

- `ready_tasks()`：返回所有 `PLANNED` 且依赖满足、且退避时间已到的任务，按 `(priority, task_id)` 排序。
- `_next_retry_delay()`：计算下一个退避到期时间。
- `stats()`：运行时视图（状态计数、就绪队列、运行中任务、重试数、阻塞列表）。
- `replan()`：动态追加后重算就绪集合。

**状态辅助**：`_begin`（开 RUNNING 记录）、`_finish`、`_fail`（记录失败尝试，未超重试上限则回到 PLANNED 并设置 `retry_after` 退避）、`_timeout`、`_cancel`。

**取消**：`cancel(task_id)` / `cancel_all()`，取消时更新终态，避免调度器无限等待。

**两个调度器**：

- `run(dispatch, timeout)`：**同步**调度，用 `concurrent.futures.ThreadPoolExecutor`（max_workers=max_concurrency）+ `concurrent.futures.wait` 轮询。
- `arun(dispatch, timeout)`：**异步**调度，用 `asyncio` Future + `asyncio.wait` 轮询，天然适配 SDK Agent 的异步接口。

两者都在结束时调用 `_check_stuck()`：若仍有 PLANNED 任务无法调度，抛 `DeadlockError` 报告其等待的依赖。

**`ScheduleSlot`**（dataclass）：一次调度记录，含任务 + 并发桶。

---

### 4.3 messaging.py — 消息通信与复命协议

> SDK status: **否**。SDK 无结构化 Agent 间消息类型。最接近的是 `Step.trajectory_id/parent_trajectory_id`（types.py:1136），仅作子 Agent 轨迹记账，非消息契约。

实现第一/二层之间的「消息通信与复命协议」（link.md 一.4）。

#### 消息类型 `MessageType`

| 值 | 含义 |
| --- | --- |
| `TASK_ASSIGN` | 任务分配 |
| `PROGRESS` | 进度汇报 |
| `BLOCKED` | 阻塞通知 |
| `DELIVER` | 交付提交 |
| `REPORT` | **复命**（子 Agent 向总指挥报告完成） |
| `CANCEL` | 取消 |

#### `AgentMessage`（dataclass）

结构化消息，字段：`msg_type, task_id, agent_id, status, artifacts, error, payload, msg_id, correlation_id, sender, require_ack, timestamp`。提供 `to_dict()`。

#### `MessageBus`（进程内消息总线）

提供 link.md 一.4 要求的完整能力：

- **去重**：按 `msg_id` 去重，重复发布返回 `False`。
- **关联追踪**：`correlation_id` 关联请求/回复；`correlate(cid)` 查历史，`wait_for(cid, msg_type, timeout)` 异步等待回复。
- **确认回执**：`require_ack=True` 时 `apublish()` 等待确认；`ack(msg_id)` 确认；`expire_acks()` / `pending_acks()` / `expired_acks()` 处理超时。
- **写入权限控制**：可选 `authorize(msg) -> bool` 回调，无权发布抛 `PermissionError`（避免多 Agent 同时写产生冲突）。
- **订阅**：`subscribe(agent_id, handler)`，`'*'` 为通配；`unsubscribe`；`subscriber_count`。
- **历史**：`history(agent_id, msg_type, task_id)` 过滤查询；`max_history` 上限。
- `reply(request, msg_type, ...)`：构造关联到请求的回复（目标指向请求发送者）。

> TODO(platform)：一旦 SDK 暴露 Agent 间传输，就用持久化 broker（如 pub/sub）替换进程内总线。`AgentMessage` 契约与传输无关。

---

### 4.4 agent_runtime.py — Agent 执行层（真实 SDK）

> SDK status: **是**。本模块在能用 SDK 的地方**直接使用真实 SDK**。

实现第二层「Agent 执行与协作」（link.md 四.第二层）。SDK import 被**保护**：编译型运行时二进制仅运行时需要，因此导入本模块做检查无需二进制，但真正运行 Agent 需要它。

#### SDK import 保护

```python
try:
    from google.antigravity import Agent, LocalAgentConfig
    from google.antigravity.hooks import hooks, policy
    from google.antigravity.types import (CapabilitiesConfig, ModelAPIRetryConfig,
        ModelOutputRetryConfig, RetryConfig, SubagentConfig, SystemInstructions)
    from google.antigravity.utils import interactive
    from google.antigravity.utils.otel import get_otel_hooks
    SDK_AVAILABLE = True
except Exception as _err:
    SDK_AVAILABLE = False
```

#### `_audit_hook_collector()`

用真实 SDK `hooks.post_tool_call`（hooks.py:411）构建审计钩子，返回收集日志的 list。这是 link.md 二.8 的审计占位存储（SDK 有工具调用钩子但无独立事件日志）。

#### `build_agent_config(system_instructions, *, ...) -> Any`

**构造真实 SDK AgentConfig**。`backend` 选择具体配置类：

| backend | 配置类 | 说明 |
| --- | --- | --- |
| `"cloud"`（默认） | `LocalAgentConfig` | Google Antigravity 云；支持 `vertex/project/location/api_key/model` |
| `"openai"` | `LocalOpenAIAgentConfig` | 本地 OpenAI 兼容服务器（Ollama/LM Studio/vLLM）；`model/base_url` |
| `"litert"` | `LiteRTAgentConfig` | 端侧 `.litertlm`；`model_path` |

内部装配（全部对应真实 SDK 旋钮）：

- **权限/安全策略**（link.md 一.6）：默认 `policy.deny_all()` + 允许 `view_file`（或 `allowed_tools`）+ 显式 `deny`/`ask_user`。
- **重试/退避**（link.md 一.3）：`RetryConfig(model_api=ModelAPIRetryConfig(max_retries), model_output=ModelOutputRetryConfig(max_retries=1))`。
- **能力**：`CapabilitiesConfig(enable_sandbox=...)`；`response_schema` 非字符串时 `json.dumps` 转为 JSON 字符串（`finish_tool_schema_json`，types.py:546）。
- **Hooks**：调用方 hooks + `get_otel_hooks()`（link.md 一.11）。
- **持久化/恢复**：`conversation_id` + `session_continuation_mode`（link.md 一.5）。
- **预算**：`budget_config`（link.md 一.6 token/工具调用/模型调用上限）。
- 其余：`workspaces/app_data_dir/save_dir/subagents/compaction_config` 等。

`TypeError` 兜底：丢弃具体配置类不接受的可选键后重试。

#### `run_task(task, prompt, config, human_in_the_loop=False) -> str`

用真实 SDK Agent 执行单个任务并记录 TaskRun：

```python
async with Agent(config) as agent:
    response = await agent.chat(prompt)
    text = await response.text()
```

- 复用 orchestrator 已开的 RUNNING 记录，避免重复计数。
- `human_in_the_loop=True` 时追加 `interactive.AskQuestionHook`（link.md 一.6/二.10）。
- 成功置 `ExecutionStatus.FINISHED` + `TaskStatus.SUCCESS`；失败置 `ERROR` + `FAILED` 并 re-raise。
- 返回文本即任务执行结果（link.md 五：ExecutionFinished 代理）。

#### `with_conversation(config, conversation_id, *, save_dir=None) -> Any`

把 A2A 的 `context_id` 映射为 SDK `conversation_id`，返回一个**逐调用**配置副本，让 LLM 跨请求保持记忆（供 `a2a.make_sdk_executor` 使用）。

- 用 `config.model_copy(update={...})` 设置 `conversation_id` + `session_continuation_mode=CREATE_OR_RESUME`（会话不存在则创建，存在则续接）。
- **关键**：必须配合**固定 `save_dir`**——SDK 把会话持久化到 `<save_dir>/<conversation_id>.db`，否则每次 `run_task` 新建的 Agent 子进程退出时会话状态丢失，记忆失效。
- `conversation_id` 为 None 或 SDK 不可用时返回原 config 不变（模拟执行器路径不受影响）。
- 这也是 `a2a_test.py` 验证「LLM 会话记忆（真实续接）」的机制（见 4.12）。

---

### 4.5 infrastructure.py — 全局基础设施

> SDK status: **混用**。SDK 有的直接用（RetryConfig/SessionContinuationMode/policy/otel），没有的占位（任务状态机/TaskStore/任务级恢复/任务级鉴权/审计事件日志/指标计数器）。

实现第四层「全局基础设施」（link.md 四.第四层）。

#### `bind_run(run_id)`（contextmanager）

用 `contextvars.ContextVar` 将审计事件关联到当前 TaskRun。

#### `StateManager` — 状态机（link.md 三.1）

> SDK status: **否**（无任务级状态机，仅 HookRunner + 步骤级 StepStatus）。

- `_TASK_TRANSITIONS` / `_ARTIFACT_TRANSITIONS`：预定义合法迁移表。
- `can_transition` / `transition`（非法迁移抛 `ValueError`）/ `try_transition`（不合法则不动）。
- `transition_artifact`：交付物状态迁移。
- 任务状态与交付物状态**分开**建模（SUCCESS 终态、FAILED/CANCELED 可回到 PLANNED 重试等）。

#### 持久化（link.md 一.5）

- `make_session_continuation(mode)`：返回真实 `SessionContinuationMode`（resume/create_or_resume/create）。
- `TaskStore`：持久化任务/运行/交付物全对象图，可镜像到 JSON 文件。
  - CRUD：`save`（=upsert）/`load`/`get`/`all`/`remove`/`clear`/`__len__`/`__contains__`。
  - JSON：`to_dict`/`save_json`（原子写：`.tmp` + `replace`）/`load_json`（兼容 `{"tasks":[...]}`、list、dict 三种格式）；`_autosave` 自动落盘。
  - 兼容别名 `dump_json`。

#### 重试与恢复（link.md 一.3）

- `build_retry_config(max_retries=3)`：返回真实 SDK `RetryConfig`。
- `recover_incomplete_runs(task_store, state)`：找出最后一次运行未达终态的任务，将其运行标记 `PROCESS_LOST`，任务回到 `PLANNED` 以便重新调度；重置 `report_submitted`；记录 `recovered_at`。返回恢复的任务 id 列表。

#### 权限控制（link.md 一.6）

- `build_security_policies(allowed, denied, ask_user)`：真实 SDK policy 对象列表。
- `DEFAULT_DANGEROUS_COMMANDS`：危险 shell 命令黑名单（`rm -rf /`、`mkfs`、`shutdown` 等）。
- `build_agent_policies(worktree, denied_commands, allow_all)`：为单个 egg 构建策略——`policy.workspace_only([worktree])` 限定文件工具只在自己 worktree；危险命令 `policy.deny("run_command", when=...)`；最后 `allow_all`/`deny_all`。
- `build_budget_config(...)`：真实 `BudgetConfig`（token/工具调用/模型调用上限 + scope）。
- `can_agent_modify_task(agent, task, action)`：**任务级授权**（SDK 只管工具不管任务归属）。接受的权限形式：`*`、`task:*`、`write:*`、`<task_id>`、`<action>:<task_id>`；owner 恒授权。
- `assert_can_modify`：无权抛 `PermissionDenied`。

#### 人工审批（link.md 二.10）

`ApprovalManager`（审批是独立流程实体）：

- 查询：`get`/`all`/`pending`/`for_task`。
- 命令：`request`（创建审批，任务转 `WAITING_APPROVAL`）、`decide`（approved→任务回 `PLANNED`；rejected→`FAILED`）、`expire`（超时→`TIMED_OUT` + 任务转 `BLOCKED`）。
- 持久化：`to_dict`/`save_json`/`load_json`。

#### 审计日志（link.md 二.8）

`AuditLog`：append-only 事件日志，带 `run_id` 关联 + 可选 JSON sink。

- `record(event, **kwargs)`：自增 `seq`，记录 `ts`/`run_id`/事件；超 `max_events` 截断；可选追加到 JSONL 文件。
- `query(event, run_id)`。
- `install_sdk_hooks()`：挂载真实 SDK `post_tool_call` 钩子，把每次工具调用写入日志（带 run_id）。
- `append_json`/`save_json`/`load_json`（兼容数组或 JSONL）。

#### 监控与指标（link.md 二.11）

`Metrics`：计数器/ gauges/直方图注册表。

- `inc(name, by)`、`set_gauge(name, value)`、`observe(name, value, cap)`、`get(name)`、`snapshot()`/`to_dict()`、`reset()`。

> SDK 提供 OpenTelemetry tracing 与 `UsageMetadata`，但不导出 token/步数等计数器，故此处自建。

---

### 4.6 acceptance.py — 交付物验收

> SDK status: **否**。SDK 不生成也不校验交付物。最接近的是 `AgentConfig.response_schema`（connection.py:97），校验的是 Agent 文本/结构化输出，而非磁盘文件。

实现第三层「交付物验收」（link.md 一.2）。

#### 交付物校验

- `validate_artifact(artifact, task)`：
  - `message/result/text` 类：无文件足迹，视为已生成。
  - `file/dir`：必须存在；文件必须非空。缺失/为空 → `REJECTED` 并记录 reason；否则 → `GENERATED`。
  - 相对路径相对于任务的 `worktree`（`task.metadata["worktree"]`，否则 `Path.cwd()`）。
- `validate_artifacts(artifacts, task)`：全部校验；无交付物时**空真**（vacuously true）。
- `mark_artifacts(artifacts, accepted)`：验收后把交付物移到 `ACCEPTED`/`REJECTED`。

#### 验收标准执行

`CriterionResult`（dataclass）：`criterion, passed, evidence, skipped`。

**字符串标准**（`_eval_string`）：

| 形式 | 说明 |
| --- | --- |
| `"file exists: path"` / `"exists: path"` | 路径存在 |
| `"non-empty: path"` / `"file non-empty: path"` | 路径存在且非空 |
| `"file contains: path :: text"` / `"contains: path :: text"` | 文件包含子串 |
| `"artifacts exist"` / `"file exists"` / `"files exist"` / `"exists"` | 所有交付物存在 |
| `"non-empty"` / `"nonempty"` / `"artifacts non-empty"` | 所有交付物非空 |
| `"min_artifacts: N"` | 至少 N 个交付物存在 |
| `"command: pytest -q"` | shell 命令退出码 0（cwd=worktree，默认超时 60s） |

**结构化标准**（`_eval_dict`）：`{"type": "file_exists"|"non_empty"|"contains"|"min_artifacts"|"command", ...}`。

**可调用标准**（`_eval_callable`）：`callable(task) -> bool | (bool, evidence)`，异常视为失败。

> **未知字符串标准被记录为「未验证」但不判失败**，因此已有的描述性标准仍可用。

`evaluate_criterion(crit, task, artifacts) -> CriterionResult`：按类型分发。

`evaluate_acceptance(acceptance, task, force=False) -> bool`：执行所有标准，记录证据（`PASS/FAIL` 前缀），置 `acceptance.passed`。**结果缓存在 Acceptance 对象上**：重复评估是 no-op（除非 `force=True`），使 completion 反复调用时保持廉价。证据行会被替换，但操作者自定义证据保留。

---

### 4.7 completion.py — 完成判定规则

> SDK status: **部分**。ExecutionFinished 可用 Agent 步骤状态或 Task.status 近似；其余三项在本模块基于 SDK 之上实现。

实现 link.md 第五节「完成判定规则」与二.12「全局终止与完成收敛」。

#### 单任务完成规则

`TaskCompleted = ExecutionFinished ∧ ArtifactsValid ∧ AcceptancePassed ∧ ReportSubmitted`。

`evaluate_task_completed(task, force=False) -> CompletionReport`：

- `execution_finished`：task 处于终态或任一 run 处于终态；blocked 时强制 False。
- `artifacts_valid`：`acceptance.validate_artifacts`（真实文件系统校验）。
- `acceptance_passed`：有 acceptance 时执行 `evaluate_acceptance`（缓存）；**未启动的任务不提前执行/缓存标准**，以免污染真实运行。
- `report_submitted`：`task.report_submitted`。
- `complete`：四者全真且非 blocked。
- `reason`：blocked 时给出审批/阻塞原因，否则列出缺失项。

`CompletionReport.missing()`：返回未满足的项（ExecutionFinished/ArtifactsValid/AcceptancePassed/ReportSubmitted）。

#### 全局收敛

`global_report(tasks, force=False) -> GlobalCompletion`：

- 忽略 `CANCELED` 任务（已撤回，非尝试）。
- **必需任务**（`required=True`）必须完成；**可选任务**允许失败（计入 `optional_failed`）。
- 无显式 required 标记时回退为「全部必须完成」。
- 统计：`total/completed/required_total/required_completed`、`blocked/failed/pending/optional_failed`、`reasons`、每任务明细 `tasks`。
- `complete = required_completed == required_total`。

`global_completion(tasks, force=False) -> bool`：返回是否全局收敛。

---

### 4.8 main.py — 端到端装配演示

把上述模块**串联**为一个可运行演示，展示死锁检测 / 拓扑调度 / 验收 / 完成收敛 / 审批 / 持久化与恢复 / 消息总线 / 指标 / 审计。

**运行**：

```sh
# 框架装配 + 模拟执行（无需 SDK 运行时二进制）
python -m demo.main

# 真正调用 SDK 的 Agent（需要编译型运行时二进制 + GEMINI_API_KEY）
python -m demo.main --live
```

**场景** `build_scenario(root)`：

- 4 个 Agent：`cmd`（Commander，权限 `*`）、`a/b/c`（各权限限定自己的 task）。
- `task-a`：写 `a.txt`，验收「file exists + non-empty + file contains 'module A'」。
- `task-b`：依赖 `task-a` 的 `accepted`，写 `b.txt`。
- `task-c`：**可选**，依赖 `task-a` 的 `finished`，验收「file exists: never.txt」（模拟上游失败，验证可选任务失败不阻塞收敛）。

**演示流程**：

1. `orch.detect_deadlock()` / `orch.topo_order()`：死锁检测 + 拓扑序。
2. `TaskStore` 持久化 → 重新加载 → `recover_incomplete_runs`（幽灵任务恢复）。
3. `ApprovalManager`：`request` → 全局报告 blocked → `decide` approved → 任务回 PLANNED。
4. `MessageBus`：订阅、发布、去重、`reply` 关联。
5. `orch.arun(dispatch)`：异步并发调度（`simulate_dispatch` 模拟 Agent 执行 + 验收 + 复命）。
6. 逐任务 `evaluate_task_completed` + `global_report` 全局收敛。
7. `AuditLog` / `Metrics` 汇总。

---

### 4.9 a2a.py — A2A 协议层（跨网络）

> SDK status: **是**（官方 `a2a` Python SDK）。把本地执行器暴露为 A2A 1.0 服务并可经网络远程调用、复命，对应 link.md「四.第二层网络扩展 / 一.4 消息通信」。

实现第二层的**跨网络**扩展：用官方 `a2a` SDK 把本地 Agent 执行器包装为可被远程调用的 A2A agent，并经 `A2AClient` 远程调用其他 A2A agent。

#### `build_agent_card(name, description, url, *, ...) -> AgentCard`

构建真实 `AgentCard`（声明 JSONRPC 接口、协议版本 1.0、能力、输入/输出模式）。card 是 agent 的身份与能力契约，客户端据此发现如何连接与对话。

#### `A2ATaskExecutor`（AgentExecutor 实现）

把本地执行回调包装为 A2A `AgentExecutor`，按 A2A 协议流式输出 Task 生命周期：

- `execute(context, event_queue)`：先入队完整 `Task` 对象（SUBMITTED），再发 WORKING → artifact → COMPLETED/FAILED/CANCELED。**协议要求 Task 对象须先于任何 `TaskStatusUpdateEvent` 到达客户端**，否则触发 `InvalidAgentResponseError`。
- `context.task_id` / `context.context_id` 对新建任务已由 `RequestContext` 生成（`current_task` 为 None）。
- 把 A2A `context_id` 透传给执行回调——这是第二层协作的关键：执行器据此映射 SDK conversation 实现 LLM 记忆（见 4.4 `with_conversation`）。

#### `A2AServer`

用 `create_jsonrpc_routes` / `create_rest_routes` / `create_agent_card_routes` / `add_a2a_routes_to_fastapi` 把执行器暴露为 A2A 服务（JSONRPC + REST + `/.well-known/agent-card.json` 发现），uvicorn 后台线程启动。

- `start() -> str`：后台线程启动，返回服务 URL。
- `stop()`：优雅停止。

#### `A2AClient`

官方客户端封装，聚合流式 `message` / `artifact_update` / `status_update` / `task` 事件为单个 `A2AReply`。

- `connect(url)`：从 `/.well-known/agent-card.json` 解析 card 并建立连接。
- `send(prompt, *, context_id, task_id, timeout) -> A2AReply`：发送一条消息；传回上一轮的 `context_id` 可延续同一远程会话。`timeout` 透传为 `ClientCallContext`（真实 LLM 后端较慢，需长超时，否则 `A2AClientTimeoutError`）。
- `A2AReply`：`text / state / task_id / context_id / artifacts`。

#### `a2a_dispatch(client, task, *, sender, bus, audit, context_id, timeout) -> str`

作为 `Orchestrator` 的 dispatch 回调：经 A2A 远程执行任务，把结果回写 Task（复命 REPORT），可挂 `MessageBus` / 审计。

#### `make_sdk_executor(config, *, agent_id, conversation_save_dir) -> Callable[[str, str | None], Awaitable[str]]`

把 `agent_runtime.run_task` 包装成 A2A 执行回调，实现真实 SDK agent 的远程化。核心：把 A2A `context_id` 经 `agent_runtime.with_conversation` 映射为 SDK `conversation_id`，配合 `CREATE_OR_RESUME` + 固定 `save_dir` 让 LLM 真正续接同一会话（跨请求记忆）。`conversation_save_dir` 提供会话持久化（见 4.4）。

> 执行器回调签名统一为 `Callable[[str, str | None], ...]`：接收 prompt 与 A2A `context_id`。

---

### 4.10 a2a_antigravity_server.py — 独立 Antigravity A2A 服务

把 `agent_runtime.run_task`（经 llama.cpp 9000 的真实 LLM）用 `make_sdk_executor` 包装，经 `A2AServer` 暴露为一个**常驻** A2A 1.0 agent（card 名 `Antigravity-Assistant`，JSONRPC 于 `/`，协议 1.0，流式），供 `a2a_graph.py` 等客户端跨网络调用。与 `a2a_test.py` 的区别是**常驻服务**而非一次性测试。

**运行**：`python -m demo.a2a_antigravity_server --port 9998`（需 llama-server 在 9000 端口提供 OpenAI 兼容 `/v1`）。

- 自动从 `/v1/models` 发现模型；创建临时 `conversation_save_dir` 持久化会话（LLM 记忆）。
- 支持脚本与模块两种运行方式（含 sys.path 引导，见 8.14）。

---

### 4.11 a2a_graph.py — LangGraph 多 A2A Agent 组合

用 `langgraph.graph.StateGraph` 组合两个远程 A2A agent，构建**路由分发**图：

```
START → route → { friday | antigravity } → END
```

- **Friday**（AgentScope A2A 服务，默认 `http://127.0.0.1:9999`）：通用助手。
- **Antigravity-Assistant**（本仓库 `a2a_antigravity_server`，默认 `http://127.0.0.1:9998`）：技术/编程助手（后端 llama.cpp 9000）。

`route` 节点按关键词把 query 分派给合适 agent；各 agent 节点经 `A2AClient` 跨网络调用，各自保留 `context_id` 支持多轮记忆。仅一个服务在线时图自动退化为单 agent；支持 `--agent` 强制指定、`--query` 单条运行。

**运行**：

```sh
# 先启动两个服务
python -m demo.a2a_antigravity_server --port 9998
python <agentscope>/examples/a2a/server.py --port 9999
# 再运行图
python -m demo.a2a_graph
python -m demo.a2a_graph --query "帮我用 Python 写一个快速排序"
```

---

### 4.12 a2a_test.py — 真实 LLM 端到端测试

用 llama.cpp（端口 9000）的真实 LLM 测试 `a2a.py` 的全部核心路径。用 `build_agent_config(backend="openai", base_url="http://127.0.0.1:9000/v1", ...)` 构建指向 llama.cpp 的 `LocalOpenAIAgentConfig`，自动从 `/v1/models` 发现模型；用 `make_sdk_executor(config, conversation_save_dir=...)` 把真实 `run_task` 包装成 A2A 执行器。

**运行**：`python -m demo.a2a_test`（或直接 `python demo/a2a_test.py`）。

**测试覆盖（全部经真实 LLM）**：

| 测试 | 说明 |
| --- | --- |
| 1. 端到端往返 | 真实 LLM 回答经 A2A 协议回传 |
| 2. context_id 延续（协议层） | 同一远程会话两轮 context_id 一致 |
| 2b. LLM 会话记忆（真实续接） | 第二轮回复真实含第一轮告知的数字（会话被续接而非每次新建） |
| 3. Orchestrator 远程分派+复命 | 经 `a2a_dispatch` 远程执行，结果写回 Task（report_payload） |
| 3b. REPORT 复命入总线 | REPORT 消息发布到 MessageBus |
| 4. 流式事件聚合 | `state=COMPLETED` + artifact 非空 |

> 直接以脚本方式运行（`python demo/a2a_test.py`）需 sys.path 引导：把脚本目录 `demo/` 从 `sys.path` 移除，避免其遮蔽已安装的顶层 `a2a` SDK 包（见 8.14）。

---

## 5. canvas 子项目（IdeaCanvas）

> 一个多 Agent 协作的可视化画布 Web 应用。每个「蛋」（egg）是一个 Antigravity SDK Agent，拥有独立 worktree；Canvas 用框架层的 Orchestrator/Models/Completion/Messaging/Infrastructure 协调这些蛋。

### 5.1 config.py — 后端配置

单一位置选择 LLM 运行位置（镜像 antigravity-sdk examples/me/local_models.py）。**默认 `openai`**（本地 OpenAI 兼容服务器），无需 `.litertlm` 文件。

| 环境变量 | 说明 |
| --- | --- |
| `IDEA_BACKEND` | `openai`（默认）/`litert`/`cloud` |
| `OPENAI_BASE_URL` | 本地 OpenAI 服务器地址（默认 `http://localhost:9000/v1`） |
| `OPENAI_MODEL` | 模型名；未设时自动探测 `/v1/models` 第一个模型 |
| `GEMINI_API_KEY` / `GOOGLE_API_KEY` | cloud 后端密钥 |
| `GOOGLE_GENAI_USE_VERTEXAI` / `GOOGLE_CLOUD_PROJECT` / `GOOGLE_CLOUD_LOCATION` | Vertex 模式 |
| `ANTIGRAVITY_MODEL` | cloud 模型名 |
| `GEMMA4_MODEL_PATH` | litert 模型路径 |

**目录常量**：`WORKSPACE`、`WORKTREE_ROOT`（各蛋 worktree）、`SESSIONS_DIR`（每个 Canvas 会话 = 一个自包含日期目录，含 `canvas.json`、`agent_data/`、各蛋 worktrees/）。

**其他**：

- `quiet_sdk_start()`：抑制 SDK 启动失败时的冗长堆栈（`logging.disable`）。
- `is_corrupt_conversation_error(err)`：识别「损坏的会话」错误（HTTP 400 空 assistant 消息 / invalid_request / cannot resume / not found）。
- `compaction_config()`：按后端的压缩策略（保持小上下文窗口健康）。
- `backend_options()`：传给 `build_agent_config` 的后端特定 kwargs。
- `build_agent_config(...)`：兼容 shim，委托给 `demo.agent_runtime.build_agent_config`。

### 5.2 context.py — 版本化共享上下文（link.md 二.9）

> SDK 无跨 Agent 知识库（仅 StateStore + HookContext，作用域为单个 Agent 会话）。

`ContextEntry`（dataclass）：`task_id, text, version, source, updated_at, ttl`；`expired()` 判断 TTL。

`ContextStore`：内存、版本化、TTL 感知的共享上下文。

- 写：`put`（按 task 版本自增）、`drop`、`prune`（清理过期）。
- 读：`get`（过期则删）、`all`、`snapshot(exclude, include, max_chars)`（渲染带版本、大小上限的上下文块，按更新时间倒序）。
- 持久化：`to_dict`/`load_dict`/`clear`。

> 这是**共享/隔离边界**，不是向量数据库。

### 5.3 egg.py — EggAgent（一个蛋 = 一个 SDK Agent）

蛋持有**长生命周期 Agent 会话**（历史跨轮累积），通过 SDK 内置文件/命令工具向自己的 `worktree` 写代码。有模型连接时流式返回真实响应；无模型时**明确报告**而非假装完成。

**构造**：`EggAgent(id, title, prompt, type_, position, subagents, parent, session_dir, conversation_id, bus, store, state, metrics, audit, context)`。

- worktree：顶层蛋在 `session_dir/worktrees/<id>`；子蛋嵌套在父蛋目录下。自动创建并写 `README.md` 占位。
- `self._lock`（asyncio.Lock）：**按蛋**串行化模型回合（单蛋不并发两回合），但**不同蛋真正并行**。
- transcript：时间序对话轨迹（user/model/tool/system），供 `/api/trace/{egg_id}` 查看。

**SDK 生命周期**：

- `_make_hooks()`：`pre_tool_call_decide`（记录工具调用，`HookResult(allow=True)`）+ `post_tool_call`（记录结果）+ `AuditLog.install_sdk_hooks()`。
- `_config(resume)`：构造 system instructions（A/B 任务判定：A 类需代码任务必须真写文件并运行验证；B 类通用任务直接回复，不凭空造程序）+ `CapabilitiesConfig(enabled_tools=minimal(), enable_subagents=False, agent_behavior=MINIMAL)` + cloud 后端的 `RunCommandConfig(enable_sandbox, timeout_seconds)` + `session_continuation_mode` + `workspaces`/`app_data_dir`/`save_dir` + `policies` + 可选 `budget_config` + `subagents` + hooks。
- `start()` / `stop()` / `_ensure_started()`：懒连接；`conversation_id` 恢复；陈旧会话自动重建。

**SDK 原生内省**：

- `_hydrate_transcript_from_sdk()`：重启后把 SDK 持久化 step history 重放进 transcript（按 `step_index` 折叠到最后一次更新、去重工具调用）。
- `_usage_snapshot()`：`Conversation.last_turn_usage`/`total_usage`（token 用量）。
- `_stop_reason_value()`：SDK stop reason（`UNSPECIFIED` 视为无）。

**交互**：

- `run(text, context)`：流式执行命令。含**整轮安全超时** `_TURN_TIMEOUT`（默认 300s，`EGG_TURN_TIMEOUT`）+ **静默看门狗** `_STALL_TIMEOUT`（默认 120s，`EGG_STALL_TIMEOUT`）。看门狗是**服务端感知**的：静默期间并发轮询模型服务器 `/slots`，按服务端真实状态分类处置——`slow_prefill`（处理中且进度推进）延长等待（最多 `EGG_STALL_EXTENSIONS` 次）；`dead_stream`（服务端已空闲但无 chunk）/`zombie`（处理中但进度冻结 `EGG_ZOMBIE_TIMEOUT`）/`server_down`（不可达）立即中止；无 `/slots` 的服务器回退到纯静默超时。中止时还会尝试 `POST /abort` 释放服务端僵尸槽位。
- **重复工具调用循环防护**：同一签名工具调用 ≥4 次触发 `_loop_guard`，中止本轮。
- **空回合治愈**：既无文本又无结构化工具调用的回合会被 SDK 持久化为空 assistant 消息，导致后续请求被 HTTP 400 拒绝；检测后调用 `_heal_conversation` 丢弃会话重建。
- **补写提示**（write nudge）：文件任务只回文本未建文件时，要求同一会话补写交付物。
- `compress()`：用真实模型压缩会话为规格说明。
- `history_text(mode, limit)`：**只传 assistant 正文**给其他 Agent（思考/工具调用不外传）。
- `interrupt()` / `halt()`：先发 `halt_request` 再取消本地流任务。

**框架记账** `_record_task_run(reply_len, now)`：把每次运行镜像进 demo 框架（Task/TaskRun/artifacts/REPORT/DELIVER/BLOCKED/CANCEL），**强制执行完成判定规则**：仅执行结束 + 交付物有效 + 验收通过时才置 `report_submitted`；先做权限检查；发布 REPORT（require_ack）+ DELIVER/BLOCKED/CANCEL；更新指标与审计。

### 5.4 graph.py — Canvas（蛋集合 + 依赖边 + 总指挥）

一个 Canvas == 一个「想法项目」（`sessions/` 下的一个日期目录）。它拥有蛋、蛋之间的依赖边、以及协调运行的**总指挥（Commander）**。

**Commander**：协调蛋运行并叙述进度（SDK 无编排器，此为真实 SDK Agent 之上的协调器）。

**Canvas 构造**：`max_concurrency`（openai 后端默认 1，因单本地服务器串行化生成并会在并发流下重置连接；cloud/其他后端默认 2）、`max_retries`、`retry_backoff`、`_context`（ContextStore）。

**会话生命周期**：

- `_new_session()`：分配**同秒内唯一**的会话目录（避免两个 Canvas 共享目录混任务）。
- `_bind_session(sid, d, recover)`：绑定持久化存储（TaskStore/ApprovalManager/AuditLog/Metrics）并可选恢复状态。
- `list_sessions()`：列出已保存会话（新→旧）。
- `save()` / `load(sid)`：序列化 `canvas.json` + tasks/approvals。

**蛋管理**：

- `generate(goal)`：把目标分解为 DAG，每任务创建一个蛋。有模型时用**真实 SDK Agent** 请求 JSON 计划（`response_schema=PLAN_SCHEMA`）；无模型时用固定种子离线兜底。校验分解（`Orchestrator.decompose` + `topo_order`）。
- `add_egg` / `delete_egg` / `move` / `combine(ids)`（把多个子蛋产出综合为新蛋）。
- `add_edge` / `delete_edge`：依赖边（自环/重复防护）。

**上下文与共享**：

- `upstream_of(eid)`：所有祖先蛋（传递，容忍环）。
- `neighbor_context(eid)`：版本化共享上下文。DAG 中蛋**只看到上游邻居**（避免泄漏下游不存在的结果）；无边蛋保持自由共享。优先 ContextStore 快照，回退原始历史。
- `_peer_dirs(egg)`：本蛋**可读**的上游 + 平行节点工作目录（下游后代除外，避免泄漏尚未生成的结果）。
- `_peer_note(peers)`：把上述目录以**只读引用**的形式自动补进提示词，替代旧的 `shared/` 拷贝；上游证据已就绪，平行节点证据可能尚未生成。

**消息总线策略**：

- `_authorize_message(msg)`：commander/operator 恒允许；其他发送者需 `can_agent_modify_task`。
- `_on_message(msg)`：require_ack 时确认；REPORT/DELIVER/BLOCKED/CANCEL 记入 commander 轨迹。

**DAG 并发执行** `_run_graph(eggs, instruction_fn, ctx_fn)`：

- 用 `asyncio.Queue` 解耦 orchestrator 并发分发与单个 async generator，使 WebSocket 面板收到所有运行蛋的交错事件。
- `_build_orchestrator(ids, reset)`：构建带依赖边的 Orchestrator。
- `dispatch(task)`：权限检查 → 发布 `TASK_ASSIGN` → 挂载上游/平行节点目录（`egg.set_read_dirs` + `_peer_note`，只读）→ 运行蛋 → 同步回 task（状态/交付物/验收/复命/运行）→ 失败时让 orchestrator 应用重试策略。
- runner：`orch.arun(dispatch)`；取消时 `orch.cancel_all("interrupted")`。
- **全局收敛**：`completion.global_report`；`confirmed`（总指挥确认）；审批过期；输出收敛文本 + completion 事件 + 审计。

**其他**：

- `commander_run(text, targets)`：总指挥协调指定蛋。
- `run_selected(targets)`：一键按依赖顺序运行所选蛋。
- `interrupt_commander()`。
- `request_approval` / `decide_approval` / `approvals`。
- `completion_report()`：全局收敛报告。
- `runtime_view()` / `runtime_summary()`：运行时可观测性（任务/运行中蛋/指标/审批/上下文/并发）。
- `view()`：前端同步视图。

### 5.5 reasoning.py — 💭 推理流捕获

基于 SDK 标准流式 API（`ChatResponse.thoughts` 与 `types.Thought`）。蛋从标准流记录 Thought chunk；本模块提供 UI 开关 + 辅助函数。

- `set_reasoning_capture(on)` / `reasoning_enabled()` / `reasoning_capture_state()`。
- `thought_text(chunk)`：返回 Thought chunk 文本，否则 None（无 SDK 时鸭子类型回退）。
- `capture_reasoning(response)`：从标准 ChatResponse 产出推理增量（捕获关闭或无 thoughts 时 no-op）。

### 5.6 server.py — FastAPI 服务

REST + WebSocket 控制通道 + 审查代码的 REST 端点。

**REST 端点**：

| 端点 | 说明 |
| --- | --- |
| `GET /` | 前端 `index.html`（no-store） |
| `GET /api/version` | 前端构建戳（页面轮询，戳变化时自重载） |
| `GET /api/egg/{id}/files` | 列出蛋 worktree 文件 |
| `GET /api/egg/{id}/file?path=` | 读取蛋文件（路径逃逸防护） |
| `GET /api/trace/commander` | 总指挥时间序轨迹（**先于** `/api/trace/{egg_id}` 声明，避免捕获） |
| `GET /api/trace/{egg_id}` | 单蛋时间序轨迹 |
| `POST /api/interrupt?egg_id=` | 打断运行蛋的模型回合（HTTP 而非 WS，因 WS 读循环忙） |
| `GET /api/sessions` | 列出已保存会话 |
| `GET /api/completion` | 全局收敛报告 |
| `GET /api/tasks` | 每任务运行时记录 |
| `GET /api/metrics` | 指标快照 |
| `GET /api/runtime` | 一站式运行时视图 |
| `GET /api/approvals` | 所有审批（含过期） |
| `POST /api/approval/request?egg_id=&reason=` | 请求审批 |
| `POST /api/approval/decide?approval_id=&approved=&note=` | 决定审批 |

**WebSocket `/ws`**：控制通道。`_dispatch(msg, ws, canvas)` 处理消息类型：`sync / generate / addEgg / command / interrupt / listSessions / loadSession / newSession / compress / combine / deleteEgg / deleteEdge / addEdge / move / commander / run / setHistoryMode / completion / runtime / tasks / metrics / approvalRequest / approvalDecide / setReasoning`。

**事件流** `_send_event(ws, ev)`：把编排事件翻译为 WS 消息（`say/instruct/egg/plan/status/end_egg/framework/completion/end`）。

**`_run_commander_panel`**：在**独立任务**中运行总指挥回合/一键运行，流式发往客户端；停止按钮可打断而不断连。`TASKS` 集合持有 fire-and-forget 任务引用防止 GC。

### 5.7 run.py — 入口

```sh
python -m demo.canvas.run        # http://localhost:8000 （从仓库根）
python run.py                    # 也支持从该目录直接运行
GEMINI_API_KEY=... python run.py
```

把仓库根加入 `sys.path`，使 `demo.canvas` 无论直接运行还是作为模块运行都能解析；用 uvicorn 启动 `demo.canvas.server:app`（host 0.0.0.0，port 8000）。无可达模型时进入**离线模式**，仍服务 UI 并报告蛋未连接。

### 5.8 index.html — 前端单页应用

「Idea 蛋 画布（SDK 版）」。深色主题，画布舞台 + 可拖拽蛋卡片 + SVG 依赖边（可点击/选择/组合）。通过 WebSocket `/ws` 与后端通信，轮询 `/api/version` 自动重载。提供目标输入、生成、运行、打断、压缩、组合、审批、运行时/指标/完成面板、审阅代码（worktree 文件树 + 内容）等功能。

---

## 6. SDK 逐项对应关系

对应 `link.md` 第六节。原则：**SDK 能用到的地方直接用，没有的地方占位**。

| 模块 | 能力 | SDK status | 说明 |
| --- | --- | --- | --- |
| models | Agent/Task/TaskRun/Dependency/Artifact/Acceptance | 否 | SDK 仅 `Agent` 存在（agent.py:34） |
| orchestrator | 任务编排、依赖/死锁 | 否 | 无编排器；拓扑排序+环检测为纯 Python |
| messaging | 消息通信与复命协议 | 否 | 无结构化消息类型 |
| agent_runtime | Agent 执行与协作 | **是** | Agent/LocalAgentConfig、SubagentConfig、policy、hooks、RetryConfig、response_schema、conversation_id、otel、AskQuestionHook 直接用 |
| infrastructure | 全局基础设施 | 混用 | RetryConfig/SessionContinuationMode/policy/otel 直接用；任务状态机/TaskStore/任务级恢复/任务级鉴权/审计事件日志/指标计数器为占位 |
| acceptance | 交付物验收 | 否 | 占位（stdlib 文件校验 + 结构化输出近似） |
| completion | 完成判定规则 | 部分 | ExecutionFinished 用 SDK StepStatus 近似，其余占位 |
| main | 总体装配 | — | 串联以上模块 |
| a2a | A2A 协议层（跨网络） | **是**（官方 a2a SDK） | AgentExecutor/DefaultRequestHandler/InMemoryTaskStore、create_jsonrpc_routes/create_rest_routes/create_agent_card_routes/add_a2a_routes_to_fastapi、ClientFactory/ClientConfig/BaseClient、A2ACardResolver、AgentCard/AgentInterface/Message/Part/Task/TaskState |

**SDK 直接复用的关键原语**：

- `Agent` / `LocalAgentConfig` / `LiteRTAgentConfig` / `LocalOpenAIAgentConfig`
- `SubagentConfig` / `START_SUBAGENT`（多 Agent 协作，LLM 驱动）
- `policy.allow/deny/ask_user/deny_all/allow_all/workspace_only`
- `hooks.pre_tool_call_decide` / `hooks.post_tool_call` / `HookResult`
- `RetryConfig` / `ModelAPIRetryConfig` / `ModelOutputRetryConfig`
- `CapabilitiesConfig` / `BuiltinTools.minimal()` / `RunCommandConfig` / `BudgetConfig` / `CompactionConfig` / `SessionContinuationMode`
- `response_schema`（结构化输出校验）
- `conversation_id` / `save_dir` / `app_data_dir`（会话持久化与恢复）
- `utils.otel.get_otel_hooks` / `UsageMetadata`
- `interactive.AskQuestionHook`（人工审批）
- 官方 `a2a` SDK：`AgentExecutor` / `DefaultRequestHandler` / `InMemoryTaskStore` / `TaskUpdater` / `RequestContext` / `EventQueue`；`create_jsonrpc_routes` / `create_rest_routes` / `create_agent_card_routes` / `add_a2a_routes_to_fastapi`；`ClientFactory` / `ClientConfig` / `BaseClient` / `ClientCallContext` / `A2ACardResolver`；`AgentCard` / `AgentInterface` / `AgentCapabilities` / `AgentSkill` / `Message` / `Part` / `Role` / `Task` / `TaskStatus` / `TaskState` / `TaskStatusUpdateEvent` / `TaskArtifactUpdateEvent` / `SendMessageRequest`

---

## 7. 运行方式

### 依赖

- Python ≥ 3.10
- 框架层（`demo.main`）：**无需 SDK 运行时二进制**（模拟执行）。
- 真实 SDK 运行（`--live` / canvas 真实模型）：需 `pip install google-antigravity`（含编译型运行时二进制）+ `GEMINI_API_KEY`。
- canvas Web 应用：`uvicorn`、`fastapi`、`websockets`（SDK 依赖已含）；前端为纯 HTML/JS（`index.html`）。

### 框架演示

```sh
# 框架装配 + 模拟执行（无需 SDK 运行时二进制）
python -m demo.main

# 真正调用 SDK 的 Agent（需要编译型运行时二进制 + GEMINI_API_KEY）
python -m demo.main --live
```

### A2A 服务与多 Agent 组合

需要 llama.cpp（或任意 OpenAI 兼容服务器）在 9000 端口提供 `/v1` 接口。

```sh
# 1) 独立 Antigravity A2A 服务（常驻，默认 :9998）
python -m demo.a2a_antigravity_server --port 9998

# 2) 真实 LLM 端到端测试 a2a.py（一次性）
python -m demo.a2a_test

# 3) LangGraph 组合两个远程 A2A agent（需先启动 Friday 等另一服务）
python -m demo.a2a_graph
python -m demo.a2a_graph --query "帮我用 Python 写一个快速排序"
python -m demo.a2a_graph --agent friday --query "你好"
```

> A2A 客户端对真实 LLM 后端需长超时：`A2AClient.send(..., timeout=...)` 与 `a2a_dispatch(..., timeout=...)` 透传 `ClientCallContext(timeout=...)`。

### IdeaCanvas 画布应用

```sh
# 默认 openai 后端（本地 OpenAI 兼容服务器，默认 :9000）
python -m demo.canvas.run
# 或
python demo/canvas/run.py

# cloud 后端（Google Antigravity 云）
GEMINI_API_KEY=your_key python -m demo.canvas.run

# litert 后端（端侧模型，需先 litert-lm import）
IDEA_BACKEND=litert GEMMA4_MODEL_PATH=~/.litert-lm/models/gemma4-26b/model.litertlm python -m demo.canvas.run
```

启动后访问 `http://localhost:8000`。无可达模型时进入离线模式，UI 仍可用但蛋报告未连接。

### 常用环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `IDEA_BACKEND` | `openai` | `openai`/`litert`/`cloud` |
| `OPENAI_BASE_URL` | `http://localhost:9000/v1` | 本地 OpenAI 服务器 |
| `OPENAI_MODEL` | 自动探测 | 模型名 |
| `GEMINI_API_KEY` | — | cloud 密钥 |
| `CANVAS_MAX_CONCURRENCY` | openai→1, 其他→2 | 蛋并发上限 |
| `CANVAS_MAX_RETRIES` | 1 | 任务重试上限 |
| `CANVAS_RETRY_BACKOFF` | 0.5 | 重试退避 |
| `CANVAS_CONTEXT_TTL` | None | 共享上下文 TTL |
| `CANVAS_PLAN_TIMEOUT` | 60 | 规划超时 |
| `EGG_TURN_TIMEOUT` | 300 | 单回合安全超时 |
| `EGG_STALL_TIMEOUT` | 120 | 静默看门狗 |
| `EGG_STALL_PROBE_INTERVAL` | 15 | `/slots` 进度采样间隔（秒） |
| `EGG_ZOMBIE_TIMEOUT` | 90 | 处理中但进度冻结超过此值判为僵尸请求 |
| `EGG_STALL_EXTENSIONS` | 2 | slow_prefill 时静默看门狗的最大延长次数 |
| `EGG_COMMAND_TIMEOUT` | 600 | cloud 命令超时 |
| `EGG_MAX_TOTAL_TOKENS` / `EGG_MAX_TOOL_CALLS` / `EGG_MAX_MODEL_CALLS` | None | 资源上限（opt-in） |

### 接入讯飞星火 MaaS（OpenAI 兼容）

SDK 的 OpenAI 后端（`LocalOpenAIAgentConfig` / proto `GemmaEndpoint`）只接受 `base_url`，**没有 api_key 字段**，harness 也不会为 OpenAI 后端读取鉴权环境变量，因此无法直接给 MaaS 这类需要 `Authorization: Bearer` 的服务加鉴权头。解决：`demo/maas_proxy.py` 本地代理。

```bash
# 1. 启动代理（注入 Bearer 鉴权、固定模型名、透传 SSE 流）
MAAS_API_KEY="ak-..." MAAS_MODEL="spark-x2.6" python3 demo/maas_proxy.py
#   默认监听 127.0.0.1:9000，转发 https://maas-api.cn-huabei-1.xf-yun.com/v2/chat/completions

# 2. demo 侧零改动，仅需环境变量
IDEA_BACKEND=openai OPENAI_BASE_URL=http://localhost:9000/v1 OPENAI_MODEL=spark-x2.6 python3 demo/server.py
```

代理端点：`POST /v1/chat/completions`（鉴权转发+流式透传）、`GET /v1/models`（SDK 模型自动探测）、`GET /slots`（模拟在途请求进度，配合 egg 的服务端感知看门狗）、`GET /health`。上游 400 时自动剥离 `stream_options` 重试一次；客户端中途断开（看门狗中止/用户停止）优雅退出不报错。

---

## 8. 关键设计决策与注意事项

1. **任务状态与交付物状态分离**（link.md 三.1）：任务执行成功但交付物验收失败，任务不算最终完成。由 `StateManager` 的两套迁移表 + `completion` 的四条件公式保证。

2. **Agent ≠ 任务**（link.md 三.3）：Agent 在线不代表其任务在执行；Agent 进程退出也不代表任务失败。故有 `AgentRunStatus` / `TaskStatus` / `ExecutionStatus` 三套状态。

3. **依赖关系与控制关系区分**（link.md 三.2）：依赖条件 `finished/accepted/reported/completed` 语义明确；上游回到 `PLANNED`（重试中）时 `finished` **不满足**，避免下游与半途上游并行。

4. **死锁检测**：`detect_deadlock`（DFS 三色）+ `topo_order`（有环抛 `DeadlockError`）+ 调度结束 `_check_stuck`（剩余不可调度任务报告等待依赖）。

5. **完成判定公式**：`TaskCompleted = ExecutionFinished ∧ ArtifactsValid ∧ AcceptancePassed ∧ ReportSubmitted`，由 `completion.evaluate_task_completed` 强制执行；`report_submitted` 仅在验收通过后置位（见 `egg._record_task_run` / `main.simulate_dispatch`）。

6. **权限控制**：SDK policy 管工具，`can_agent_modify_task` 管任务归属（agent 只能改授权的任务/文件）；`MessageBus.authorize` 管消息写入（避免多 Agent 同时写冲突）。

7. **工作树隔离**（link.md 一.6）：每个蛋独立 worktree；上游交付物复制到 `shared/` 设为只读，下游不能「修正」上游副本而静默丢失变更。

8. **持久化与恢复**：`TaskStore`（JSON 原子写）+ `recover_incomplete_runs`（挂起运行标记 `PROCESS_LOST`，任务回 `PLANNED`）；SDK `conversation_id` + `session_continuation_mode` 负责 LLM 会话恢复。

9. **会话治愈**：空回合被 SDK 持久化为空 assistant 消息会导致后续请求 HTTP 400；`egg._heal_conversation` 检测并丢弃会话重建。

10. **防重复**：`MessageBus` 按 `msg_id` 去重；`completion`/`acceptance` 结果缓存避免重复执行；`TaskStore` 原子写避免半写。

11. **并发模型**：`Orchestrator.arun`（asyncio）适配 SDK 异步接口；蛋级 `_lock` 串行化单蛋模型回合，不同蛋并行；canvas openai 后端默认并发 1（本地服务器串行化生成 + 并发流下重置连接）。

12. **可观测性**：`Metrics`（计数/gauge/直方图）+ `AuditLog`（append-only + run_id 关联 + JSONL sink）+ `runtime_view`/`runtime_summary`（运行时视图：哪个蛋卡住、哪个任务等待、为何未结束）。

13. **人工审批**：`ApprovalManager` 是独立流程实体（谁发起/谁审批/拒绝怎么办/超时如何处理），任务转 `WAITING_APPROVAL`，超时转 `BLOCKED`。

14. **sys.path 修正**（`demo/__init__.py`）：仓库源码树缺少安装时生成的 `*_pb2` 模块，会让 `import google.antigravity` 解析到源码树而失败；`_prefer_installed_sdk` 把仓库根移到 `sys.path` 后部，让 pip 安装的 SDK 优先。

15. **A2A context_id → SDK conversation_id 映射（LLM 记忆）**：A2A 的协议层 `context_id` 延续不等于 LLM 记忆——每次 `run_task` 新建的 Agent 子进程会丢失会话状态。`agent_runtime.with_conversation` 把 A2A `context_id` 映射为 SDK `conversation_id`，配合 `CREATE_OR_RESUME` + 固定 `save_dir`（会话落盘到 `<save_dir>/<conversation_id>.db`），让 LLM 跨 A2A 请求真正续接同一会话。`make_sdk_executor(config, conversation_save_dir=...)` 是其接入点。

16. **`demo/a2a.py` 同名遮蔽**：直接以脚本方式运行（`python demo/a2a_test.py`）时，Python 把脚本目录 `demo/` 放入 `sys.path`，而 `demo/` 内有一个 `a2a.py`，会**遮蔽已安装的顶层 `a2a` SDK 包**（同名）。于是 `import a2a` 解析到 `demo/a2a.py`，它作为顶层模块再做相对导入即报「无父包」。修复：脚本引导时把脚本目录 `demo/` 从 `sys.path` 移除，只保留 repo_root（见 `a2a_test.py` / `a2a_antigravity_server.py` / `a2a_graph.py` 顶部引导）。

---

## 附：模块依赖关系

```
main.py
 ├─ orchestrator.py ─── models.py
 ├─ agent_runtime.py ── models.py ── (google.antigravity SDK)
 ├─ acceptance.py ───── models.py
 ├─ completion.py ───── models.py ── acceptance.py
 ├─ infrastructure.py ─ models.py ── (google.antigravity SDK)
 └─ messaging.py ────── models.py（仅类型引用）

a2a.py ────────────── agent_runtime.run_task/with_conversation + models
             + (官方 a2a SDK)
a2a_antigravity_server.py ─ a2a.A2AServer + make_sdk_executor + agent_runtime
a2a_graph.py ───────────── a2a.A2AClient + (langgraph)
a2a_test.py ────────────── a2a.A2AServer/A2AClient/a2a_dispatch + agent_runtime + orchestrator/messaging/models

canvas/
 ├─ config.py ───────── agent_runtime.build_agent_config
 ├─ context.py ───────── (独立)
 ├─ egg.py ──────────── models/acceptance/completion/messaging/infrastructure
 │               + agent_runtime + reasoning + (google.antigravity SDK)
 ├─ graph.py ────────── egg.EggAgent + 上述框架模块
 ├─ reasoning.py ─────── (google.antigravity types)
 ├─ server.py ───────── graph.Canvas
 └─ run.py ───────────── server.app（uvicorn）
```

---

*文档基于 `demo/` 目录当前源码撰写。若源码更新，请同步核对各模块 docstring 中的「SDK status」标注与 `link.md` 第六节对应关系。*
