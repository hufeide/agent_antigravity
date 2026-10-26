

## 一、现有设计中最值得补充的模块

1\. 任务编排与调度管理

> **SDK 对应：** 否（SDK 无任务拆分/DAG/编排器；仅有 LLM 驱动的 SubagentConfig types.py:292、START_SUBAGENT types.py:351，由外部 harness 协调，非 SDK 编排框架）


优先级最高

你现在有生命周期和依赖，但还没有明确表达谁负责拆分任务、谁决定执行顺序，以及哪些任务可以并行。

- 总指挥拆分子任务，形成任务树或 DAG。
- 判断任务能否并行执行，以及依赖满足后何时启动。
- 控制并发数量、优先级、超时和资源分配。
- 支持动态追加任务、取消任务和重新规划。

2\. 交付物验收与完成判定

> **SDK 对应：** 否（SDK 不校验交付物；仅 AgentConfig.response_schema connection.py:97 / CapabilitiesConfig.finish_tool_schema_json types.py:546 可校验 Agent 结构化输出）


优先级最高

这是你现有结构中最需要明确的一点：Agent 声称完成，不等于任务真正完成。

- 是否产生了要求的文件？
- 文件是否存在、非空、可读取，且格式正确？
- 是否满足任务规定的验收标准？
- 子 Agent 是否提交交付物路径、执行结果和验收证据？
- 验收通过后，是否才允许向总指挥报告完成？

建议将“交付物已生成”“交付物已验收”“子任务已复命”“总指挥已确认”设计为不同状态。

3\. 执行监督与故障恢复

> **SDK 对应：** 部分（重试 RetryConfig types.py:679 + ModelAPIRetryConfig types.py:651；取消 Connection.cancel connection.py:577；失败类型 StepStatus types.py:1034；崩溃恢复 SessionContinuationMode types.py:1049；去重 _StepTracker event_processor.py:111）


需要区分正常失败、超时、异常退出、主动取消和进程失联。

- 超时检测与取消传播。
- 失败重试、退避策略、最大重试次数。
- 取消时也必须更新终态，避免调度器无限等待。
- 崩溃后恢复未完成任务，避免重复执行已完成任务。
- 防止重复写入交付物或重复提交完成事件。

4\. 消息通信与复命协议

> **SDK 对应：** 否（SDK 无 AgentMessage 类型；Step.trajectory_id/parent_trajectory_id types.py:1136 仅作子 Agent 轨迹记账）


Agent 之间不仅需要依赖关系，还需要统一的通信协议。

- 任务分配、进度汇报、阻塞通知、交付提交。
- 结构化消息包含 `task_id`、`agent_id`、`status`、`artifacts` 和 `error`。
- 明确谁有权修改任务状态，避免多个 Agent 同时写入产生冲突。
- 支持消息去重、关联追踪、确认回执和超时处理。

5\. 持久化与恢复管理

> **SDK 对应：** 部分（会话持久化 conversation_id agent.py:204 / SessionContinuationMode types.py:1044；状态历史 Step/StepStatus/Step.error conversation.py:219；无任务级关联存储）


如果当前状态只存在内存中，程序重启后可能丢失任务进度。

- 持久化任务、Agent 运行记录、依赖关系和交付物清单。
- 记录状态变更历史和失败原因。
- 重启后恢复任务，而不是重新执行全部流程。
- 对运行记录、交付物版本和状态变更进行关联。

6\. 权限、安全与资源隔离

> **SDK 对应：** 部分（policy.allow/deny/ask_user policy.py；BuiltinTools.read_only types.py:359；enable_sandbox/SandboxStatus types.py:224/855；无 worktree 隔离）


多个 Agent 协作时，需要防止越权和互相干扰。

- Agent 只能修改授权的任务和文件。
- 限制 Shell、文件系统、网络等工具的访问权限。
- 为不同 Agent 隔离工作目录或使用独立 worktree。
- 控制 Token、GPU、CPU、内存和执行时间。
- 对高风险操作设置人工审批。

## 二、从架构完整性看，还缺少哪些机制？

除了上面的核心模块，我建议继续补充以下部分。这些模块往往在系统刚开始运行时不明显，但 Agent 数量增加后就会成为主要问题。

7\. 依赖解析与死锁检测

> **SDK 对应：** 否（SDK 无依赖图；仅 harness 侧死锁预防 CHANGELOG.md:322）


你已经设计了依赖管理，但还需要考虑循环依赖、依赖任务失败、依赖条件不满足，以及依赖被取消的情况。

例如 A 等待 B、B 等待 C、C 又等待 A。系统应识别循环并报告，而不是一直等待。

8\. 审计日志与事件追踪

> **SDK 对应：** 部分（工具调用审计用 hooks.post_tool_call hooks.py:411 + utils/otel.py；无独立事件/状态变更日志）


记录每次任务分配、状态变化、工具调用、交付物提交、验收和复命。每个事件都关联任务 ID 和执行 ID，以便还原问题发生的全过程。

9\. 上下文与共享知识管理

> **SDK 对应：** 部分（StateStore utils/state.py:28 + HookContext/ToolContext；无独立知识库）


Agent 之间需要明确哪些信息共享、哪些信息隔离，以及如何向新启动的 Agent 传递必要上下文。

应考虑上下文版本、摘要、消息引用、历史记录和信息过期，避免 Agent 因为上下文不一致而重复工作或产生矛盾。

10\. 人工介入与审批管理

> **SDK 对应：** 是（policy.ask_user policy.py:375 + AskQuestionHook + examples/getting_started/human_in_the_loop.py）


你图中有“需要批准”状态，但还缺少完整的审批流程。

谁发起审批、由谁审批、审批哪些内容、拒绝后怎么办、审批超时如何处理，都需要明确。审批也应该作为独立的流程实体，而不只是一个状态标签。

11\. 监控与运行可观测性

> **SDK 对应：** 部分（utils/otel.py + UsageMetadata types.py:869 + Conversation.total_usage conversation.py:306；无 tokens/步数等指标计数器）


需要监控正在运行的 Agent、任务耗时、队列长度、失败率、重试次数、Token 消耗、资源占用和阻塞原因。

同时提供运行时视图，让你可以看出哪个 Agent 卡住、哪个任务正在等待，以及整个工作流为什么尚未结束。

12\. 全局终止与完成收敛

> **SDK 对应：** 部分（StopHook hooks.py:239 按轮闸门 + BudgetConfig types.py:590；跨 Agent 收敛检测 否）


当所有子任务结束时，总指挥如何判断整个项目真正结束？

需要定义必需任务是否全部通过、可选任务是否允许失败、是否还有运行中的子任务，以及是否存在尚未验收或尚未复命的交付物。

## 三、你这张图本身还有三个值得调整的地方

### 1. 把“管理模块”和“状态值”分开

> **SDK 对应：** 状态管理器 否（无任务状态机，仅 HookRunner hook_runner.py:43）；任务状态 部分（StepStatus types.py:1034，Agent 步骤级）；交付物状态 否


目前“状态管理”既作为深蓝色模块出现，又在 Agent 下方以浅蓝色节点出现，容易让人分不清这是同一个模块，还是某个任务的状态。

建议拆成三层：

- 状态管理器：负责状态转换、合法性检查和并发更新。
- 任务状态：计划中、运行中、等待审批、成功、失败、取消。
- 交付物状态：未生成、已生成、验收中、验收通过、验收失败。

这三类状态不能完全混为一谈。例如任务执行成功，但交付物验收失败，任务就不能直接算作最终完成。

### 2. 把依赖关系和控制关系区分开

> **SDK 对应：** 否（SDK 无调用/依赖/数据/状态/验收/复命 等关系模型）


图中的虚线同时连接生命周期、交付物、状态、Agent 和依赖管理，建议明确不同连线的语义。

| 关系类型 | 表示什么     |
| ---- | -------- |
| 调用关系 | 谁负责调用谁   |
| 依赖关系 | 谁必须等待谁   |
| 数据关系 | 谁读取或生成什么 |
| 状态事件 | 谁触发状态变化  |
| 验收关系 | 谁检查谁的交付物 |
| 复命关系 | 谁向谁报告完成  |

否则，随着模块增多，连线会越来越复杂，而且无法直接据此生成可靠的程序代码。

### 3. 明确 Agent 与任务不是一回事

> **SDK 对应：** Agent 是（Agent agent.py:34 / AgentConfig connection.py:69 / CapabilitiesConfig types.py:478）；Task/TaskRun/Dependency/Artifact/Acceptance 均否（SDK 中无这些类型）


一个 Agent 可以执行多个任务，一个任务也可能经过多个 Agent 处理。因此，建议单独建立：

- `Agent`：身份、能力、权限、运行状态。
- `Task`：任务内容、负责人、优先级、执行状态。
- `TaskRun`：某次具体执行的记录。
- `Dependency`：任务之间的依赖条件。
- `Artifact`：任务产生的文件或其他交付物。
- `Acceptance`：验收规则、验收结果和证据。

这样能够避免把 Agent 的运行状态误当成任务状态。例如，Agent 仍然在线，不代表它负责的任务还在执行；Agent 进程退出，也不一定代表任务失败。

## 四、建议最终采用的整体架构

下面是一个适合你这个多 Agent 系统的分层方案。

## 第一层：总指挥与任务编排

> **SDK 对应：** 否（SDK 无编排器；仅单 Agent + LLM 驱动子 Agent）


任务拆分 · 动态调度 · 依赖解析 · 全局完成判定

第二层：Agent 执行与协作

> **SDK 对应：** 部分（Agent agent.py:34；多 Agent 协作 SubagentConfig types.py:292 / START_SUBAGENT types.py:351，LLM 驱动）


Agent A

Agent B

Agent C

Agent N

任务执行 · 消息通信 · 工具调用 · 工作目录隔离

第三层：任务结果与交付闭环

> **SDK 对应：** 否（无交付物/验收/复命）


交付物生成

自动验收

子任务复命

总指挥确认

第四层：全局基础设施

> **SDK 对应：** 生命周期 是（HookRunner + hooks）；持久化 是（conversation_id）；重试恢复 是（RetryConfig）；权限 是（policy）；审计 部分；监控 部分


生命周期与状态机 · 持久化 · 重试与恢复 · 权限控制 · 审计日志 · 监控告警

需要注意：第四层的基础设施并非只能位于底部，它们实际上会横跨前三层，为所有模块提供服务。

## 五、最关键的完成判定规则

> **SDK 对应：** ExecutionFinished 部分（StepStatus types.py:1034）；ArtifactsValid 否；AcceptancePassed 否；ReportSubmitted 否；response_schema 可作结果校验近似


我建议把整个系统的最终完成条件定义为：

\\[ \begin{aligned} \text{TaskCompleted} ={}& \text{ExecutionFinished}\\\ &\land\text{ArtifactsValid}\\\ &\land\text{AcceptancePassed}\\\ &\land\text{ReportSubmitted} \end{aligned} \\]

这里的意思是：任务执行已经结束、要求的交付物通过验证、验收成功，并且子 Agent 已经提交复命，才能将子任务判定为完成。

如果任务不要求交付文件，可以将 `ArtifactsValid` 替换为适用的结果验证条件，而不是强制所有任务都必须生成文件。

总指挥还需要单独执行全局验收：所有必需子任务通过、没有未处理的失败或阻塞，并且整个工作流满足结束条件，才可以宣布整个项目完成。


---

