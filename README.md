# demo/ — 实现 link.md 的多 Agent 编排框架

本目录把 link.md 描述的设计落地为一个可运行的框架骨架。
原则：**SDK 能用到的地方直接用，没有的地方占位**。逐条对应关系见 link.md 第六节。

## 运行

```sh
# 框架装配 + 模拟执行（无需 SDK 运行时二进制）
python -m demo.main

# 真正调用 SDK 的 Agent（需要编译型运行时二进制 + GEMINI_API_KEY）
python -m demo.main --live
```

## 模块与 link.md 的对应

| 文件 | link.md 章节 | SDK 现状 |
| --- | --- | --- |
| `models.py` | 三.3（Agent/Task/TaskRun/Dependency/Artifact/Acceptance） | 否：这些类型 SDK 均不存在，仅 `Agent` 存在(agent.py:34) |
| `orchestrator.py` | 一.1 任务编排、二.7 依赖/死锁 | 否：无编排器；拓扑排序+环检测是纯 Python 实现 |
| `messaging.py` | 一.4 消息通信与复命协议 | 否：无结构化消息类型 |
| `agent_runtime.py` | 四.第二层 Agent 执行与协作 | **直接用 SDK**：Agent/LocalAgentConfig、SubagentConfig、policy(allow/deny/ask_user)、hooks、RetryConfig、response_schema、conversation_id、otel、AskQuestionHook |
| `infrastructure.py` | 四.第四层 全局基础设施 | 混用：RetryConfig/SessionContinuationMode/policy/otel 直接用；任务状态机、TaskStore、任务级恢复、任务级鉴权、审计事件日志、指标计数器为占位 |
| `acceptance.py` | 一.2 交付物验收 | 否：占位（stdlib 文件校验 + 结构化输出近似） |
| `completion.py` | 五 完成判定规则 | 部分：ExecutionFinished 用 SDK StepStatus 近似，其余占位 |
| `main.py` | 总体装配 | 串联以上模块，演示死锁检测/拓扑调度/验收/完成收敛 |

## 关键结论

`google.antigravity` 是**单 Agent** 基础设施；link.md 的**多 Agent 编排**（任务 DAG、编排器、交付物验收、消息协议、依赖死锁、全局收敛）SDK 均不直接提供。
详见 `link.md` 第六节「SDK 逐项对应关系」。
