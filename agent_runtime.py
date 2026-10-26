"""Agent runtime layer (link.md section 四.第二层: Agent 执行与协作).

This module uses the REAL SDK (google.antigravity) wherever it can:

  * Agent / LocalAgentConfig ................ agent.py:34, local_connection_config.py:204
  * Sub-agents (multi-agent collaboration) .. types.SubagentConfig:292, START_SUBAGENT:351
  * Security policy (permissions) ........... policy.allow/deny/ask_user: policy.py:272/319/375
  * Hooks / lifecycle interception .......... hooks.post_tool_call: hooks.py:411
  * Retry / backoff (fault recovery) ........ types.RetryConfig:679, ModelAPIRetryConfig:651
  * Structured output (result validation) ... AgentConfig.response_schema: connection.py:97
  * Persistence / resume .................... conversation_id / SessionContinuationMode
  * Observability ........................... utils.otel.get_otel_hooks, UsageMetadata
  * Human approval for risky ops ............ policy.ask_user + interactive.AskQuestionHook

The SDK import is guarded: the compiled runtime binary is required at runtime
(see README "Installation"), so importing this module for inspection works even
without the binary, but actually running an agent requires it.

Anything the SDK does NOT provide (task dispatch, artifact acceptance, reporting
protocol) is delegated to the placeholder framework modules and marked clearly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Sequence

from . import models

logger = logging.getLogger(__name__)

# --- Guarded SDK import (binary not required for import-time inspection) -----
try:
    from google.antigravity import Agent, LocalAgentConfig
    from google.antigravity.hooks import hooks, policy
    from google.antigravity.types import (
        CapabilitiesConfig,
        ModelAPIRetryConfig,
        ModelOutputRetryConfig,
        RetryConfig,
        SubagentConfig,
        SystemInstructions,
    )
    from google.antigravity.utils import interactive
    from google.antigravity.utils.otel import get_otel_hooks

    SDK_AVAILABLE = True
except Exception as _err:  # pragma: no cover - environment dependent
    SDK_AVAILABLE = False
    _SDK_ERROR = _err
    logger.warning("google.antigravity SDK not fully importable: %s", _err)


def _audit_hook_collector() -> list[dict[str, Any]]:
    """Build a post_tool_call audit hook and return the collected log.

    SDK: hooks.post_tool_call (google/antigravity/hooks/hooks.py:411). This is a
    real SDK hook; the log it writes is our placeholder audit store (link.md
    notes the SDK has tool-call hooks but no dedicated audit event log).
    """
    log: list[dict[str, Any]] = []

    if SDK_AVAILABLE:

        @hooks.post_tool_call
        async def _audit(data: Any) -> None:
            entry = {
                "tool": getattr(data, "name", None),
                "args": getattr(data, "args", None),
                "result": getattr(data, "result", None),
            }
            log.append(entry)
            logger.info("[audit] tool=%s", entry["tool"])

    return log


def build_agent_config(
    system_instructions: str,
    *,
    tools: Sequence[Any] = (),
    subagents: Sequence["SubagentConfig"] = (),
    allowed_tools: Sequence[str] | None = None,
    denied_tools: Sequence[str] | None = None,
    ask_user_tools: Sequence[str] = (),
    max_retries: int = 3,
    enable_sandbox: bool = True,
    response_schema: Any = None,
    conversation_id: str | None = None,
    session_continuation_mode: Any = None,
    hooks: Sequence[Any] = (),
    enable_otel: bool = True,
    api_key: str | None = None,
    backend: str = "cloud",
    model: str | None = None,
    base_url: str | None = None,
    model_path: str | None = None,
    vertex: bool | None = None,
    project: str | None = None,
    location: str | None = None,
    workspaces: Sequence[str] | None = None,
    app_data_dir: str | None = None,
    save_dir: str | None = None,
    hook_runner: Any = None,
    capabilities: Any = None,
    policies: Sequence[Any] | None = None,
    compaction_config: Any = None,
    budget_config: Any = None,
) -> Any:
    """Construct a real SDK AgentConfig.

    backend picks the concrete config class:
      * "cloud" (default) -> LocalAgentConfig (Google Antigravity cloud)
      * "openai"          -> LocalOpenAIAgentConfig (Ollama / LM Studio / vLLM)
      * "litert"          -> LiteRTAgentConfig (on-device .litertlm)

    Every option corresponds to a real SDK knob. Callers that only need the
    cloud default (e.g. demo.main) can ignore the backend-specific arguments;
    the canvas passes them to run against a local model.
    """
    if not SDK_AVAILABLE:
        raise RuntimeError(f"SDK unavailable: {_SDK_ERROR}")

    # --- Permissions / security policy (link.md 一.6) -----------------------
    # Default-deny write tools, allow read, ask_user for risky tools. An explicit
    # policies list (e.g. the canvas allow_all) replaces the default policy.
    if policies is None:
        policies = [policy.deny_all()]
        if allowed_tools:
            for t in allowed_tools:
                policies.append(policy.allow(t))
        else:
            policies.append(policy.allow("view_file"))
        for t in denied_tools or []:
            policies.append(policy.deny(t))
        for t in ask_user_tools:
            policies.append(policy.ask_user(t))

    # --- Fault recovery: retry / backoff (link.md 一.3) ---------------------
    retry = RetryConfig(
        model_api=ModelAPIRetryConfig(max_retries=max_retries),
        model_output=ModelOutputRetryConfig(max_retries=1),
    )

    # --- Capabilities: sandbox, sub-agents, structured output ---------------
    caps = capabilities
    if caps is None:
        caps = CapabilitiesConfig(enable_sandbox=enable_sandbox)
    if response_schema is not None:
        # types.CapabilitiesConfig.finish_tool_schema_json is a JSON *string*
        # (types.py:546); handing the raw dict through reaches the protobuf
        # layer as a dict and fails with "bad argument type for built-in
        # operation" when the harness config is built.
        caps.finish_tool_schema_json = (
            response_schema
            if isinstance(response_schema, str)
            else json.dumps(response_schema, ensure_ascii=False)
        )

    # Agent.__aenter__ registers every hook in config.hooks on its own
    # HookRunner (agent.py:62-74). Pass caller hooks here, then append otel.
    cfg_hooks = list(hooks)
    if enable_otel:
        cfg_hooks.extend(get_otel_hooks())  # link.md 一.11

    kwargs: dict[str, Any] = dict(
        system_instructions=system_instructions,
        capabilities=caps,
        tools=list(tools),
        subagents=list(subagents),
        policies=list(policies),
        hooks=cfg_hooks,
        retry_config=retry,
        conversation_id=conversation_id,  # link.md 一.5 persistence/resume
        api_key=api_key,
    )
    if session_continuation_mode is not None:
        # SDK-native session establishment: RESUME / CREATE_OR_RESUME / CREATE_ONLY
        # (types.SessionContinuationMode, connection.py:94). Pass None to let the
        # SDK fall back to its default behaviour.
        kwargs["session_continuation_mode"] = session_continuation_mode
    if workspaces:
        kwargs["workspaces"] = list(workspaces)
    if app_data_dir:
        kwargs["app_data_dir"] = app_data_dir
    if save_dir:
        kwargs["save_dir"] = save_dir
    if response_schema is not None and capabilities is not None:
        kwargs["response_schema"] = response_schema
    if hook_runner is not None:
        kwargs["hook_runner"] = hook_runner
    if compaction_config is not None:
        kwargs["compaction_config"] = compaction_config
    if budget_config is not None:
        # link.md 一.6: token / tool-call / model-call caps.
        kwargs["budget_config"] = budget_config

    if backend == "litert":
        from google.antigravity import LiteRTAgentConfig

        if model_path:
            kwargs["model_path"] = model_path
        config_cls = LiteRTAgentConfig
    elif backend == "openai":
        from google.antigravity import LocalOpenAIAgentConfig

        if model:
            kwargs["model"] = model
        if base_url:
            kwargs["base_url"] = base_url
        config_cls = LocalOpenAIAgentConfig
    else:
        config_cls = LocalAgentConfig
        if vertex is not None:
            kwargs["vertex"] = vertex
        if project:
            kwargs["project"] = project
        if location:
            kwargs["location"] = location
        if model:
            kwargs["model"] = model

    try:
        return config_cls(**kwargs)
    except TypeError:
        # Drop optional keys a concrete config class may not accept, then retry.
        for k in ("workspaces", "app_data_dir", "save_dir", "subagents",
                  "response_schema", "conversation_id", "compaction_config",
                  "hook_runner", "vertex", "project", "location",
                  "session_continuation_mode", "budget_config"):
            kwargs.pop(k, None)
        return config_cls(**kwargs)

async def run_task(
    task: "models.Task",
    prompt: str,
    config: "LocalAgentConfig",
    human_in_the_loop: bool = False,
) -> str:
    """Execute one task with a real SDK Agent and record a TaskRun.

    SDK: async with Agent(config) as agent: await agent.chat(prompt)
    (agent.py: chat). The returned text/structured output is the task's
    execution result (link.md 五: ExecutionFinished proxy).

    human_in_the_loop adds interactive.AskQuestionHook for approval prompts
    (link.md 一.6 / 二.10).
    """
    if not SDK_AVAILABLE:
        raise RuntimeError(f"SDK unavailable: {_SDK_ERROR}")

    # The orchestrator may already have opened a RUNNING record for this
    # execution (Orchestrator._begin); reuse it instead of double-counting.
    if task.runs and task.runs[-1].status == models.ExecutionStatus.RUNNING:
        run = task.runs[-1]
    else:
        run = models.TaskRun(task_id=task.task_id, agent_id=task.owner_agent_id)
        run.status = models.ExecutionStatus.RUNNING
        task.runs.append(run)
    task.status = models.TaskStatus.RUNNING

    hooks_list = list(config.hooks)
    if human_in_the_loop:
        hooks_list.append(interactive.AskQuestionHook())

    config.hooks = hooks_list
    try:
        async with Agent(config) as agent:
            response = await agent.chat(prompt)
            text = await response.text()
        run.status = models.ExecutionStatus.FINISHED
        task.status = models.TaskStatus.SUCCESS
        return text
    except Exception as exc:  # noqa: BLE001
        run.status = models.ExecutionStatus.ERROR
        run.error = str(exc)
        task.status = models.TaskStatus.FAILED
        logger.exception("task %s failed", task.task_id)
        raise
    finally:
        run.end_time = time.time()
