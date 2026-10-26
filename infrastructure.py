"""Global infrastructure layer (link.md section 四.第四层).

Maps each infrastructure concern to the SDK where possible, and provides the
missing piece where the SDK has no equivalent (see link.md section 六.四).

  * Lifecycle & state machine ...... SDK Agent lifecycle + HookRunner (real);
                                    task/artifact FSM below.
  * Persistence ................... SDK conversation_id / SessionContinuationMode
                                    (real); TaskStore JSON persistence below.
  * Retry & recovery .............. SDK RetryConfig (real); task crash-recovery
                                    below (recover_incomplete_runs).
  * Permission control ............ SDK policy module (real); task-scoped authz
                                    below (can_agent_modify_task).
  * Audit log ..................... SDK tool-call hooks (real); durable event log
                                    below with run_id correlation.
  * Monitoring / alerting ......... SDK otel hooks + UsageMetadata (real); metric
                                    registry below.
  * Human approval ................ SDK AskQuestionHook (real, interactive);
                                    durable ApprovalManager below.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

from . import models

logger = logging.getLogger(__name__)

# --- Guarded SDK import -------------------------------------------------------
try:
    from google.antigravity.hooks import hooks, policy
    from google.antigravity.types import RetryConfig, SessionContinuationMode

    SDK_AVAILABLE = True
    _SDK_ERROR: Exception | None = None
except Exception as _err:  # pragma: no cover
    SDK_AVAILABLE = False
    _SDK_ERROR = _err

_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "demo_run_id", default=None
)


class PermissionDenied(PermissionError):
    """Raised when an agent tries to modify a task it does not own."""


@contextlib.contextmanager
def bind_run(run_id: str | None) -> Iterator[None]:
    """Correlate audit events with one TaskRun for the duration of a run."""
    token = _RUN_ID.set(run_id)
    try:
        yield
    finally:
        _RUN_ID.reset(token)


# ---------------------------------------------------------------------------
# State machine (link.md 三.1): task/artifact state separation
# ---------------------------------------------------------------------------
class StateManager:
    """Task / artifact state machine.

    SDK status: 否. The SDK has no task-level state machine; only HookRunner
    for tool/lifecycle interception (hook_runner.py:43) and StepStatus at the
    agent step level (types.py:1034). This FSM enforces legal transitions.
    """

    _TASK_TRANSITIONS = {
        models.TaskStatus.PLANNED: {
            models.TaskStatus.RUNNING,
            models.TaskStatus.WAITING_APPROVAL,
            models.TaskStatus.BLOCKED,
            models.TaskStatus.CANCELED,
        },
        models.TaskStatus.RUNNING: {
            models.TaskStatus.WAITING_APPROVAL,
            models.TaskStatus.BLOCKED,
            models.TaskStatus.SUCCESS,
            models.TaskStatus.FAILED,
            models.TaskStatus.CANCELED,
        },
        models.TaskStatus.WAITING_APPROVAL: {
            models.TaskStatus.PLANNED,
            models.TaskStatus.RUNNING,
            models.TaskStatus.BLOCKED,
            models.TaskStatus.SUCCESS,
            models.TaskStatus.FAILED,
            models.TaskStatus.CANCELED,
        },
        models.TaskStatus.BLOCKED: {
            models.TaskStatus.PLANNED,
            models.TaskStatus.RUNNING,
            models.TaskStatus.FAILED,
            models.TaskStatus.CANCELED,
        },
        models.TaskStatus.SUCCESS: set(),
        models.TaskStatus.FAILED: {models.TaskStatus.PLANNED},
        models.TaskStatus.CANCELED: {models.TaskStatus.PLANNED},
    }

    _ARTIFACT_TRANSITIONS = {
        models.ArtifactStatus.NOT_GENERATED: {
            models.ArtifactStatus.GENERATED,
            models.ArtifactStatus.REJECTED,
        },
        models.ArtifactStatus.GENERATED: {
            models.ArtifactStatus.UNDER_REVIEW,
            models.ArtifactStatus.ACCEPTED,
            models.ArtifactStatus.REJECTED,
        },
        models.ArtifactStatus.UNDER_REVIEW: {
            models.ArtifactStatus.ACCEPTED,
            models.ArtifactStatus.REJECTED,
        },
        models.ArtifactStatus.ACCEPTED: {models.ArtifactStatus.REJECTED},
        models.ArtifactStatus.REJECTED: {models.ArtifactStatus.GENERATED},
    }

    def can_transition(self, task: "models.Task", new: "models.TaskStatus") -> bool:
        return new in self._TASK_TRANSITIONS.get(task.status, set())

    def transition(self, task: "models.Task", new: "models.TaskStatus") -> None:
        if not self.can_transition(task, new):
            raise ValueError(f"Illegal task transition {task.status} -> {new}")
        task.status = new
        logger.info("task %s -> %s", task.task_id, new.value)

    def try_transition(self, task: "models.Task", new: "models.TaskStatus") -> bool:
        """Transition when legal; otherwise leave the task untouched."""
        if not self.can_transition(task, new):
            return False
        self.transition(task, new)
        return True

    def transition_artifact(
        self, artifact: "models.Artifact", new: "models.ArtifactStatus"
    ) -> None:
        if new not in self._ARTIFACT_TRANSITIONS.get(artifact.status, set()):
            raise ValueError(
                f"Illegal artifact transition {artifact.status} -> {new}"
            )
        artifact.status = new


# ---------------------------------------------------------------------------
# Persistence (link.md 一.5)
# ---------------------------------------------------------------------------
def make_session_continuation(mode: str = "resume") -> Any:
    """Return a real SessionContinuationMode for conversation resume."""
    if not SDK_AVAILABLE:
        raise RuntimeError(f"SDK unavailable: {_SDK_ERROR}")
    return {
        "resume": SessionContinuationMode.RESUME,
        "create_or_resume": SessionContinuationMode.CREATE_OR_RESUME,
        "create": SessionContinuationMode.CREATE_ONLY,
    }[mode]


class TaskStore:
    """Durable task / run / artifact store.

    SDK status: 部分. The SDK persists conversations (conversation_id,
    SessionContinuationMode) but has no task-record store. This one keeps the
    full Task object graph and can mirror it to a JSON file so a crashed
    session can be reloaded and recovered.
    """

    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self._tasks: dict[str, "models.Task"] = {}
        self.path = Path(path) if path else None
        if self.path is not None and self.path.exists():
            try:
                self.load_json(str(self.path))
            except Exception:
                logger.warning("could not load task store %s", self.path, exc_info=True)

    # -- basic CRUD ---------------------------------------------------------
    def save(self, task: "models.Task") -> None:
        self._tasks[task.task_id] = task
        self._autosave()

    upsert = save

    def load(self, task_id: str) -> "models.Task | None":
        return self._tasks.get(task_id)

    def get(self, task_id: str) -> "models.Task | None":
        return self._tasks.get(task_id)

    def all(self) -> list["models.Task"]:
        return list(self._tasks.values())

    def remove(self, task_id: str) -> bool:
        existed = self._tasks.pop(task_id, None) is not None
        if existed:
            self._autosave()
        return existed

    def clear(self) -> None:
        self._tasks.clear()
        self._autosave()

    def __len__(self) -> int:
        return len(self._tasks)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._tasks

    # -- JSON ---------------------------------------------------------------
    def _autosave(self) -> None:
        if self.path is not None:
            try:
                self.save_json(str(self.path))
            except Exception:
                logger.warning("task store autosave failed", exc_info=True)

    def to_dict(self) -> dict[str, Any]:
        return {tid: t.to_dict() for tid, t in self._tasks.items()}

    def save_json(self, path: str | os.PathLike[str] | None = None) -> None:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for task store")
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(target)

    def load_json(self, path: str | os.PathLike[str] | None = None) -> int:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for task store")
        data = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "tasks" in data and isinstance(data["tasks"], list):
            entries = {t.get("task_id"): t for t in data["tasks"]}
        elif isinstance(data, list):
            entries = {t.get("task_id"): t for t in data}
        else:
            entries = data
        for tid, payload in entries.items():
            task = models.Task.from_dict({**payload, "task_id": payload.get("task_id") or tid})
            self._tasks[task.task_id] = task
        if path and self.path is None:
            self.path = target
        return len(entries)

    # Backwards-compatible alias used by demo/main.py.
    def dump_json(self, path: str) -> None:
        self.save_json(path)


# ---------------------------------------------------------------------------
# Recovery (link.md 一.3)
# ---------------------------------------------------------------------------
def build_retry_config(max_retries: int = 3) -> Any:
    """Return a real SDK RetryConfig for model API/output retries."""
    if not SDK_AVAILABLE:
        raise RuntimeError(f"SDK unavailable: {_SDK_ERROR}")
    from google.antigravity.types import (
        ModelAPIRetryConfig,
        ModelOutputRetryConfig,
    )

    return RetryConfig(
        model_api=ModelAPIRetryConfig(max_retries=max_retries),
        model_output=ModelOutputRetryConfig(max_retries=1),
    )


def recover_incomplete_runs(
    task_store: "TaskStore", state: "StateManager | None" = None
) -> list[str]:
    """Find tasks whose last run never reached a terminal state and reset them.

    SDK status: 部分. The SDK resumes sessions (Agent.resume, agent.py:209)
    but does not auto-recover individual task runs or dedupe effects. This
    marks the dangling run PROCESS_LOST and returns the task to PLANNED so the
    orchestrator can schedule it again.
    """
    recovered: list[str] = []
    now = time.time()
    for task in task_store.all():
        last = task.runs[-1] if task.runs else None
        dangling_run = last is not None and last.status in (
            models.ExecutionStatus.PENDING,
            models.ExecutionStatus.RUNNING,
        )
        stuck_task = task.status in (
            models.TaskStatus.RUNNING,
            models.TaskStatus.WAITING_APPROVAL,
        )
        if not (dangling_run or stuck_task):
            continue

        if dangling_run:
            last.status = models.ExecutionStatus.PROCESS_LOST
            last.end_time = now
            last.error = last.error or "process lost during recovery"
        if task.status in (
            models.TaskStatus.RUNNING,
            models.TaskStatus.WAITING_APPROVAL,
            models.TaskStatus.BLOCKED,
        ):
            task.status = models.TaskStatus.PLANNED
        task.report_submitted = False
        task.metadata["recovered_at"] = now
        recovered.append(task.task_id)
        task_store.save(task)
        logger.info("recovered task %s (run=%s)", task.task_id, getattr(last, "run_id", None))

    return recovered


# ---------------------------------------------------------------------------
# Permissions (link.md 一.6)
# ---------------------------------------------------------------------------
def build_security_policies(
    allowed: list[str], denied: list[str] = (), ask_user: list[str] = ()
) -> list[Any]:
    """Return real SDK policy objects (policy module)."""
    if not SDK_AVAILABLE:
        raise RuntimeError(f"SDK unavailable: {_SDK_ERROR}")
    policies = [policy.deny_all()]
    for t in allowed:
        policies.append(policy.allow(t))
    for t in denied:
        policies.append(policy.deny(t))
    for t in ask_user:
        policies.append(policy.ask_user(t))
    return policies


DEFAULT_DANGEROUS_COMMANDS = (
    "rm -rf /",
    "mkfs",
    "shutdown",
    "reboot",
    ":(){",
    "dd if=/dev/zero",
    "chmod -R 777 /",
)


def _tool_args_text(args: Any) -> str:
    if isinstance(args, dict):
        return str(args.get("CommandLine") or args.get("command") or args)
    return str(getattr(args, "CommandLine", "") or args)


def build_agent_policies(
    worktree: str | os.PathLike[str],
    denied_commands: Sequence[str] | None = None,
    allow_all: bool = True,
) -> list[Any]:
    """Build the SDK policy list for one egg.

    link.md 一.6: the agent may only touch its own worktree, and obviously
    destructive shell commands are denied. File tools are scoped with the
    SDK's real policy.workspace_only (a specific deny beats the wildcard
    allow), while everything else remains autonomous.
    """
    if not SDK_AVAILABLE:
        return []
    policies: list[Any] = list(policy.workspace_only([str(worktree)]))
    patterns = DEFAULT_DANGEROUS_COMMANDS if denied_commands is None else denied_commands
    for pattern in patterns:
        policies.append(
            policy.deny(
                "run_command",
                when=lambda args, _p=pattern: _p in _tool_args_text(args),
                name="deny_dangerous_command",
                reason=f"command contains {pattern!r}",
            )
        )
    policies.append(policy.allow_all() if allow_all else policy.deny_all())
    return policies


def build_budget_config(
    max_total_tokens: int | None = None,
    max_tool_calls: int | None = None,
    max_model_calls: int | None = None,
    scope: str = "lifetime",
) -> Any:
    """Return a real SDK BudgetConfig, or None when no limit is configured.

    link.md 一.6 / 二.12: token / tool-call caps are the resource budget the
    SDK can actually enforce (types.BudgetConfig, connection.py:104).
    """
    if not SDK_AVAILABLE:
        return None
    if not any((max_total_tokens, max_tool_calls, max_model_calls)):
        return None
    from google.antigravity.types import BudgetConfig, BudgetScope

    try:
        return BudgetConfig(
            max_total_tokens=max_total_tokens,
            max_tool_calls=max_tool_calls,
            max_model_calls=max_model_calls,
            scope=(
                BudgetScope.FORWARD_LOOKING
                if str(scope).lower() == "forward"
                else BudgetScope.LIFETIME
            ),
        )
    except Exception:  # pragma: no cover - older SDK without BudgetConfig
        logger.warning("BudgetConfig unavailable", exc_info=True)
        return None


def can_agent_modify_task(
    agent: "models.AgentInfo", task: "models.Task", action: str = "write"
) -> bool:
    """Task-scoped authorization (link.md 一.6 / 一.4).

    SDK status: 否. The SDK policies gate tools, not task ownership. This
    enforces "Agent 只能修改授权的任务/文件" on top of the SDK.

    Accepted permission forms (in AgentInfo.permissions):
      "*"                -- everything
      "task:*"           -- every task
      "write:*"          -- every task for that action
      "<task_id>"        -- one task
      "<action>:<task_id>" -- one task for one action
    Ownership (agent.agent_id == task.owner_agent_id) always grants access.
    """
    perms = {p for p in (agent.permissions or []) if p}
    if "*" in perms or "task:*" in perms:
        return True
    if action and f"{action}:*" in perms:
        return True
    if agent.agent_id and agent.agent_id == task.owner_agent_id:
        return True
    if task.task_id in perms:
        return True
    if action and f"{action}:{task.task_id}" in perms:
        return True
    return False


def assert_can_modify(
    agent: "models.AgentInfo", task: "models.Task", action: str = "write"
) -> None:
    if not can_agent_modify_task(agent, task, action):
        raise PermissionDenied(
            f"agent {agent.agent_id!r} may not {action} task {task.task_id!r}"
        )


# ---------------------------------------------------------------------------
# Human approval (link.md 二.10)
# ---------------------------------------------------------------------------
class ApprovalManager:
    """Durable approval requests for risky tasks.

    SDK status: 部分. interactive.AskQuestionHook prompts inside a live turn;
    this manager persists the request/decision across turns and gates task
    state (WAITING_APPROVAL -> RUNNING / FAILED).
    """

    def __init__(
        self,
        store: "TaskStore | None" = None,
        state: "StateManager | None" = None,
        path: str | os.PathLike[str] | None = None,
    ) -> None:
        self.store = store
        self.state = state or StateManager()
        self.path = Path(path) if path else None
        self._approvals: dict[str, "models.Approval"] = {}
        if self.path is not None and self.path.exists():
            try:
                self.load_json(str(self.path))
            except Exception:
                logger.warning("could not load approvals", exc_info=True)

    # -- queries ------------------------------------------------------------
    def get(self, approval_id: str) -> "models.Approval | None":
        return self._approvals.get(approval_id)

    def all(self) -> list["models.Approval"]:
        return list(self._approvals.values())

    def pending(self) -> list["models.Approval"]:
        return [
            a for a in self._approvals.values()
            if a.status == models.ApprovalStatus.PENDING
        ]

    def for_task(self, task_id: str) -> list["models.Approval"]:
        return [a for a in self._approvals.values() if a.task_id == task_id]

    # -- commands -----------------------------------------------------------
    def request(
        self,
        task: "models.Task",
        requested_by: str,
        reason: str = "",
        ttl: float | None = None,
    ) -> "models.Approval":
        existing = task.approval
        if existing is not None and existing.status == models.ApprovalStatus.PENDING:
            return existing
        approval = models.Approval(
            task_id=task.task_id,
            requested_by=requested_by,
            reason=reason,
            deadline=(time.time() + ttl) if ttl else None,
        )
        task.approval = approval
        task.metadata["blocked_reason"] = reason or "waiting for approval"
        self._approvals[approval.approval_id] = approval
        if self.state.try_transition(task, models.TaskStatus.WAITING_APPROVAL) is False:
            task.status = models.TaskStatus.WAITING_APPROVAL
        if self.store is not None:
            self.store.save(task)
        self._autosave()
        logger.info("approval requested for task %s: %s", task.task_id, reason)
        return approval

    def decide(
        self,
        approval_id: str,
        approved: bool,
        decided_by: str = "",
        note: str = "",
    ) -> "models.Approval":
        approval = self._approvals.get(approval_id)
        if approval is None:
            raise KeyError(f"unknown approval {approval_id!r}")
        if approval.status != models.ApprovalStatus.PENDING:
            return approval
        approval.status = (
            models.ApprovalStatus.APPROVED
            if approved
            else models.ApprovalStatus.REJECTED
        )
        approval.decided_by = decided_by
        approval.decision_note = note
        approval.decided_at = time.time()

        task = self.store.load(approval.task_id) if self.store is not None else None
        if task is not None:
            task.approval = approval
            if approved:
                # Approved -> schedulable again (the orchestrator only picks
                # PLANNED tasks; RUNNING would mean "already dispatched").
                task.metadata.pop("blocked_reason", None)
                if not self.state.try_transition(task, models.TaskStatus.PLANNED):
                    task.status = models.TaskStatus.PLANNED
            else:
                task.metadata["blocked_reason"] = note or "approval rejected"
                if not self.state.try_transition(task, models.TaskStatus.FAILED):
                    task.status = models.TaskStatus.FAILED
            if self.store is not None:
                self.store.save(task)
        self._autosave()
        return approval

    def expire(self, now: float | None = None) -> list["models.Approval"]:
        now = time.time() if now is None else now
        expired = []
        for approval in self.pending():
            if approval.deadline is not None and approval.deadline <= now:
                approval.status = models.ApprovalStatus.TIMED_OUT
                approval.decision_note = "deadline exceeded"
                expired.append(approval)
                task = (
                    self.store.load(approval.task_id)
                    if self.store is not None else None
                )
                if task is not None:
                    task.metadata["blocked_reason"] = "approval timed out"
                    if not self.state.try_transition(
                        task, models.TaskStatus.BLOCKED
                    ):
                        task.status = models.TaskStatus.BLOCKED
                    self.store.save(task)
        if expired:
            self._autosave()
        return expired

    # -- persistence --------------------------------------------------------
    def _autosave(self) -> None:
        if self.path is not None:
            try:
                self.save_json(str(self.path))
            except Exception:
                logger.warning("approval autosave failed", exc_info=True)

    def to_dict(self) -> dict[str, Any]:
        return {aid: a.to_dict() for aid, a in self._approvals.items()}

    def save_json(self, path: str | os.PathLike[str] | None = None) -> None:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for approvals")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def load_json(self, path: str | os.PathLike[str] | None = None) -> int:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for approvals")
        data = json.loads(target.read_text(encoding="utf-8"))
        for aid, payload in data.items():
            approval = models.Approval.from_dict({**payload, "approval_id": aid})
            self._approvals[approval.approval_id] = approval
        return len(data)


# ---------------------------------------------------------------------------
# Audit log (link.md 二.8)
# ---------------------------------------------------------------------------
class AuditLog:
    """Append-only event log with run_id correlation and optional JSON sink.

    SDK provides tool-call hooks (real) but no dedicated state-change / event
    log (utils/state.py is a KV store).
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        max_events: int = 5000,
    ) -> None:
        self.events: list[dict[str, Any]] = []
        self.path = Path(path) if path else None
        self.max_events = max_events
        self._seq = 0
        if self.path is not None and self.path.exists():
            try:
                self.load_json(str(self.path))
            except Exception:
                logger.warning("could not load audit log", exc_info=True)

    def record(self, event: str, **kwargs: Any) -> dict[str, Any]:
        self._seq += 1
        entry = {
            "seq": self._seq,
            "ts": time.time(),
            "run_id": kwargs.pop("run_id", None) or _RUN_ID.get(),
            "event": event,
            **kwargs,
        }
        self.events.append(entry)
        if len(self.events) > self.max_events:
            del self.events[: len(self.events) - self.max_events]
        if self.path is not None:
            try:
                self.append_json(entry)
            except Exception:
                logger.warning("audit append failed", exc_info=True)
        return entry

    def query(self, event: str | None = None, run_id: str | None = None) -> list[dict[str, Any]]:
        return [
            e for e in self.events
            if (event is None or e.get("event") == event)
            and (run_id is None or e.get("run_id") == run_id)
        ]

    def install_sdk_hooks(self) -> list[Any]:
        """Attach a real SDK post_tool_call hook that writes into this log."""
        if not SDK_AVAILABLE:
            return []

        @hooks.post_tool_call
        async def _audit(context: Any, data: Any) -> None:
            self.record(
                "tool_call",
                tool=getattr(data, "name", None),
                args=getattr(data, "args", None),
                result=getattr(data, "result", None),
            )

        return [_audit]

    # -- persistence --------------------------------------------------------
    def append_json(self, entry: dict[str, Any]) -> None:
        assert self.path is not None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    def save_json(self, path: str | os.PathLike[str] | None = None) -> None:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for audit log")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.events, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def load_json(self, path: str | os.PathLike[str] | None = None) -> int:
        target = Path(path) if path else self.path
        if target is None:
            raise ValueError("no path supplied for audit log")
        raw = target.read_text(encoding="utf-8").strip()
        if not raw:
            return 0
        if raw.startswith("["):
            data = json.loads(raw)
        else:
            data = [json.loads(line) for line in raw.splitlines() if line.strip()]
        self.events = list(data)
        self._seq = max((int(e.get("seq", 0)) for e in data), default=0)
        return len(data)


# ---------------------------------------------------------------------------
# Observability / metrics (link.md 二.11)
# ---------------------------------------------------------------------------
class Metrics:
    """Metric registry (counters / gauges / histograms).

    SDK status: 部分. The SDK exposes OpenTelemetry tracing spans
    (utils/otel.py) and UsageMetadata (types.py:869) / Conversation.total_usage,
    but no exported counters/gauges for tokens/steps/failures.
    """

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}

    def inc(self, name: str, by: float = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + by

    def set_gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def observe(self, name: str, value: float, cap: int = 1000) -> None:
        bucket = self._histograms.setdefault(name, [])
        bucket.append(float(value))
        if len(bucket) > cap:
            del bucket[: len(bucket) - cap]

    def get(self, name: str, default: float = 0) -> float:
        return self._counters.get(name, self._gauges.get(name, default))

    def snapshot(self) -> dict[str, Any]:
        hist = {}
        for name, values in self._histograms.items():
            if values:
                hist[name] = {
                    "count": len(values),
                    "sum": sum(values),
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values),
                }
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": hist,
        }

    def to_dict(self) -> dict[str, Any]:
        return self.snapshot()

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
