"""Domain models for the multi-agent orchestration framework described in link.md.

SDK status: 否 - none of these types exist in google.antigravity.
The SDK ships a single Agent plus conversation/tool/hook primitives, but no
task / task-run / dependency / artifact / acceptance domain model. These are
the orchestration-level entities the rest of the framework operates on.

See link.md section 三.3 ("Agent 与任务不是一回事").
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Enumerations (link.md: task state / artifact state separation, section 三.1)
# ---------------------------------------------------------------------------
class TaskStatus(enum.Enum):
    """Task lifecycle state (planned/running/waiting-approval/blocked/...)."""

    PLANNED = "planned"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    BLOCKED = "blocked"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"


class ArtifactStatus(enum.Enum):
    """Artifact lifecycle (link.md: 未生成/已生成/验收中/验收通过/验收失败)."""

    NOT_GENERATED = "not_generated"
    GENERATED = "generated"
    UNDER_REVIEW = "under_review"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class ExecutionStatus(enum.Enum):
    """Per-run execution status (link.md section 三.3: TaskRun)."""

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    ABNORMAL_EXIT = "abnormal_exit"
    PROCESS_LOST = "process_lost"


class AgentRunStatus(enum.Enum):
    """Agent identity / capability / permission / run status (link.md 三.3)."""

    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"


class ApprovalStatus(enum.Enum):
    """Approval lifecycle (link.md 二.10 人工介入与审批管理)."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"


# ---------------------------------------------------------------------------
# Entities (link.md 三.3)
# ---------------------------------------------------------------------------
@dataclass
class AgentInfo:
    """An agent's identity, capability, permission and run status.

    SDK note: the SDK provides Agent / AgentConfig / CapabilitiesConfig for a
    single agent. This dataclass models the orchestration-level registry.
    """

    agent_id: str
    name: str
    capabilities: list[str] = field(default_factory=list)
    permissions: list[str] = field(default_factory=list)
    status: AgentRunStatus = AgentRunStatus.OFFLINE

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "capabilities": list(self.capabilities),
            "permissions": list(self.permissions),
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentInfo":
        return cls(
            agent_id=data["agent_id"],
            name=data.get("name", ""),
            capabilities=list(data.get("capabilities", [])),
            permissions=list(data.get("permissions", [])),
            status=AgentRunStatus(data.get("status", "offline")),
        )


@dataclass
class Artifact:
    """A deliverable produced by a task (link.md: Artifact entity)."""

    artifact_id: str = field(default_factory=lambda: f"art-{uuid.uuid4().hex[:8]}")
    task_id: str = ""
    path: str = ""
    kind: str = "file"  # file | dir | message | result
    status: ArtifactStatus = ArtifactStatus.NOT_GENERATED
    version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "task_id": self.task_id,
            "path": self.path,
            "kind": self.kind,
            "status": self.status.value,
            "version": self.version,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Artifact":
        return cls(
            artifact_id=data.get("artifact_id") or f"art-{uuid.uuid4().hex[:8]}",
            task_id=data.get("task_id", ""),
            path=data.get("path", ""),
            kind=data.get("kind", "file"),
            status=ArtifactStatus(data.get("status", "not_generated")),
            version=int(data.get("version", 1)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class Acceptance:
    """Acceptance rule, result and evidence for a task (link.md: Acceptance)."""

    task_id: str
    criteria: list[Any] = field(default_factory=list)
    passed: bool | None = None
    evidence: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        # Callables are runtime-only and cannot be serialized.
        return {
            "task_id": self.task_id,
            "criteria": [c for c in self.criteria if isinstance(c, str)],
            "passed": self.passed,
            "evidence": list(self.evidence),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Acceptance":
        return cls(
            task_id=data.get("task_id", ""),
            criteria=list(data.get("criteria", [])),
            passed=data.get("passed"),
            evidence=list(data.get("evidence", [])),
        )


@dataclass
class Dependency:
    """A dependency condition between two tasks (link.md: Dependency)."""

    task_id: str
    depends_on: str
    condition: str = "finished"  # finished | accepted | reported

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "depends_on": self.depends_on,
            "condition": self.condition,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Dependency":
        return cls(
            task_id=data.get("task_id", ""),
            depends_on=data.get("depends_on", ""),
            condition=data.get("condition", "finished"),
        )


@dataclass
class TaskRun:
    """A single execution record of a task (link.md: TaskRun)."""

    run_id: str = field(default_factory=lambda: f"run-{uuid.uuid4().hex[:8]}")
    task_id: str = ""
    agent_id: str = ""
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: float | None = None
    end_time: float | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskRun":
        return cls(
            run_id=data.get("run_id") or f"run-{uuid.uuid4().hex[:8]}",
            task_id=data.get("task_id", ""),
            agent_id=data.get("agent_id", ""),
            status=ExecutionStatus(data.get("status", "pending")),
            start_time=data.get("start_time"),
            end_time=data.get("end_time"),
            error=data.get("error"),
        )


@dataclass
class Approval:
    """An approval request for a risky task/tool (link.md 二.10).

    Approval is a first-class entity, not just a task status: it records who
    asked, who decided, why, and when the request expires.
    """

    task_id: str
    requested_by: str
    reason: str = ""
    approval_id: str = field(default_factory=lambda: f"apr-{uuid.uuid4().hex[:8]}")
    status: ApprovalStatus = ApprovalStatus.PENDING
    decided_by: str = ""
    decision_note: str = ""
    created_at: float = field(default_factory=time.time)
    deadline: float | None = None
    decided_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "approval_id": self.approval_id,
            "task_id": self.task_id,
            "requested_by": self.requested_by,
            "reason": self.reason,
            "status": self.status.value,
            "decided_by": self.decided_by,
            "decision_note": self.decision_note,
            "created_at": self.created_at,
            "deadline": self.deadline,
            "decided_at": self.decided_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Approval":
        return cls(
            task_id=data.get("task_id", ""),
            requested_by=data.get("requested_by", ""),
            reason=data.get("reason", ""),
            approval_id=data.get("approval_id") or f"apr-{uuid.uuid4().hex[:8]}",
            status=ApprovalStatus(data.get("status", "pending")),
            decided_by=data.get("decided_by", ""),
            decision_note=data.get("decision_note", ""),
            created_at=data.get("created_at", time.time()),
            deadline=data.get("deadline"),
            decided_at=data.get("decided_at"),
        )


@dataclass
class Task:
    """A unit of work with owner, priority, required flag and execution status.

    report_submitted tracks the "子任务已复命" state (link.md 一.2); it is only
    set after acceptance passes.
    """

    task_id: str = field(default_factory=lambda: f"task-{uuid.uuid4().hex[:8]}")
    title: str = ""
    description: str = ""
    owner_agent_id: str = ""
    priority: int = 5  # 1 (highest) .. 10 (lowest)
    required: bool = True  # optional tasks may fail without blocking convergence
    status: TaskStatus = TaskStatus.PLANNED
    dependencies: list[Dependency] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)
    acceptance: Acceptance | None = None
    runs: list[TaskRun] = field(default_factory=list)
    report_submitted: bool = False
    confirmed: bool = False  # 总指挥确认（link.md 一.2 的第四态）
    report_payload: dict[str, Any] = field(default_factory=dict)
    approval: Approval | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "description": self.description,
            "owner_agent_id": self.owner_agent_id,
            "priority": self.priority,
            "required": self.required,
            "status": self.status.value,
            "dependencies": [d.to_dict() for d in self.dependencies],
            "artifacts": [a.to_dict() for a in self.artifacts],
            "acceptance": self.acceptance.to_dict() if self.acceptance else None,
            "runs": [r.to_dict() for r in self.runs],
            "report_submitted": self.report_submitted,
            "confirmed": self.confirmed,
            "report_payload": dict(self.report_payload),
            "approval": self.approval.to_dict() if self.approval else None,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        task = cls(
            task_id=data.get("task_id") or f"task-{uuid.uuid4().hex[:8]}",
            title=data.get("title", ""),
            description=data.get("description", ""),
            owner_agent_id=data.get("owner_agent_id", ""),
            priority=int(data.get("priority", 5)),
            required=bool(data.get("required", True)),
            status=TaskStatus(data.get("status", "planned")),
            report_submitted=bool(data.get("report_submitted", False)),
            confirmed=bool(data.get("confirmed", False)),
            report_payload=dict(data.get("report_payload", {})),
            metadata=dict(data.get("metadata", {})),
        )
        task.dependencies = [Dependency.from_dict(x) for x in data.get("dependencies", [])]
        task.artifacts = [Artifact.from_dict(x) for x in data.get("artifacts", [])]
        task.runs = [TaskRun.from_dict(x) for x in data.get("runs", [])]
        acc = data.get("acceptance")
        task.acceptance = Acceptance.from_dict(acc) if acc else None
        apr = data.get("approval")
        task.approval = Approval.from_dict(apr) if apr else None
        return task
