"""Task completion rule (link.md section 五).

TaskCompleted = ExecutionFinished AND ArtifactsValid AND AcceptancePassed AND ReportSubmitted

SDK status: 部分. ExecutionFinished can be approximated by the agent's step
status (StepStatus.DONE/ERROR/CANCELED, types.py:1034) or our Task.status;
the other three clauses are implemented here on top of the SDK:

  * ArtifactsValid   -> acceptance.validate_artifacts (existence / non-empty)
  * AcceptancePassed -> acceptance.evaluate_acceptance (executes criteria)
  * ReportSubmitted  -> Task.report_submitted (set by the sub-agent, and only
                        after acceptance passes -- see canvas/egg.py)

'global_completion' adds the convergence view required by link.md 二.12 /
四.第一层: required tasks must complete; optional tasks may fail; blocked or
approval-pending tasks are reported with a reason instead of silently passing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from . import acceptance as acceptance_mod
from . import models

logger = logging.getLogger(__name__)

_TERMINAL_TASK = (
    models.TaskStatus.SUCCESS,
    models.TaskStatus.FAILED,
    models.TaskStatus.CANCELED,
)
_TERMINAL_RUN = (
    models.ExecutionStatus.FINISHED,
    models.ExecutionStatus.ERROR,
    models.ExecutionStatus.CANCELED,
    models.ExecutionStatus.TIMEOUT,
    models.ExecutionStatus.ABNORMAL_EXIT,
    models.ExecutionStatus.PROCESS_LOST,
)


@dataclass
class CompletionReport:
    execution_finished: bool
    artifacts_valid: bool
    acceptance_passed: bool
    report_submitted: bool
    complete: bool
    blocked: bool = False
    reason: str = ""
    task_id: str = ""
    status: str = ""

    def missing(self) -> list[str]:
        gaps = []
        if not self.execution_finished:
            gaps.append("ExecutionFinished")
        if not self.artifacts_valid:
            gaps.append("ArtifactsValid")
        if not self.acceptance_passed:
            gaps.append("AcceptancePassed")
        if not self.report_submitted:
            gaps.append("ReportSubmitted")
        return gaps

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "execution_finished": self.execution_finished,
            "artifacts_valid": self.artifacts_valid,
            "acceptance_passed": self.acceptance_passed,
            "report_submitted": self.report_submitted,
            "complete": self.complete,
            "blocked": self.blocked,
            "reason": self.reason,
            "missing": self.missing(),
        }


@dataclass
class GlobalCompletion:
    """Convergence report for a whole task graph (link.md 二.12)."""

    complete: bool
    total: int = 0
    completed: int = 0
    required_total: int = 0
    required_completed: int = 0
    blocked: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    optional_failed: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def missing(self) -> list[str]:
        return list(self.reasons)

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "total": self.total,
            "completed": self.completed,
            "required_total": self.required_total,
            "required_completed": self.required_completed,
            "blocked": list(self.blocked),
            "failed": list(self.failed),
            "pending": list(self.pending),
            "optional_failed": list(self.optional_failed),
            "reasons": list(self.reasons),
            "tasks": dict(self.tasks),
        }


def _approval_pending(task: "models.Task") -> bool:
    return (
        task.approval is not None
        and task.approval.status == models.ApprovalStatus.PENDING
    )


def evaluate_task_completed(
    task: "models.Task", force: bool = False
) -> CompletionReport:
    """Evaluate the link.md completion rule for a single task."""
    approval_pending = _approval_pending(task)
    blocked = task.status == models.TaskStatus.BLOCKED or approval_pending

    # ExecutionFinished -- approximated by task/run terminal status.
    execution_finished = (
        task.status in _TERMINAL_TASK
        or any(r.status in _TERMINAL_RUN for r in task.runs)
    )
    if blocked:
        execution_finished = False

    # ArtifactsValid -- real filesystem validation.
    artifacts_valid = acceptance_mod.validate_artifacts(task.artifacts, task)

    # AcceptancePassed -- executes the task's criteria (cached on the object).
    acceptance_passed = True
    if task.acceptance is not None:
        attempted = bool(task.runs) or task.status in (
            models.TaskStatus.SUCCESS,
            models.TaskStatus.FAILED,
            models.TaskStatus.CANCELED,
        )
        if not attempted and task.acceptance.passed is None:
            # Not started yet: do NOT execute (and cache) criteria against an
            # empty workspace -- that would poison the real run later.
            acceptance_passed = False
        else:
            acceptance_passed = acceptance_mod.evaluate_acceptance(
                task.acceptance, task, force=force
            )

    # ReportSubmitted -- whether the sub-agent has 复命.
    report_submitted = bool(task.report_submitted)

    complete = (
        execution_finished
        and artifacts_valid
        and acceptance_passed
        and report_submitted
        and not blocked
    )

    reason = ""
    if blocked:
        if approval_pending:
            reason = "waiting for approval"
        else:
            reason = task.metadata.get("blocked_reason") or "task blocked"
    elif not complete:
        reason = ", ".join(
            m for m in (
                "ExecutionFinished" if not execution_finished else "",
                "ArtifactsValid" if not artifacts_valid else "",
                "AcceptancePassed" if not acceptance_passed else "",
                "ReportSubmitted" if not report_submitted else "",
            ) if m
        )

    return CompletionReport(
        execution_finished=execution_finished,
        artifacts_valid=artifacts_valid,
        acceptance_passed=acceptance_passed,
        report_submitted=report_submitted,
        complete=complete,
        blocked=blocked,
        reason=reason,
        task_id=task.task_id,
        status=task.status.value,
    )


def global_report(tasks: list["models.Task"], force: bool = False) -> GlobalCompletion:
    """Build the structured convergence report for a task graph.

    Required tasks must complete; optional tasks are allowed to fail. Canceled
    tasks are ignored entirely (they were withdrawn, not attempted).
    """
    live = [t for t in tasks if t.status != models.TaskStatus.CANCELED]
    if not live:
        return GlobalCompletion(complete=False, reasons=["no tasks to converge"])

    required = [t for t in live if getattr(t, "required", True)]
    if not required:
        # No explicit required flag: fall back to "everything must complete".
        required = live
    required_ids = {t.task_id for t in required}

    report = GlobalCompletion(
        complete=False,
        total=len(live),
        required_total=len(required),
    )
    for task in live:
        rep = evaluate_task_completed(task, force=force)
        report.tasks[task.task_id] = rep.to_dict()
        if rep.complete:
            report.completed += 1
        if task.task_id in required_ids and rep.complete:
            report.required_completed += 1

        if rep.blocked:
            report.blocked.append(task.task_id)
        elif task.status == models.TaskStatus.FAILED:
            if task.task_id in required_ids:
                report.failed.append(task.task_id)
            else:
                report.optional_failed.append(task.task_id)
        elif not rep.complete:
            report.pending.append(task.task_id)

    report.complete = report.required_completed == report.required_total
    if not report.complete:
        if report.blocked:
            report.reasons.append(
                f"blocked: {', '.join(report.blocked)}"
            )
        if report.failed:
            report.reasons.append(f"failed: {', '.join(report.failed)}")
        if report.pending:
            report.reasons.append(f"pending: {', '.join(report.pending)}")
        if not report.reasons:
            report.reasons.append("required tasks incomplete")
    return report


def global_completion(tasks: list["models.Task"], force: bool = False) -> bool:
    """Global termination / convergence (link.md section 二.12, 四.第一层)."""
    return global_report(tasks, force=force).complete
