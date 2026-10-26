"""Artifact generation & acceptance (link.md section 一.2).

SDK status: 否. The SDK does not generate or validate deliverables. The closest
real SDK capability is structured output validation via
'AgentConfig.response_schema' (connection.py:97, types.py:546), which validates
the agent's *text/structured output*, not files on disk.

This module implements the acceptance harness the design asks for:
'validate_artifact' checks that a deliverable exists / is non-empty and moves
it through the ArtifactStatus FSM, and 'evaluate_acceptance' executes the
task's criteria (strings, dicts or callables) and records evidence.

Criteria forms
--------------
* "file exists: path/to/file"      -- path exists (relative to worktree)
* "non-empty: path/to/file"        -- path exists and is non-empty
* "file contains: path :: text"    -- file contains a literal substring
* "artifacts exist" / "file exists"   -- every task artifact exists
* "non-empty" / "artifacts non-empty" -- every task artifact non-empty
* "min_artifacts: 2"               -- at least N artifacts exist
* "command: pytest -q"             -- shell command exits 0 (cwd=worktree)
* {"type": "file_exists", "path": "..."} -- structured form
* callable(task) -> bool | (bool, evidence) -- custom check

Unknown string criteria are recorded as unverified (and do NOT fail the task),
so existing descriptive criteria keep working.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from . import models

logger = logging.getLogger(__name__)

DEFAULT_COMMAND_TIMEOUT = 60.0
_EVIDENCE_PREFIXES = ("PASS ", "FAIL ")


@dataclass
class CriterionResult:
    """Outcome of evaluating a single acceptance criterion."""

    criterion: str
    passed: bool
    evidence: str = ""
    skipped: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "criterion": self.criterion,
            "passed": self.passed,
            "evidence": self.evidence,
            "skipped": self.skipped,
        }


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
def _worktree(task: "models.Task | None") -> Path:
    if task is not None:
        wt = task.metadata.get("worktree") if task.metadata else None
        if wt:
            return Path(wt)
    return Path.cwd()


def _resolve(path: str, task: "models.Task | None") -> Path:
    p = Path(os.path.expanduser(str(path)))
    return p if p.is_absolute() else _worktree(task) / p


def _label(criterion: Any) -> str:
    if isinstance(criterion, str):
        return criterion
    if isinstance(criterion, dict):
        return str(criterion.get("type") or criterion)
    return getattr(criterion, "__name__", repr(criterion))


# ---------------------------------------------------------------------------
# Artifact validation (link.md: ArtifactStatus FSM)
# ---------------------------------------------------------------------------
def validate_artifact(
    artifact: "models.Artifact", task: "models.Task | None" = None
) -> bool:
    """Validate one artifact and advance its status.

    file/dir  -> must exist; a file must be non-empty.
    message/result -> no filesystem footprint; accepted as generated.
    """
    if artifact.kind in ("message", "result", "text"):
        artifact.status = models.ArtifactStatus.GENERATED
        return True
    if not artifact.path:
        logger.warning("artifact %s has no path", artifact.artifact_id)
        artifact.status = models.ArtifactStatus.REJECTED
        artifact.metadata["reason"] = "missing path"
        return False
    p = _resolve(artifact.path, task)
    if not p.exists():
        logger.warning("artifact %s missing: %s", artifact.artifact_id, p)
        artifact.status = models.ArtifactStatus.REJECTED
        artifact.metadata["reason"] = f"missing: {p}"
        return False
    if artifact.kind == "file" and p.is_file() and p.stat().st_size == 0:
        logger.warning("artifact %s is empty: %s", artifact.artifact_id, p)
        artifact.status = models.ArtifactStatus.REJECTED
        artifact.metadata["reason"] = f"empty: {p}"
        return False
    # Only advance to GENERATED; never downgrade an already-ACCEPTED artifact
    # back to GENERATED on re-validation. completion.evaluate_task_completed
    # re-validates artifacts AFTER evaluate_acceptance -> mark_artifacts has
    # already moved them to ACCEPTED; unconditionally setting GENERATED here
    # would erase the acceptance verdict.
    if artifact.status != models.ArtifactStatus.ACCEPTED:
        artifact.status = models.ArtifactStatus.GENERATED
    artifact.metadata.pop("reason", None)
    return True


def validate_artifacts(
    artifacts: Iterable["models.Artifact"], task: "models.Task | None" = None
) -> bool:
    """Validate every artifact for a task (vacuously true when there are none)."""
    return all(validate_artifact(a, task) for a in artifacts)


def mark_artifacts(
    artifacts: Iterable["models.Artifact"], accepted: bool
) -> None:
    """Move generated artifacts to ACCEPTED / REJECTED after acceptance."""
    target = (
        models.ArtifactStatus.ACCEPTED if accepted else models.ArtifactStatus.REJECTED
    )
    for art in artifacts:
        if art.status in (
            models.ArtifactStatus.GENERATED,
            models.ArtifactStatus.UNDER_REVIEW,
            models.ArtifactStatus.ACCEPTED,
            models.ArtifactStatus.REJECTED,
        ):
            art.status = target


# ---------------------------------------------------------------------------
# Criteria evaluation
# ---------------------------------------------------------------------------
def _check_file(path: str, task, need_nonempty: bool) -> tuple[bool, str]:
    p = _resolve(path, task)
    if not p.exists():
        return False, f"{p} does not exist"
    if need_nonempty and p.is_file() and p.stat().st_size == 0:
        return False, f"{p} is empty"
    return True, f"{p} ok"


def _eval_string(crit: str, task, artifacts: list["models.Artifact"]) -> tuple[bool, str]:
    c = crit.strip()
    low = c.lower()

    if low in ("artifacts exist", "file exists", "files exist", "exists"):
        if not artifacts:
            return True, "no artifacts declared"
        missing = [a.path for a in artifacts if not _resolve(a.path, task).exists()]
        return (not missing), ("all artifacts present" if not missing else f"missing: {missing}")

    if low in ("non-empty", "nonempty", "artifacts non-empty"):
        if not artifacts:
            return True, "no artifacts declared"
        empty = [
            a.path
            for a in artifacts
            if _resolve(a.path, task).is_file()
            and _resolve(a.path, task).stat().st_size == 0
        ]
        return (not empty), ("all artifacts non-empty" if not empty else f"empty: {empty}")

    for prefix, need_nonempty in (
        ("file exists:", False),
        ("exists:", False),
        ("non-empty:", True),
        ("nonempty:", True),
        ("file non-empty:", True),
    ):
        if low.startswith(prefix):
            return _check_file(c[len(prefix):].strip(), task, need_nonempty)

    if low.startswith("file contains:") or low.startswith("contains:"):
        body = c.split(":", 1)[1]
        if "::" in body:
            path, needle = body.split("::", 1)
        elif " contains " in body:
            path, needle = body.split(" contains ", 1)
        else:
            return False, "malformed 'file contains' criterion (use path :: text)"
        p = _resolve(path.strip(), task)
        needle = needle.strip()
        if not p.is_file():
            return False, f"{p} is not a file"
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            return False, f"cannot read {p}: {exc}"
        return (needle in text), (
            f"{needle!r} found in {p.name}"
            if needle in text
            else f"{needle!r} not found in {p.name}"
        )

    if low.startswith("min_artifacts:") or low.startswith("min artifacts:"):
        try:
            n = int(c.split(":", 1)[1].strip())
        except ValueError:
            return False, "min_artifacts needs an integer"
        present = [a for a in artifacts if _resolve(a.path, task).exists()]
        return len(present) >= n, f"{len(present)}/{n} artifacts present"

    if low.startswith("command:"):
        cmd = c.split(":", 1)[1].strip()
        if not cmd:
            return False, "empty command"
        timeout = float(
            (task.metadata.get("acceptance_timeout") if task else None)
            or DEFAULT_COMMAND_TIMEOUT
        )
        try:
            proc = subprocess.run(
                cmd,
                shell=True,
                cwd=str(_worktree(task)),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return False, f"command timed out after {timeout:g}s: {cmd}"
        except OSError as exc:
            return False, f"command failed to start: {exc}"
        tail = (proc.stdout or proc.stderr or "").strip().splitlines()[-1:] or [""]
        return proc.returncode == 0, f"exit={proc.returncode} {tail[0]}".strip()

    # Descriptive criterion we do not know how to execute: record, don't fail.
    return True, "unverified (no executable check)"


def _eval_dict(crit: dict, task, artifacts) -> tuple[bool, str]:
    kind = str(crit.get("type", "")).lower()
    if kind in ("file_exists", "exists"):
        return _check_file(str(crit.get("path", "")), task, False)
    if kind in ("non_empty", "nonempty"):
        return _check_file(str(crit.get("path", "")), task, True)
    if kind == "contains":
        p = _resolve(str(crit.get("path", "")), task)
        needle = str(crit.get("text", ""))
        if not p.is_file():
            return False, f"{p} is not a file"
        return needle in p.read_text(encoding="utf-8", errors="replace"), (
            f"{needle!r} in {p.name}"
        )
    if kind == "min_artifacts":
        n = int(crit.get("count", 1))
        present = [a for a in artifacts if _resolve(a.path, task).exists()]
        return len(present) >= n, f"{len(present)}/{n} artifacts present"
    if kind == "command":
        return _eval_string(f"command: {crit.get('command', '')}", task, artifacts)
    return True, f"unverified criterion type {kind!r}"


def _eval_callable(crit: Callable, task) -> tuple[bool, str]:
    try:
        result = crit(task)
    except Exception as exc:  # noqa: BLE001 - a criterion must not crash acceptance
        return False, f"raised {type(exc).__name__}: {exc}"
    if isinstance(result, tuple):
        ok, evidence = result[0], result[1] if len(result) > 1 else ""
        return bool(ok), str(evidence)
    return bool(result), ""


def evaluate_criterion(crit: Any, task, artifacts) -> CriterionResult:
    label = _label(crit)
    if callable(crit) and not isinstance(crit, type):
        ok, ev = _eval_callable(crit, task)
    elif isinstance(crit, dict):
        ok, ev = _eval_dict(crit, task, artifacts)
    else:
        ok, ev = _eval_string(str(crit), task, artifacts)
    return CriterionResult(criterion=label, passed=bool(ok), evidence=ev)


def evaluate_acceptance(
    acceptance: "models.Acceptance",
    task: "models.Task | None" = None,
    force: bool = False,
) -> bool:
    """Execute every criterion, record evidence and set 'acceptance.passed'.

    Results are cached on the Acceptance object: re-evaluating an already
    evaluated acceptance is a no-op unless force=True. That keeps
    completion.evaluate_task_completed cheap when it is called repeatedly
    (per task and again during global convergence).
    """
    if acceptance.passed is not None and not force:
        return acceptance.passed

    artifacts = list(task.artifacts) if task is not None else []
    results: list[CriterionResult] = []
    for crit in acceptance.criteria:
        try:
            results.append(evaluate_criterion(crit, task, artifacts))
        except Exception as exc:  # noqa: BLE001
            logger.exception("criterion %r crashed", crit)
            results.append(CriterionResult(_label(crit), False, f"crashed: {exc}"))

    # Replace stale evaluation lines, keep any operator-supplied evidence.
    acceptance.evidence[:] = [
        e for e in acceptance.evidence if not e.startswith(_EVIDENCE_PREFIXES)
    ]
    for r in results:
        acceptance.evidence.append(
            f"{'PASS' if r.passed else 'FAIL'} {r.criterion}: {r.evidence}".rstrip()
        )

    passed = all(r.passed for r in results)  # empty criteria -> vacuously true
    acceptance.passed = passed

    if task is not None:
        task.metadata["acceptance_results"] = [r.as_dict() for r in results]
        if not passed:
            failed = [r for r in results if not r.passed]
            task.metadata["acceptance_failed"] = [r.criterion for r in failed]
        else:
            task.metadata.pop("acceptance_failed", None)
    mark_artifacts(artifacts, passed)
    logger.info(
        "acceptance %s -> passed=%s (%d criteria)",
        acceptance.task_id,
        passed,
        len(results),
    )
    return passed
