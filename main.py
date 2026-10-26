"""End-to-end demonstration of the link.md multi-agent framework."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import tempfile
import time
from pathlib import Path

try:
    from . import acceptance, completion, infrastructure, messaging, models, orchestrator
    from .agent_runtime import build_agent_config, run_task, SDK_AVAILABLE
except ImportError:  # pragma: no cover
    import acceptance  # type: ignore
    import completion  # type: ignore
    import infrastructure  # type: ignore
    import messaging  # type: ignore
    import models  # type: ignore
    import orchestrator  # type: ignore
    from agent_runtime import build_agent_config, run_task, SDK_AVAILABLE  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("demo")


def build_scenario(root: Path):
    agents = {
        "cmd": models.AgentInfo("cmd", "Commander", ["decompose", "review"], ["*"],
                                models.AgentRunStatus.ONLINE),
        "a": models.AgentInfo("a", "AgentA", ["write_file"], ["task-a"],
                              models.AgentRunStatus.ONLINE),
        "b": models.AgentInfo("b", "AgentB", ["write_file"], ["task-b"],
                              models.AgentRunStatus.ONLINE),
        "c": models.AgentInfo("c", "AgentC", ["write_file"], ["task-c"],
                              models.AgentRunStatus.ONLINE),
    }

    t_a = models.Task(task_id="task-a", title="Write module A", owner_agent_id="a",
                      priority=1, metadata={"worktree": str(root)})
    t_a.acceptance = models.Acceptance(task_id=t_a.task_id, criteria=[
        "file exists: a.txt", "non-empty: a.txt", "file contains: a.txt :: module A"])

    t_b = models.Task(task_id="task-b", title="Write module B (needs A accepted)",
                      owner_agent_id="b", priority=2,
                      metadata={"worktree": str(root)})
    t_b.dependencies.append(models.Dependency(
        task_id=t_b.task_id, depends_on=t_a.task_id, condition="accepted"))
    t_b.acceptance = models.Acceptance(
        task_id=t_b.task_id, criteria=["file exists: b.txt", "min_artifacts: 1"])

    t_c = models.Task(task_id="task-c", title="Optional probe", owner_agent_id="c",
                      priority=5, required=False, metadata={"worktree": str(root)})
    t_c.dependencies.append(models.Dependency(
        task_id=t_c.task_id, depends_on=t_a.task_id, condition="finished"))
    t_c.acceptance = models.Acceptance(
        task_id=t_c.task_id, criteria=["file exists: never.txt"])

    orch = orchestrator.Orchestrator(max_concurrency=2)
    for task in (t_a, t_b, t_c):
        orch.register(task)
    return agents, [t_a, t_b, t_c], orch


async def simulate_dispatch(task: models.Task, root: Path) -> None:
    logger.info("dispatch %s (%s) on %s", task.task_id, task.title, task.owner_agent_id)
    await asyncio.sleep(0.05)
    if task.task_id == "task-c":
        raise RuntimeError("simulated upstream failure")
    name = "a.txt" if task.task_id == "task-a" else "b.txt"
    (root / name).write_text("module " + name.split(".")[0].upper() + chr(10),
                             encoding="utf-8")
    task.artifacts.append(models.Artifact(task_id=task.task_id, path=name))
    passed = acceptance.evaluate_acceptance(task.acceptance, task, force=True)
    task.report_submitted = passed
    if not passed:
        task.status = models.TaskStatus.FAILED
        task.metadata["blocked_reason"] = "acceptance failed"
    else:
        task.status = models.TaskStatus.SUCCESS


def demo_approval(store, tasks, approval_mgr):
    t_b = next(t for t in tasks if t.task_id == "task-b")
    approval = approval_mgr.request(t_b, requested_by="cmd", reason="task-b touches prod")
    report = completion.global_report(tasks)
    logger.info("with pending approval -> complete=%s blocked=%s",
                report.complete, report.blocked)
    assert report.complete is False and t_b.task_id in report.blocked
    decided = approval_mgr.decide(approval.approval_id, approved=True,
                                  decided_by="operator", note="ok")
    logger.info("approval %s -> %s; task-b status=%s", decided.approval_id,
                decided.status.value, t_b.status.value)
    assert decided.status == models.ApprovalStatus.APPROVED
    assert t_b.status == models.TaskStatus.PLANNED


def demo_persistence_and_recovery(tmp: Path, tasks) -> None:
    path = tmp / "tasks.json"
    store = infrastructure.TaskStore(path)
    for task in tasks:
        store.save(task)
    logger.info("task store persisted -> %s (%d tasks)", path, len(store))
    reloaded = infrastructure.TaskStore(path)
    logger.info("task store reloaded -> %d tasks", len(reloaded))
    assert len(reloaded) == len(store)
    ghost = models.Task(task_id="task-ghost", title="crashed", owner_agent_id="a")
    ghost.status = models.TaskStatus.RUNNING
    ghost.runs.append(models.TaskRun(task_id=ghost.task_id, agent_id="a",
                                     status=models.ExecutionStatus.RUNNING,
                                     start_time=time.time()))
    reloaded.save(ghost)
    recovered = infrastructure.recover_incomplete_runs(reloaded)
    logger.info("recovered -> %s (status=%s, run=%s)", recovered,
                ghost.status.value, ghost.runs[-1].status.value)
    assert "task-ghost" in recovered
    assert ghost.status == models.TaskStatus.PLANNED
    assert ghost.runs[-1].status == models.ExecutionStatus.PROCESS_LOST


def demo_messaging() -> None:
    bus = messaging.MessageBus()
    seen = []
    bus.subscribe("a", seen.append)
    req = messaging.AgentMessage(msg_type=messaging.MessageType.TASK_ASSIGN,
                                 task_id="task-a", agent_id="a", sender="cmd")
    assert bus.publish(req) is True
    assert bus.publish(req) is False
    reply = bus.reply(req, messaging.MessageType.REPORT, status="done")
    assert bus.publish(reply) is True
    logger.info("bus: %d delivered, correlation=%s", len(seen),
                [m.msg_type.value for m in bus.correlate(req.msg_id)])


async def main() -> None:
    parser = argparse.ArgumentParser(description="link.md framework demo")
    parser.add_argument("--live", action="store_true", help="attempt a real SDK run")
    args = parser.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="linkmd-"))
    worktree = tmp / "worktree"
    worktree.mkdir()
    agents, tasks, orch = build_scenario(worktree)

    logger.info("deadlock check -> cycles=%s", orch.detect_deadlock())
    logger.info("topological order -> %s", orch.topo_order())

    store = infrastructure.TaskStore(tmp / "tasks.json")
    for t in tasks:
        store.save(t)
    approvals = infrastructure.ApprovalManager(
        store, infrastructure.StateManager(), path=tmp / "approvals.json")
    demo_approval(store, tasks, approvals)

    bus = messaging.MessageBus()
    audit = infrastructure.AuditLog(tmp / "audit.jsonl")
    metrics = infrastructure.Metrics()

    async def dispatch(task: models.Task) -> None:
        with infrastructure.bind_run("run-" + task.task_id):
            bus.publish(messaging.AgentMessage(
                msg_type=messaging.MessageType.PROGRESS, task_id=task.task_id,
                agent_id=task.owner_agent_id, sender="cmd", status="running"))
            await simulate_dispatch(task, worktree)
            metrics.inc("tasks_dispatched")
            metrics.observe("task_duration", 0.05)
            audit.record("task_finished", task_id=task.task_id,
                         status=task.status.value)
            store.save(task)
            bus.publish(messaging.AgentMessage(
                msg_type=(messaging.MessageType.DELIVER
                          if task.status == models.TaskStatus.SUCCESS
                          else messaging.MessageType.BLOCKED),
                task_id=task.task_id, agent_id=task.owner_agent_id,
                sender=task.owner_agent_id, status=task.status.value))

    if args.live:
        if SDK_AVAILABLE:
            for t in tasks:
                config = build_agent_config(
                    "You are " + agents[t.owner_agent_id].name + ". "
                    + (t.description or t.title),
                    allowed_tools=["view_file", "write_file", "run_command"])
                try:
                    await run_task(t, t.title, config)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("live run for %s failed: %s", t.task_id, exc)
        else:
            logger.warning("--live requested but SDK runtime unavailable; simulate instead")
            await orch.arun(dispatch)
    else:
        await orch.arun(dispatch)

    for t in tasks:
        report = completion.evaluate_task_completed(t)
        logger.info("task %s complete=%s missing=%s reason=%s", t.task_id,
                    report.complete, report.missing(), report.reason)

    global_report = completion.global_report(tasks)
    logger.info("global completion -> complete=%s (%d/%d required, optional_failed=%s)",
                global_report.complete, global_report.required_completed,
                global_report.required_total, global_report.optional_failed)
    logger.info("reasons -> %s", global_report.reasons)

    demo_messaging()
    demo_persistence_and_recovery(tmp, tasks)
    store.save_json(tmp / "tasks-final.json")
    logger.info("metrics -> %s", json.dumps(metrics.snapshot(), ensure_ascii=False))
    logger.info("audit events -> %d", len(audit.events))
    logger.info("artifacts -> %s", [a.path for t in tasks for a in t.artifacts])
    logger.info("workspace -> %s", tmp)
    assert global_report.complete, global_report.reasons


if __name__ == "__main__":
    asyncio.run(main())
