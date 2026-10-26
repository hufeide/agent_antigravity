"""Task orchestration & scheduling (link.md section 一.1, 二.7, 四.第一层).

SDK status: 否. The SDK has no orchestrator, DAG, scheduler, dependency resolver
or deadlock detector. Its only multi-agent primitive is LLM-driven sub-agent
spawning via start_subagent (types.py:351) coordinated by the external
harness, not by the Python SDK (link.md 六.1 / 六.7).

This module implements the orchestration layer:

  * topological scheduling + circular-dependency / deadlock detection
  * dependency conditions: finished | accepted | reported | completed
  * priority ordering and a real max_concurrency limit
  * per-task timeout and cancellation
  * dynamic append / re-plan (a new task becomes schedulable immediately)

Dispatch is delegated to a callable (sync via run(), async via arun()) that
executes the task with a real SDK Agent -- see agent_runtime.run_task and
canvas/egg.py.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import heapq
import inspect
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

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


class DeadlockError(RuntimeError):
    """Raised when a circular dependency (or an unsatisfiable graph) is found."""


@dataclass
class ScheduleSlot:
    """One scheduled task with its resolved priority + concurrency bucket."""

    task: "models.Task"
    concurrency_group: str = "default"


class Orchestrator:
    """Splits, schedules and resolves dependencies for a task graph."""

    def __init__(
        self,
        max_concurrency: int = 4,
        dispatch: Callable[["models.Task"], Any] | None = None,
        on_event: Callable[..., None] | None = None,
        task_timeout: float | None = None,
        max_retries: int = 0,
        retry_backoff: float = 0.5,
    ) -> None:
        self.max_concurrency = max(1, int(max_concurrency))
        self.task_timeout = task_timeout
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff = max(0.0, float(retry_backoff))
        self._dispatch = dispatch  # wire to agent_runtime.run_task / egg.run
        self._on_event = on_event
        self._tasks: dict[str, "models.Task"] = {}
        self._running_async: dict[str, asyncio.Future] = {}

    # ------------------------------------------------------------------
    # Registration / decomposition
    # ------------------------------------------------------------------
    def register(self, task: "models.Task") -> "models.Task":
        self._tasks[task.task_id] = task
        return task

    def unregister(self, task_id: str) -> bool:
        return self._tasks.pop(task_id, None) is not None

    def get(self, task_id: str) -> "models.Task | None":
        return self._tasks.get(task_id)

    def all(self) -> list["models.Task"]:
        return list(self._tasks.values())

    def add_dependency(
        self, task_id: str, depends_on: str, condition: str = "finished"
    ) -> None:
        task = self._tasks[task_id]
        for dep in task.dependencies:
            if dep.depends_on == depends_on:
                dep.condition = condition
                return
        task.dependencies.append(
            models.Dependency(
                task_id=task_id, depends_on=depends_on, condition=condition
            )
        )

    def decompose(self, parent: "models.Task", subtasks: list["models.Task"]) -> None:
        """Register children and make them depend on the parent's completion."""
        for sub in subtasks:
            if not any(d.depends_on == parent.task_id for d in sub.dependencies):
                sub.dependencies.append(
                    models.Dependency(task_id=sub.task_id, depends_on=parent.task_id)
                )
            self.register(sub)
        self.register(parent)

    def append(self, task: "models.Task") -> "models.Task":
        """Dynamic re-plan: register a task discovered while the graph runs."""
        self.register(task)
        self._emit("appended", task)
        return task

    # ------------------------------------------------------------------
    # Graph logic: cycle detection + topological order
    # ------------------------------------------------------------------
    def detect_deadlock(self) -> list[list[str]]:
        """Detect circular dependencies (A->B->C->A). Returns list of cycles."""
        graph: dict[str, list[str]] = defaultdict(list)
        for t in self._tasks.values():
            for d in t.dependencies:
                if d.depends_on in self._tasks:
                    graph[d.depends_on].append(d.task_id)

        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in self._tasks}
        stack: list[str] = []
        cycles: list[list[str]] = []

        def dfs(node: str) -> None:
            color[node] = GRAY
            stack.append(node)
            for nxt in graph.get(node, []):
                if color.get(nxt, WHITE) == GRAY:
                    idx = stack.index(nxt)
                    cycles.append(stack[idx:] + [nxt])
                elif color.get(nxt, WHITE) == WHITE:
                    dfs(nxt)
            stack.pop()
            color[node] = BLACK

        for tid in self._tasks:
            if color[tid] == WHITE:
                dfs(tid)
        return cycles

    def topo_order(self) -> list[str]:
        """Return a valid execution order, or raise DeadlockError on a cycle."""
        cycles = self.detect_deadlock()
        if cycles:
            raise DeadlockError(f"Circular dependency detected: {cycles}")
        indeg: dict[str, int] = {t: 0 for t in self._tasks}
        adj: dict[str, list[str]] = defaultdict(list)
        for t in self._tasks.values():
            for d in t.dependencies:
                if d.depends_on not in self._tasks:
                    continue
                adj[d.depends_on].append(d.task_id)
                indeg[d.task_id] += 1
        # priority queue: lower priority number = higher priority first
        pq = [(self._tasks[t].priority, t) for t in self._tasks if indeg[t] == 0]
        heapq.heapify(pq)
        order: list[str] = []
        while pq:
            _, node = heapq.heappop(pq)
            order.append(node)
            for nxt in adj[node]:
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    heapq.heappush(pq, (self._tasks[nxt].priority, nxt))
        if len(order) != len(self._tasks):
            raise DeadlockError("Unresolved dependencies remain after topo sort")
        return order

    # ------------------------------------------------------------------
    # Dependency conditions
    # ------------------------------------------------------------------
    def dependency_satisfied(self, dep: "models.Dependency") -> bool:
        upstream = self._tasks.get(dep.depends_on)
        if upstream is None:
            # External dependency (e.g. a human gate): assume satisfied.
            return True
        cond = (dep.condition or "finished").lower()
        if cond == "finished":
            # A task that is PLANNED again is waiting for a RETRY: its earlier
            # failed run must not count as "finished", otherwise downstream
            # tasks start in parallel with the retry and consume a half-done
            # upstream result.
            if upstream.status == models.TaskStatus.PLANNED:
                return False
            # BLOCKED / WAITING_APPROVAL are settled too: they will not make
            # progress on their own, so downstream must not deadlock behind
            # them (global convergence still reports them as incomplete).
            settled = _TERMINAL_TASK + (
                models.TaskStatus.BLOCKED,
                models.TaskStatus.WAITING_APPROVAL,
            )
            return upstream.status in settled or any(
                r.status in _TERMINAL_RUN for r in upstream.runs
            )
        if cond == "accepted":
            return bool(
                upstream.acceptance is not None
                and upstream.acceptance.passed is True
            )
        if cond == "reported":
            return bool(upstream.report_submitted)
        if cond == "completed":
            from . import completion

            return completion.evaluate_task_completed(upstream).complete
        return upstream.status == models.TaskStatus.SUCCESS

    def unmet_dependencies(self, task: "models.Task") -> list[str]:
        return [
            f"{d.depends_on}({d.condition})"
            for d in task.dependencies
            if not self.dependency_satisfied(d)
        ]

    def ready_tasks(self) -> list["models.Task"]:
        """Tasks that are PLANNED and whose dependency conditions all hold."""
        now = time.monotonic()
        ready = [
            t
            for t in self._tasks.values()
            if t.status == models.TaskStatus.PLANNED
            and not self.unmet_dependencies(t)
            and now >= float(t.metadata.get("retry_after", 0) or 0)
        ]
        return sorted(ready, key=lambda t: (t.priority, t.task_id))

    def _next_retry_delay(self) -> float | None:
        """Seconds until the next backoff expires (None when nothing waits)."""
        now = time.monotonic()
        delays = [
            float(t.metadata.get("retry_after", 0) or 0) - now
            for t in self._tasks.values()
            if t.status == models.TaskStatus.PLANNED and t.metadata.get("retry_after")
        ]
        delays = [d for d in delays if d > 0]
        return min(delays) if delays else None

    def stats(self) -> dict[str, Any]:
        """Runtime view: status counts, ready queue and running tasks."""
        by_status: dict[str, int] = {}
        for task in self._tasks.values():
            key = task.status.value
            by_status[key] = by_status.get(key, 0) + 1
        ready = self.ready_tasks()
        running = [
            t.task_id for t in self._tasks.values()
            if t.status == models.TaskStatus.RUNNING
        ]
        retries = sum(
            max(0, int(t.metadata.get("attempt", 1)) - 1)
            for t in self._tasks.values()
        )
        return {
            "total": len(self._tasks),
            "by_status": by_status,
            "ready": [t.task_id for t in ready],
            "queued": len(ready),
            "running": running,
            "max_concurrency": self.max_concurrency,
            "max_retries": self.max_retries,
            "retries": retries,
            "blocked": [
                {"task_id": t.task_id,
                 "reason": t.metadata.get("blocked_reason") or t.metadata.get("last_error")}
                for t in self._tasks.values()
                if t.status == models.TaskStatus.BLOCKED
            ],
        }

    def replan(self) -> list[str]:
        """Recompute readiness after a dynamic append; return newly ready ids."""
        return [t.task_id for t in self.ready_tasks()]

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------
    def _emit(self, event: str, task: "models.Task", **extra: Any) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event, task, **extra)
        except Exception:  # pragma: no cover - observers must not break scheduling
            logger.debug("on_event failed", exc_info=True)

    def _begin(self, task: "models.Task") -> None:
        task.metadata.setdefault("attempt", 1)
        task.metadata.pop("retry_after", None)
        task.status = models.TaskStatus.RUNNING
        run = models.TaskRun(
            task_id=task.task_id,
            agent_id=task.owner_agent_id,
            status=models.ExecutionStatus.RUNNING,
            start_time=time.time(),
        )
        task.runs.append(run)
        self._emit("started", task)

    def _finish(self, task: "models.Task") -> None:
        if task.status in (models.TaskStatus.PLANNED, models.TaskStatus.RUNNING):
            task.status = models.TaskStatus.SUCCESS
        run = task.runs[-1] if task.runs else None
        if run is not None and run.status == models.ExecutionStatus.RUNNING:
            run.status = models.ExecutionStatus.FINISHED
            run.end_time = time.time()
        self._emit("finished", task)

    def _fail(self, task: "models.Task", exc: BaseException) -> bool:
        """Record a failed attempt. Returns True when the task will be retried."""
        attempt = int(task.metadata.get("attempt", 1))
        run = task.runs[-1] if task.runs else None
        if run is not None:
            run.status = models.ExecutionStatus.ERROR
            run.end_time = time.time()
            run.error = str(exc)

        if attempt <= self.max_retries:
            task.metadata["attempt"] = attempt + 1
            task.metadata["last_error"] = f"{type(exc).__name__}: {exc}"
            task.metadata["retry_after"] = (
                time.monotonic() + self.retry_backoff * attempt
            )
            task.status = models.TaskStatus.PLANNED
            task.report_submitted = False
            logger.warning(
                "task %s failed (attempt %d/%d), retrying: %s",
                task.task_id, attempt, self.max_retries + 1, exc,
            )
            self._emit("retry", task, attempt=attempt + 1, error=str(exc))
            return True

        task.status = models.TaskStatus.FAILED
        task.metadata["blocked_reason"] = f"{type(exc).__name__}: {exc}"
        logger.error("task %s failed: %s", task.task_id, exc)
        self._emit("failed", task, error=str(exc))
        return False

    def _timeout(self, task: "models.Task", timeout: float) -> None:
        task.status = models.TaskStatus.FAILED
        task.metadata["blocked_reason"] = f"timeout after {timeout:g}s"
        run = task.runs[-1] if task.runs else None
        if run is not None:
            run.status = models.ExecutionStatus.TIMEOUT
            run.end_time = time.time()
            run.error = f"timeout after {timeout:g}s"
        logger.warning("task %s timed out after %ss", task.task_id, timeout)
        self._emit("timeout", task)

    def _cancel(self, task: "models.Task", reason: str = "canceled") -> None:
        task.status = models.TaskStatus.CANCELED
        task.metadata["blocked_reason"] = reason
        run = task.runs[-1] if task.runs else None
        if run is not None and run.status == models.ExecutionStatus.RUNNING:
            run.status = models.ExecutionStatus.CANCELED
            run.end_time = time.time()
            run.error = reason
        self._emit("canceled", task)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------
    def cancel(self, task_id: str, reason: str = "canceled by operator") -> bool:
        task = self._tasks.get(task_id)
        if task is None or task.status in _TERMINAL_TASK:
            return False
        fut = self._running_async.pop(task_id, None)
        if fut is not None and not fut.done():
            fut.cancel()
        self._cancel(task, reason)
        return True

    def cancel_all(self, reason: str = "canceled by operator") -> list[str]:
        return [
            t.task_id
            for t in self._tasks.values()
            if t.status not in _TERMINAL_TASK and self.cancel(t.task_id, reason)
        ]

    # ------------------------------------------------------------------
    # Schedulers
    # ------------------------------------------------------------------
    def _invoke_sync(self, dispatch: Callable, task: "models.Task") -> Any:
        result = dispatch(task)
        if inspect.isawaitable(result):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(result)
            raise RuntimeError(
                "dispatch returned an awaitable inside a running loop; "
                "use Orchestrator.arun() instead of run()"
            )
        return result

    def run(
        self,
        dispatch: Callable[["models.Task"], Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, "models.Task"]:
        """Execute the graph synchronously, honouring max_concurrency."""
        dispatch = dispatch or self._dispatch
        timeout = self.task_timeout if timeout is None else timeout
        if dispatch is None:
            for tid in self.topo_order():
                logger.warning("No dispatch wired; task %s not executed", tid)
            return self._tasks

        started: dict[str, float] = {}
        futures: dict[concurrent.futures.Future, "models.Task"] = {}
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrency
        ) as pool:
            while True:
                for task in self.ready_tasks():
                    if len(futures) >= self.max_concurrency:
                        break
                    self._begin(task)
                    started[task.task_id] = time.monotonic()
                    futures[pool.submit(self._invoke_sync, dispatch, task)] = task

                if not futures:
                    delay = self._next_retry_delay()
                    if delay is not None:
                        time.sleep(min(delay, 1.0))
                        continue
                    break

                done, _ = concurrent.futures.wait(
                    list(futures),
                    timeout=0.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for fut in done:
                    task = futures.pop(fut)
                    started.pop(task.task_id, None)
                    try:
                        fut.result()
                    except BaseException as exc:  # noqa: BLE001
                        self._fail(task, exc)
                    else:
                        self._finish(task)

                if timeout is not None:
                    now = time.monotonic()
                    for tid, t0 in list(started.items()):
                        if now - t0 <= timeout:
                            continue
                        for fut, task in list(futures.items()):
                            if task.task_id == tid:
                                fut.cancel()
                                futures.pop(fut, None)
                                break
                        started.pop(tid, None)
                        self._timeout(self._tasks[tid], timeout)

        self._check_stuck()
        return self._tasks

    async def _invoke_async(self, dispatch: Callable, task: "models.Task") -> Any:
        result = dispatch(task)
        if inspect.isawaitable(result):
            return await result
        return result

    async def arun(
        self,
        dispatch: Callable[["models.Task"], Awaitable[Any] | Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, "models.Task"]:
        """Execute the graph asynchronously, honouring max_concurrency."""
        dispatch = dispatch or self._dispatch
        timeout = self.task_timeout if timeout is None else timeout
        if dispatch is None:
            for tid in self.topo_order():
                logger.warning("No dispatch wired; task %s not executed", tid)
            return self._tasks

        running: dict[str, asyncio.Future] = {}
        started: dict[str, float] = {}
        while True:
            for task in self.ready_tasks():
                if len(running) >= self.max_concurrency:
                    break
                self._begin(task)
                started[task.task_id] = time.monotonic()
                fut = asyncio.ensure_future(self._invoke_async(dispatch, task))
                running[task.task_id] = fut
                self._running_async[task.task_id] = fut

            if not running:
                delay = self._next_retry_delay()
                if delay is not None:
                    await asyncio.sleep(min(delay, 1.0))
                    continue
                break

            await asyncio.wait(
                list(running.values()),
                timeout=0.1,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for tid, fut in list(running.items()):
                if not fut.done():
                    continue
                running.pop(tid, None)
                self._running_async.pop(tid, None)
                started.pop(tid, None)
                task = self._tasks[tid]
                if fut.cancelled():
                    if task.status != models.TaskStatus.CANCELED:
                        self._cancel(task)
                    continue
                exc = fut.exception()
                if exc is not None:
                    self._fail(task, exc)
                else:
                    self._finish(task)

            if timeout is not None:
                now = time.monotonic()
                for tid, t0 in list(started.items()):
                    if now - t0 <= timeout:
                        continue
                    fut = running.pop(tid, None)
                    self._running_async.pop(tid, None)
                    started.pop(tid, None)
                    if fut is not None and not fut.done():
                        fut.cancel()
                    self._timeout(self._tasks[tid], timeout)

        self._check_stuck()
        return self._tasks

    # ------------------------------------------------------------------
    # Convergence guard
    # ------------------------------------------------------------------
    def _check_stuck(self) -> None:
        stuck = [
            t for t in self._tasks.values() if t.status == models.TaskStatus.PLANNED
        ]
        if not stuck:
            return
        details = "; ".join(
            f"{t.task_id} waits on {self.unmet_dependencies(t)}" for t in stuck
        )
        raise DeadlockError(f"Unschedulable tasks remain: {details}")
