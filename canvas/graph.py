"""Canvas: the in-memory + on-disk model backing the IdeaCanvas UI.

One Canvas == one "idea project" (a dated session directory under
'sessions/'). It owns the eggs (each an SDK Agent, see egg.EggAgent), the
dependency edges between them, and the 总指挥 (Commander) that coordinates runs.

The orchestration logic reuses the demo framework so the link.md design is
reflected at runtime:

  * demo.orchestrator.Orchestrator  -> real concurrent DAG scheduling
  * demo.models                     -> Task / Artifact / Approval mirrors
  * demo.completion                 -> TaskCompleted rule + global convergence
  * demo.messaging                  -> AgentMessage 复命 (report) protocol
  * demo.infrastructure             -> state / store / approval / audit / metrics
"""
import asyncio
import datetime
import json
import os
import re
import shutil
import uuid
from pathlib import Path

from .config import SESSIONS_DIR, WORKSPACE, BACKEND, build_agent_config
from .context import ContextStore
from .egg import EggAgent, SDK_OK

PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "prompt": {"type": "string"},
                    "depends_on": {"type": "string"},
                },
                "required": ["title", "prompt"],
            },
        }
    },
    "required": ["tasks"],
}


def _extract_json(text):
    """Parse the first JSON object out of a model reply.

    Local models routinely wrap JSON in ``` fences or add a sentence before
    it, so plain json.loads() is too strict for planning.
    """
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1] if "\n" in raw else raw
        raw = raw.rsplit("```", 1)[0]
    try:
        return json.loads(raw)
    except ValueError:
        pass
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[idx:])
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    raise ValueError(f"planner reply is not JSON: {raw[:200]!r}")


def _env_float(name, default=None):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default

# --- demo framework (single-Agent SDK; see link.md section 六) --------------
try:
    from demo.orchestrator import Orchestrator
    from demo import models, completion, messaging, infrastructure, acceptance
    FRAMEWORK_OK = True
except Exception as _fw_err:  # pragma: no cover
    FRAMEWORK_OK = False
    _FW_ERR = _fw_err
    print(f"[canvas] demo framework unavailable: {_fw_err}")


class Commander:
    """The 总指挥: coordinates egg runs and narrates progress.

    SDK status (link.md 六.四): the SDK has no orchestrator; this Commander is a
    coordinator on top of the real SDK Agents (eggs) and the demo Orchestrator.
    """

    def __init__(self):
        self.busy = False
        self._agent = None
        self.transcript = []
        self._task = None

    def begin_run(self, kind, task):
        self.busy = True
        self._task = task

    def end_run(self, task):
        self.busy = False
        if self._task is task:
            self._task = None

    def interrupt(self):
        self.busy = False
        t = self._task
        if t is not None and not t.done():
            t.cancel()


class Canvas:
    def __init__(self):
        self.eggs = {}
        self.edges = {}
        self.commander = Commander()
        self.history_mode = "brief"
        self.session_id = None
        self.session_dir = None
        # A single local model server serialises generation anyway and resets
        # connections under two concurrent streams, so default to one egg at a
        # time there; cloud/other backends can really run eggs in parallel.
        default_conc = "1" if BACKEND == "openai" else "2"
        self.max_concurrency = max(
            1, int(os.environ.get("CANVAS_MAX_CONCURRENCY", default_conc)))
        self.max_retries = max(0, int(_env_float("CANVAS_MAX_RETRIES", 1) or 0))
        self.retry_backoff = _env_float("CANVAS_RETRY_BACKOFF", 0.5) or 0.5
        self._new_session()
        # framework bookkeeping
        self._context = ContextStore(ttl=_env_float("CANVAS_CONTEXT_TTL"))
        if FRAMEWORK_OK:
            self._state = infrastructure.StateManager()
            self._bind_session(self.session_id, self.session_dir, recover=False)
            # link.md 一.4: only the task owner (or the commander) may write.
            self._bus = messaging.MessageBus(authorize=self._authorize_message)
            self._bus.subscribe("*", self._on_message)
        else:
            self._state = self._store = self._bus = None
            self._approvals = self._audit = self._metrics = None

    # ---- session lifecycle ----
    def _new_session(self):
        """Allocate a session directory that is unique even within one second.

        Two canvases started in the same second (two tabs, or a quick restart)
        would otherwise share a session directory and therefore the same
        tasks.json / audit log, mixing their tasks together.
        """
        base = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        ts, n = base, 2
        while (SESSIONS_DIR / ts).exists():
            ts = f"{base}-{n}"
            n += 1
        self.session_id = ts
        self.session_dir = SESSIONS_DIR / ts
        self.session_dir.mkdir(parents=True, exist_ok=True)
        return ts

    def _bind_session(self, sid, d, recover=True):
        """(Re)bind durable stores to a session directory and recover state."""
        if not FRAMEWORK_OK:
            return []
        self._store = infrastructure.TaskStore(Path(d) / "tasks.json")
        self._approvals = infrastructure.ApprovalManager(
            self._store, self._state, path=Path(d) / "approvals.json")
        self._audit = infrastructure.AuditLog(Path(d) / "audit.jsonl")
        self._metrics = infrastructure.Metrics()
        recovered = []
        if recover:
            recovered = infrastructure.recover_incomplete_runs(
                self._store, self._state)
            if recovered:
                self._audit.record(
                    "session_recovered", session=sid, tasks=recovered)
        return recovered

    async def start(self):
        # Eggs start lazily on first command (so offline mode still serves).
        return

    async def shutdown(self):
        for e in self.eggs.values():
            try:
                await e.stop()
            except Exception:
                pass

    @classmethod
    def list_sessions(cls):
        out = []
        for d in sorted(SESSIONS_DIR.glob("*"), reverse=True):
            if d.is_dir():
                out.append({
                    "id": d.name,
                    "name": d.name,
                    "mtime": d.stat().st_mtime,
                })
        return out

    # ---- eggs ----
    async def generate(self, goal):
        """Decompose the goal into a DAG and create one egg per task.

        link.md 一.1: the commander (not a hard-coded list) decides the split.
        A real SDK Agent is asked for a JSON plan when a model is available;
        the fixed seeds remain as an offline fallback so the UI always works.
        """
        goal = (goal or "").strip()
        plan = []
        if goal and SDK_OK:
            try:
                timeout = _env_float("CANVAS_PLAN_TIMEOUT", 60.0)
                plan = await asyncio.wait_for(self._plan_with_agent(goal), timeout=timeout)
            except Exception as exc:  # noqa: BLE001 - planning is best-effort
                print(f"[canvas] planner unavailable ({exc}); using seeds")

        if not plan:
            seeds = [
                ("方案设计", "调研并设计实现方案（架构/接口/风险），产出规格说明 markdown。"),
                ("编码实现", "按方案实现可运行的代码，写入工作区并验证。"),
                ("评审验收", "审阅前两个蛋的产出，给出验收意见与改进清单。"),
            ]
            plan = [
                (title, f"{base}\n\n项目目标：{goal}" if goal else base, None)
                for title, base in seeds
            ]

        created = []
        by_title = {}
        for i, item in enumerate(plan):
            title, prompt = item[0], item[1]
            egg = await self.add_egg(title, prompt, x=120 + i * 260, y=160)
            created.append(egg)
            by_title[title] = egg

        linked = False
        for item, egg in zip(plan, created):
            dep = item[2] if len(item) > 2 else None
            if dep and dep in by_title and by_title[dep] is not egg:
                # edge points from the dependency to the dependent egg
                await self.add_edge(by_title[dep].id, egg.id)
                linked = True
        if not linked and len(created) >= 2:
            for first, second in zip(created, created[1:]):
                await self.add_edge(first.id, second.id)

        # Validate the decomposition with the orchestrator primitive.
        try:
            parent = models.Task(
                task_id=f"goal-{uuid.uuid4().hex[:6]}", title="goal",
                required=False, status=models.TaskStatus.SUCCESS,
                report_submitted=True, confirmed=True)
            parent.acceptance = models.Acceptance(task_id=parent.task_id, criteria=[])
            parent.acceptance.passed = True
            children = [
                models.Task(task_id=e.id, title=e.title, owner_agent_id=e.id)
                for e in created
            ]
            probe = Orchestrator(max_concurrency=self.max_concurrency)
            probe.decompose(parent, children)
            for item, egg in zip(plan, created):
                dep = item[2] if len(item) > 2 else None
                if dep and dep in by_title:
                    probe.add_dependency(egg.id, by_title[dep].id)
            probe.topo_order()
        except Exception as exc:  # noqa: BLE001
            print(f"[canvas] decomposition invalid: {exc}")

        self.save()
        return created

    async def _plan_with_agent(self, goal):
        """Ask a real SDK Agent for a JSON task plan (link.md 一.1)."""
        from google.antigravity import Agent

        config = build_agent_config(
            "You are the commander of a multi-agent project. Decompose the goal "
            "into 2-5 concrete tasks. Prefer mostly independent tasks; set "
            "depends_on to another task title when one must wait. "
            "You have NO tools and must NOT do the work yourself: never explain, "
            "never apologize, never write code. Answer with a single JSON object "
            'and nothing else, shaped exactly like {"tasks": [{"title": "...", '
            '"prompt": "...", "depends_on": "<other title or omitted>"}]}.',
            response_schema=PLAN_SCHEMA,
            tools=(),
        )
        async with Agent(config) as agent:
            response = await agent.chat(f"Goal: {goal}")
            text = await response.text()
        data = _extract_json(text)
        plan = []
        for item in (data.get("tasks") or [])[:5]:
            title = str(item.get("title") or "").strip()
            prompt = str(item.get("prompt") or "").strip()
            dep = item.get("depends_on")
            if title and prompt:
                plan.append((title, prompt, str(dep).strip() if dep else None))
        return plan

    async def add_egg(self, title, prompt, x=200, y=200):
        eid = f"e{uuid.uuid4().hex[:8]}"
        egg = EggAgent(
            eid, title, prompt, "egg", (x, y),
            session_dir=str(self.session_dir),
            bus=self._bus, store=self._store, state=self._state,
            metrics=self._metrics, audit=self._audit, context=self._context,
        )
        self.eggs[eid] = egg
        self.save()
        return egg

    async def delete_egg(self, eid):
        egg = self.eggs.pop(eid, None)
        if egg is not None:
            try:
                await egg.stop()
            except Exception:
                pass
        # drop edges touching this egg
        for k in [k for k, v in self.edges.items() if v["from"] == eid or v["to"] == eid]:
            self.edges.pop(k, None)
        if FRAMEWORK_OK:
            self._store.remove(eid)
        self.save()

    async def move(self, eid, x, y):
        egg = self.eggs.get(eid)
        if egg:
            egg.x, egg.y = x, y
            self.save()

    async def combine(self, ids):
        srcs = [self.eggs[i] for i in ids if i in self.eggs]
        if not srcs:
            return None
        combined = "\n\n".join(s.history_text() for s in srcs)
        prompt = (
            "以下是由多个子蛋产出的内容，请综合整理为一份统一的规格/实现说明：\n\n"
            + combined
        )
        new = await self.add_egg(f"组合：{srcs[0].title}", prompt, x=200, y=420)
        self.save()
        return new

    # ---- edges ----
    async def add_edge(self, frm, to):
        if frm == to or frm not in self.eggs or to not in self.eggs:
            return
        for v in self.edges.values():
            if v["from"] == frm and v["to"] == to:
                return
        eid = f"x{uuid.uuid4().hex[:6]}"
        self.edges[eid] = {"id": eid, "from": frm, "to": to}
        self.save()

    async def delete_edge(self, eid):
        self.edges.pop(eid, None)
        self.save()

    # ---- context for an egg ----
    def upstream_of(self, eid):
        """Ids of ALL ancestor eggs (transitive: edges pointing at it, then theirs).

        Transitive because a review egg needs the design, not only the code egg
        that consumed it. Cycles are tolerated here (the orchestrator rejects
        them separately) via the visited set.
        """
        parents = {}
        for v in self.edges.values():
            parents.setdefault(v["to"], []).append(v["from"])
        out, stack, seen = [], list(parents.get(eid, [])), {eid}
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            out.append(cur)
            stack.extend(parents.get(cur, []))
        return out

    def neighbor_context(self, eid):
        """Versioned shared context (link.md 二.9).

        In a DAG an egg must only see its UPSTREAM neighbours: handing it the
        output of a downstream egg would leak results that do not exist yet
        (and absolute paths from other worktrees the egg cannot read). Eggs
        with no edges at all keep the free-for-all behaviour so ad-hoc chat
        between unrelated eggs still shares context.

        Prefer the ContextStore snapshot: each block carries its task id and
        version, is TTL-filtered and size-capped. Fall back to raw histories
        for eggs that have not published a context yet.
        """
        upstream = self.upstream_of(eid)
        has_edges = bool(upstream) or any(
            v["from"] == eid for v in self.edges.values())
        if has_edges:
            sources = [i for i in upstream if i in self.eggs]
        else:
            sources = [i for i in self.eggs if i != eid]
        if not sources:
            return ""
        if self._context is not None:
            text = self._context.snapshot(include=tuple(sources))
            if text:
                return text
        parts = []
        for oid in sources:
            egg = self.eggs.get(oid)
            if egg is None:
                continue
            txt = egg.history_text(limit=2000)
            if txt:
                parts.append(f"【蛋 {egg.title}】\n{txt}")
        return "\n\n".join(parts)

    def _share_upstream(self, egg):
        """Copy the upstream eggs' deliverables into <worktree>/shared/.

        Eggs are isolated worktrees (link.md 一.6), so a downstream egg cannot
        read the files an upstream egg produced — a reviewer would literally
        have nothing to review. Before a run starts, copy the upstream tasks'
        recorded artifacts (their real deliverables, not caches or agent data)
        into a per-upstream subdirectory the egg is allowed to read.
        """
        ups = [u for u in self.upstream_of(egg.id) if u in self.eggs]
        if not ups or self._store is None:
            return []
        dest_root = Path(egg.worktree) / "shared"
        if dest_root.exists():
            try:
                dest_root.chmod(0o755)   # read-only from the previous run
            except OSError:
                pass
        copied = []
        for uid in ups:
            task = self._store.load(uid)
            if task is None or not task.artifacts:
                continue
            # keep unicode letters (Chinese titles) but drop path separators
            slug = "".join(c if (c.isalnum() or c in "._-") else "-"
                           for c in (task.title or uid))[:40].strip("-") or uid
            for art in task.artifacts:
                src = Path(art.path)
                if not src.is_file():
                    continue
                if any(p in ("agent_data", "__pycache__", "shared")
                       for p in src.parts):
                    continue
                dest_dir = dest_root / slug
                try:
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    dest_dir.chmod(0o755)   # may be 0555 from a previous run
                    dest = dest_dir / src.name
                    if dest.exists():
                        dest.chmod(0o644)
                    shutil.copy2(src, dest)
                    # Upstream output is a REFERENCE, not scratch space: a
                    # downstream egg must not "fix" the upstream worktree by
                    # editing this copy and silently lose the change.
                    dest.chmod(0o444)
                    dest_dir.chmod(0o555)
                except OSError:
                    continue
                copied.append((task.title or uid, f"shared/{slug}/{src.name}"))
        if copied:
            try:
                # The whole tree is INPUT: make it unwritable so an egg that
                # mistakes shared/ for its own workspace fails loudly instead
                # of losing its deliverable in a copy.
                dest_root.chmod(0o555)
            except OSError:
                pass
        return copied

    def _shared_note(self, copied):
        if not copied:
            return ""
        lines = ["【上游交付物（只读）已放入你工作区的 shared/ 目录】"]
        lines += [f"- {title}: {rel}" for title, rel in copied]
        lines += [
            "（注意：shared/ 是只读输入，写入会失败；你的交付物必须写到工作区",
            "根目录，例如 ./REVIEW.md。上文出现的其他蛋的绝对路径属于它们的",
            "工作区，你无权访问。）",
        ]
        return "\n".join(lines)

    # ---- message bus policy / commander inbox (link.md 一.4) ----
    def _authorize_message(self, msg):
        sender = (msg.sender or "").strip()
        if sender in ("", "commander", "cmd", "operator"):
            return True
        if self._store is None:
            return True
        task = self._store.load(msg.task_id)
        if task is None:
            return True
        agent = models.AgentInfo(agent_id=sender, name="", permissions=[])
        return infrastructure.can_agent_modify_task(agent, task, "write")

    def _on_message(self, msg):
        if msg.require_ack:
            try:
                self._bus.ack(msg.msg_id, agent_id="commander")
            except Exception:  # pragma: no cover
                pass
        if msg.msg_type in (
            messaging.MessageType.REPORT,
            messaging.MessageType.DELIVER,
            messaging.MessageType.BLOCKED,
            messaging.MessageType.CANCEL,
        ):
            self.commander.transcript.append({
                "role": "system",
                "kind": "message",
                "text": f"{msg.msg_type.value} {msg.task_id} {msg.status}".strip(),
                "msg_id": msg.msg_id,
                "correlation_id": msg.correlation_id,
                "timestamp": msg.timestamp,
            })

    # ---- task mirrors for the orchestrator (link.md 三.3) ----
    def _tasks_for(self, ids, reset=False):
        """Return durable Task mirrors for the given egg ids."""
        out = {}
        if not FRAMEWORK_OK:
            return out
        for i in ids:
            if i not in self.eggs:
                continue
            task = None if reset else self._store.load(i)
            if task is None:
                egg = self.eggs[i]
                task = models.Task(
                    task_id=egg.id, title=egg.title, description=egg.prompt,
                    owner_agent_id=egg.id,
                    metadata={"worktree": str(egg.worktree)})
                task.acceptance = models.Acceptance(
                    task_id=egg.id, criteria=["artifacts exist"])
                self._store.save(task)
            out[i] = task
        return out

    @staticmethod
    def _reset_task(task):
        """Clear a previous run so the task can be scheduled again."""
        pending_approval = (
            task.approval is not None
            and task.approval.status == models.ApprovalStatus.PENDING
        )
        task.status = (
            models.TaskStatus.WAITING_APPROVAL if pending_approval
            else models.TaskStatus.PLANNED
        )
        task.report_submitted = False
        task.report_payload = {}
        task.runs = []
        # Keep the version high-water mark: clearing the artifact list must not
        # make the next run restart at v1 (link.md 二.5 artifact versioning).
        previous_versions = {a.path: a.version for a in task.artifacts}
        if previous_versions:
            merged = dict(task.metadata.get("artifact_versions") or {})
            for path, ver in previous_versions.items():
                merged[path] = max(int(merged.get(path, 0)), int(ver))
            task.metadata["artifact_versions"] = merged
        task.artifacts = []
        if not pending_approval:
            task.approval = None
        task.metadata.pop("blocked_reason", None)
        task.metadata.pop("acceptance_failed", None)
        task.metadata.pop("acceptance_results", None)
        if task.acceptance is not None:
            task.acceptance.passed = None
            task.acceptance.evidence = []

    def _build_orchestrator(self, ids, reset=False):
        orch = Orchestrator(
            max_concurrency=self.max_concurrency,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
        )
        task_of = self._tasks_for(ids)
        for task in task_of.values():
            if reset:
                self._reset_task(task)
                self._store.save(task)
            orch.register(task)
        for v in self.edges.values():
            if v["from"] in task_of and v["to"] in task_of:
                # An edge is drawn left-to-right (source -> target), so the
                # TARGET is downstream and depends on the SOURCE: from runs
                # first, to starts once from is finished.
                orch.add_dependency(v["to"], v["from"], condition="finished")
        return orch, task_of

    # ---- ordering via the demo Orchestrator (link.md 二.7) ----
    def _ordered_targets(self, targets):
        ids = [t for t in targets if t in self.eggs]
        if not ids:
            return []
        if not FRAMEWORK_OK:
            return [self.eggs[i] for i in ids]
        orch, _ = self._build_orchestrator(ids)
        try:
            order = orch.topo_order()
        except Exception:
            order = ids
        seen = set()
        out = []
        for i in order + ids:
            if i in self.eggs and i not in seen:
                seen.add(i)
                out.append(self.eggs[i])
        return out

    # ---- run one egg + integrate the demo framework ----
    async def _run_egg(self, egg, instruction, ctx):
        yield {"k": "status", "egg": egg.id, "status": "running"}
        yield {"k": "instruct", "egg": egg.id, "text": instruction}
        async for tok in egg.run(instruction, context=ctx):
            yield {"k": "egg", "egg": egg.id, "text": tok}
        yield {"k": "end_egg", "egg": egg.id}
        yield {"k": "status", "egg": egg.id, "status": "idle"}
        # --- framework integration (link.md 一.2 / 五) ---
        # EggAgent.run() already mirrors the run into demo.models /
        # completion / messaging (Task, TaskRun, artifacts, REPORT).
        task = getattr(egg, "last_task", None)
        rep = getattr(egg, "last_completion_report", None)
        if task is not None and rep is not None:
            verdict = ("✅ 完成判定 complete=%s 缺失=%s"
                       % (rep.complete, rep.missing()))
            yield {"k": "say", "egg": "commander",
                   "text": f"[{egg.title}] {verdict}"}
            if not rep.complete:
                reason = task.metadata.get("blocked_reason") or rep.reason
                if reason:
                    yield {"k": "say", "egg": "commander",
                           "text": f"[{egg.title}] ❌ 未完成原因：{reason}"}

    # ---- concurrent graph execution ----
    async def _run_graph(self, eggs, instruction_fn, ctx_fn):
        """Execute eggs as a real DAG with bounded concurrency.

        A queue decouples the orchestrator's concurrent dispatches from this
        single async generator, so the websocket panel receives interleaved
        events from all running eggs.
        """
        if not FRAMEWORK_OK:
            for egg in eggs:
                async for ev in self._run_egg(egg, instruction_fn(egg), ctx_fn(egg)):
                    yield ev
            return

        q: asyncio.Queue = asyncio.Queue()
        ids = [e.id for e in eggs]
        orch, task_of = self._build_orchestrator(ids, reset=True)

        def _event(name, task, **kw):
            if name in ("started", "retry", "failed", "finished",
                        "timeout", "canceled"):
                try:
                    self._store.save(task)
                except Exception:  # pragma: no cover
                    pass
            q.put_nowait({"k": "framework", "egg": "commander",
                          "text": f"[{name}] {task.task_id}",
                          "event": name, "taskId": task.task_id, **kw})

        orch._on_event = _event

        async def dispatch(task):
            egg = self.eggs.get(task.task_id)
            if egg is None:
                return
            agent = models.AgentInfo(
                agent_id=egg.id, name=egg.title, permissions=[egg.id],
                status=models.AgentRunStatus.ONLINE)
            if not infrastructure.can_agent_modify_task(agent, task, "write"):
                task.metadata["blocked_reason"] = "permission denied"
                task.status = models.TaskStatus.BLOCKED
                await q.put({"k": "say", "egg": "commander",
                             "text": f"⛔ 无权执行「{egg.title}」"})
                return
            # link.md 一.4: TASK_ASSIGN is a real message; the egg's REPORT is
            # correlated with it so the commander can match reply to assignment.
            assign = messaging.AgentMessage(
                msg_type=messaging.MessageType.TASK_ASSIGN,
                task_id=task.task_id, agent_id=egg.id, sender="commander",
                status="assigned",
                payload={"title": task.title, "priority": task.priority,
                         "required": task.required,
                         "attempt": int(task.metadata.get("attempt", 1))})
            self._bus.publish(assign)
            egg.current_assign_id = assign.msg_id
            base_runs = list(task.runs)
            shared = self._share_upstream(egg)
            ctx = ctx_fn(egg) or ""
            note = self._shared_note(shared)
            if note:
                ctx = f"{ctx}\n\n{note}".strip()
                await q.put({"k": "say", "egg": "commander",
                             "text": f"📦 {egg.title} 收到上游交付物 "
                                     f"{len(shared)} 个（shared/）"})
            try:
                async for ev in self._run_egg(egg, instruction_fn(egg), ctx):
                    await q.put(ev)
            finally:
                egg.current_assign_id = None
            src = getattr(egg, "last_task", None)
            if src is not None:
                task.status = src.status
                task.artifacts = list(src.artifacts)
                task.acceptance = src.acceptance
                task.report_submitted = src.report_submitted
                # Keep previous attempts; drop only this attempt's RUNNING
                # placeholder (the egg records the authoritative run).
                task.runs = [
                    r for r in base_runs
                    if r.status != models.ExecutionStatus.RUNNING
                ] + list(src.runs)
                task.metadata.update(src.metadata)
            self._store.save(task)
            if task.status == models.TaskStatus.FAILED:
                # Let the orchestrator apply its retry/backoff policy to
                # task-level failures too (not just dispatch exceptions).
                raise RuntimeError(
                    task.metadata.get("blocked_reason") or "task failed")

        async def runner():
            try:
                await orch.arun(dispatch)
            except asyncio.CancelledError:
                orch.cancel_all("interrupted")
                raise
            except Exception as exc:  # noqa: BLE001
                await q.put({"k": "say", "egg": "commander",
                             "text": f"⚠ 调度失败：{exc}"})
            finally:
                await q.put(None)

        runner_task = asyncio.ensure_future(runner())
        self.commander.begin_run("graph", runner_task)
        try:
            while True:
                ev = await q.get()
                if ev is None:
                    break
                yield ev
        finally:
            self.commander.end_run(runner_task)
            if not runner_task.done():
                runner_task.cancel()
                orch.cancel_all("interrupted")

        # --- global convergence (link.md 二.12 / 四.第一层) ---
        report = completion.global_report(orch.all())
        self._metrics.set_gauge("convergence", 1.0 if report.complete else 0.0)
        self._metrics.inc("graph_runs")

        # 总指挥确认 (link.md 一.2 的第四态): a task that satisfies the
        # completion rule is explicitly acknowledged by the commander.
        confirmed = []
        for task in orch.all():
            if completion.evaluate_task_completed(task).complete:
                task.confirmed = True
                confirmed.append(task.task_id)
                self._store.save(task)
        if confirmed:
            self._audit.record("commander_confirmed", tasks=confirmed)
            self._metrics.inc("tasks_confirmed", len(confirmed))

        expired = self._approvals.expire()
        if expired:
            yield {"k": "say", "egg": "commander",
                   "text": f"⌛ 审批超时：{', '.join(a.approval_id for a in expired)}"}

        if report.complete:
            text = (f"✅ 全局收敛：{report.completed}/{report.total} 个任务完成"
                    f"（总指挥已确认 {len(confirmed)}）。")
        else:
            text = "⚠ 未收敛：" + "; ".join(report.reasons or ["required tasks incomplete"])
        yield {"k": "say", "egg": "commander", "text": text}
        yield {"k": "completion", "report": report.to_dict()}
        self._audit.record("graph_finished", session=self.session_id,
                           complete=report.complete, reasons=report.reasons)
        self.save()

    # ---- Commander run (chat to 总指挥) ----
    async def commander_run(self, text, targets):
        eggs = self._ordered_targets(targets or list(self.eggs.keys()))
        yield {"k": "say", "egg": "commander",
               "text": f"🧭 总指挥开始协调 {len(eggs)} 个蛋：{text}"}
        for egg in eggs:
            yield {"k": "say", "egg": "commander",
                   "text": f"→ 派发任务给「{egg.title}」"}
        async for ev in self._run_graph(
                eggs, lambda egg: text, lambda egg: self.neighbor_context(egg.id)):
            yield ev
        yield {"k": "say", "egg": "commander", "text": "🏁 总指挥协调完成。"}
        yield {"k": "end"}

    # ---- one-click run of selected eggs ----
    async def run_selected(self, targets):
        eggs = self._ordered_targets(targets)
        if not eggs:
            yield {"k": "say", "egg": "commander", "text": "⚠ 未选择任何蛋。"}
            yield {"k": "end"}
            return
        yield {"k": "say", "egg": "commander",
               "text": f"🚀 按依赖顺序运行 {len(eggs)} 个蛋（并发上限 {self.max_concurrency}）"}
        async for ev in self._run_graph(
                eggs, lambda egg: egg.prompt,
                lambda egg: self.neighbor_context(egg.id)):
            yield ev
        yield {"k": "say", "egg": "commander", "text": "🏁 运行结束。"}
        yield {"k": "end"}

    def interrupt_commander(self):
        self.commander.interrupt()

    # ---- approvals (link.md 二.10) ----
    def request_approval(self, egg_id, reason=""):
        if not FRAMEWORK_OK:
            return None
        tasks = self._tasks_for([egg_id])
        task = tasks.get(egg_id)
        if task is None:
            return None
        approval = self._approvals.request(
            task, requested_by="operator", reason=reason or "manual approval")
        self._audit.record("approval_requested", task_id=egg_id,
                           approval_id=approval.approval_id)
        self.save()
        return approval.to_dict()

    def decide_approval(self, approval_id, approved, note=""):
        if not FRAMEWORK_OK:
            return None
        approval = self._approvals.decide(
            approval_id, bool(approved), decided_by="operator", note=note)
        self._audit.record("approval_decided", approval_id=approval_id,
                           approved=bool(approved), task_id=approval.task_id)
        self.save()
        return approval.to_dict()

    def approvals(self):
        if not FRAMEWORK_OK:
            return []
        self._approvals.expire()
        return [a.to_dict() for a in self._approvals.all()]

    # ---- global completion view ----
    def completion_report(self):
        if not FRAMEWORK_OK:
            return {"complete": False, "reasons": ["framework unavailable"]}
        tasks = self._store.all()
        if not tasks:
            tasks = list(self._tasks_for(list(self.eggs)).values())
        return completion.global_report(tasks).to_dict()

    # ---- runtime observability (link.md 二.11) ----
    def runtime_view(self):
        """Full runtime view: tasks, running eggs, metrics, approvals, context."""
        if not FRAMEWORK_OK:
            return {"tasks": [], "eggs": [], "metrics": {}, "approvals": [],
                    "completion": self.completion_report(), "context": {}}
        tasks = []
        for task in self._store.all():
            report = completion.evaluate_task_completed(task)
            tasks.append({
                "task_id": task.task_id,
                "title": task.title,
                "status": task.status.value,
                "required": task.required,
                "confirmed": task.confirmed,
                "report_submitted": task.report_submitted,
                "blocked_reason": task.metadata.get("blocked_reason"),
                "attempt": int(task.metadata.get("attempt", 1)),
                "acceptance": task.acceptance.passed if task.acceptance else None,
                "artifacts": [
                    {"path": a.path, "status": a.status.value, "version": a.version}
                    for a in task.artifacts
                ],
                "complete": report.complete,
                "missing": report.missing(),
            })
        return {
            "tasks": tasks,
            "eggs": [
                {"id": e.id, "title": e.title, "busy": bool(getattr(e, "busy", False))}
                for e in self.eggs.values()
            ],
            "metrics": self._metrics.snapshot(),
            "approvals": self.approvals(),
            "completion": self.completion_report(),
            "context": self._context.to_dict() if self._context else {},
            "concurrency": {
                "max": self.max_concurrency,
                "max_retries": self.max_retries,
                "backoff": self.retry_backoff,
            },
        }

    def runtime_summary(self):
        """Compact runtime summary embedded in every canvas sync."""
        view = self.runtime_view()
        tasks = view["tasks"]
        return {
            "total": len(tasks),
            "running": [e["id"] for e in view["eggs"] if e["busy"]],
            "blocked": [
                {"task_id": t["task_id"], "reason": t["blocked_reason"]}
                for t in tasks if t["status"] == "blocked"
            ],
            "incomplete": [t["task_id"] for t in tasks if not t["complete"]],
            "confirmed": [t["task_id"] for t in tasks if t["confirmed"]],
            "complete": view["completion"].get("complete"),
            "metrics": view["metrics"],
        }

    # ---- serialization / view ----
    def view(self):
        return {
            "eggs": [e.spec() for e in self.eggs.values()],
            "edges": list(self.edges.values()),
            "commander": {"busy": self.commander.busy,
                          "session": self.session_id},
            "completion": self.completion_report(),
            "approvals": self.approvals(),
            "runtime": self.runtime_summary(),
        }

    def save(self):
        if self.session_dir is None:
            return
        data = {
            "session_id": self.session_id,
            "eggs": [e.spec() for e in self.eggs.values()],
            "edges": list(self.edges.values()),
            "commander_transcript": self.commander.transcript,
            "context": self._context.to_dict() if self._context else {},
        }
        try:
            (self.session_dir / "canvas.json").write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        if FRAMEWORK_OK:
            try:
                self._store.save_json(str(self.session_dir / "tasks.json"))
                self._approvals.save_json(str(self.session_dir / "approvals.json"))
            except Exception:
                pass

    async def load(self, sid):
        d = SESSIONS_DIR / sid
        if not d.is_dir():
            raise FileNotFoundError(sid)
        self.session_id = sid
        self.session_dir = d
        self.eggs.clear()
        self.edges.clear()
        data = json.loads((d / "canvas.json").read_text(encoding="utf-8"))
        if FRAMEWORK_OK:
            self._bind_session(sid, d)
        for spec in data.get("eggs", []):
            egg = EggAgent(
                spec["id"], spec["title"], spec.get("prompt", ""),
                spec.get("type", "egg"), (spec.get("x", 200), spec.get("y", 200)),
                session_dir=str(self.session_dir),
                conversation_id=spec.get("conversation_id"),
                bus=self._bus, store=self._store, state=self._state,
                metrics=self._metrics, audit=self._audit, context=self._context,
            )
            self.eggs[spec["id"]] = egg
        for e in data.get("edges", []):
            self.edges[e["id"]] = e
        self.commander.transcript = data.get("commander_transcript", [])
        if self._context is not None:
            self._context.load_dict(data.get("context", {}))
        return self.view()
