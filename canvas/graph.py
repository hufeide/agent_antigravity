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
import inspect
import re
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from .config import (SESSIONS_DIR, WORKSPACE, BACKEND, build_agent_config,
                     backend_options)
from .trace import GlobalTrace
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
                    "depends_on": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["title", "prompt"],
            },
        }
    },
    "required": ["tasks"],
}

# The commander (总指挥) uses this schema to decide what to do with an
# operator message. Instead of blindly forwarding the message to every egg,
# the commander first reasons about the message and picks an action:
#   answer   - the message is a question/clarification -> answer it directly
#   replan   - the message reveals a gap or new requirement -> create new eggs
#   forward  - the message is relevant to specific eggs -> pass tailored info
#   run      - the message is an explicit execute command -> run the plan
COMMANDER_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["answer", "replan", "forward", "run"],
        },
        "reasoning": {"type": "string"},
        "reply": {"type": "string"},
        "plan": {
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "prompt": {"type": "string"},
                            "depends_on": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": ["title", "prompt"],
                    },
                }
            },
        },
        "targets": {
            "type": "array",
            "items": {"type": "string"},
        },
        "instructions": {
            "type": "object",
            "additionalProperties": {"type": "string"},
        },
        "remove": {
            "type": "array",
            "description": "Egg titles or IDs to remove when simplifying/replacing the plan. Only use when the operator explicitly asks to remove or replace existing work.",
            "items": {"type": "string"},
        },
    },
    "required": ["action", "reasoning", "reply"],
}


def _extract_json(text):
    """Parse the first JSON object or array out of a model reply.

    Local models routinely wrap JSON in ``` fences or add a sentence
    before it, so plain json.loads() is too strict for planning.
    """
    raw = (text or "").strip()
    # Strip BOM / zero-width characters some models emit
    raw = raw.lstrip("\ufeff​‌‍")
    if raw.startswith("```"):
        # Remove opening fence line (```json, ```, etc.)
        raw = raw.split("\n", 1)[-1] if "\n" in raw else raw
        # Remove closing fence
        raw = raw.rsplit("```", 1)[0]
    try:
        return json.loads(raw)
    except ValueError:
        pass
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch not in "{[":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[idx:])
        except ValueError:
            continue
        if isinstance(obj, (dict, list)):
            return obj
    raise ValueError(f"planner reply is not JSON: {raw[:200]!r}")



async def _response_json(response):
    """Read JSON from structured output first, then fall back to response text."""
    errors = []
    for name in ("structured_output", "parsed"):
        candidate = getattr(response, name, None)
        if candidate is None:
            continue
        try:
            if callable(candidate):
                candidate = candidate()
            if inspect.isawaitable(candidate):
                candidate = await candidate
            if isinstance(candidate, dict):
                return candidate
            if isinstance(candidate, str) and candidate.strip():
                try:
                    return _extract_json(candidate)
                except ValueError as exc:
                    errors.append(str(exc))
            for converter_name in ("model_dump", "dict"):
                converter = getattr(candidate, converter_name, None)
                if callable(converter):
                    converted = converter()
                    if isinstance(converted, dict):
                        return converted
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")
    try:
        value = getattr(response, "text", None)
        if callable(value):
            value = value()
        if inspect.isawaitable(value):
            value = await value
        if isinstance(value, str) and value.strip():
            return _extract_json(value)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"text: {exc}")
    detail = "; ".join(errors[-3:])
    raise ValueError("SDK response did not contain a valid JSON object" +
                     (f": {detail}" if detail else ""))


def _assert_acyclic(nodes, edges):
    """Return a stable topological order; raise instead of executing a cyclic DAG."""
    ordered = list(dict.fromkeys(nodes))
    node_set = set(ordered)
    adjacency = {node: [] for node in ordered}
    indegree = {node: 0 for node in ordered}
    seen = set()
    for source, target in edges:
        if source not in node_set or target not in node_set:
            continue
        if source == target:
            raise ValueError(f"dependency cycle: {source} depends on itself")
        if (source, target) in seen:
            continue
        seen.add((source, target))
        adjacency[source].append(target)
        indegree[target] += 1
    ready = [node for node in ordered if indegree[node] == 0]
    result = []
    while ready:
        node = ready.pop(0)
        result.append(node)
        for child in adjacency[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
    if len(result) != len(ordered):
        cyclic = [str(node) for node in ordered if indegree[node] > 0]
        raise ValueError("dependency cycle detected: " + ", ".join(cyclic))
    return result


def _normalize_plan_items(items, available_titles=None, max_tasks=5, min_tasks=1):
    """Validate task titles, prompts, prerequisites and cycles before mutation."""
    if not isinstance(items, list):
        raise ValueError("plan.tasks must be a list")
    if not min_tasks <= len(items) <= max_tasks:
        raise ValueError(f"plan must contain {min_tasks}–{max_tasks} tasks")
    available = set(available_titles or ())
    result, titles = [], set()
    for index, item in enumerate(items):
        if isinstance(item, dict):
            title = str(item.get("title") or "").strip()
            prompt = str(item.get("prompt") or "").strip()
            deps = item.get("depends_on") or []
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            title = str(item[0] or "").strip()
            prompt = str(item[1] or "").strip()
            deps = item[2] if len(item) > 2 else []
            deps = deps or []
        else:
            raise ValueError(f"task #{index + 1} is not an object")
        if not title or not prompt:
            raise ValueError(f"task #{index + 1} needs a non-empty title and prompt")
        if title in titles:
            raise ValueError(f"duplicate task title: {title}")
        if isinstance(deps, str):
            deps = [deps]
        if not isinstance(deps, (list, tuple, set)):
            raise ValueError(f"depends_on for {title!r} must be a list of titles")
        clean = []
        for dep in deps:
            dep = str(dep or "").strip()
            if not dep:
                continue
            if dep == title:
                raise ValueError(f"task {title!r} cannot depend on itself")
            if dep not in clean:
                clean.append(dep)
        titles.add(title)
        result.append((title, prompt, clean))
    known = titles | available
    for title, _prompt, deps in result:
        missing = [dep for dep in deps if dep not in known]
        if missing:
            raise ValueError(f"task {title!r} references unknown dependencies: {', '.join(missing)}")
    _assert_acyclic(list(available) + [title for title, _, _ in result],
                    [(dep, title) for title, _, deps in result for dep in deps])
    return result


def _node_depths(nodes, edges):
    """Longest-path depth (distance from a root) for each node in a DAG.

    Roots (no incoming edges) have depth 0. Returns {node: depth}; isolated or
    unreachable nodes get 0. Assumes an acyclic graph (callers validate first).
    """
    node_set = set(nodes)
    adjacency = {n: [] for n in node_set}
    indegree = {n: 0 for n in node_set}
    for source, target in edges:
        if source in node_set and target in node_set and source != target:
            adjacency[source].append(target)
            indegree[target] += 1
    ready = [n for n in nodes if indegree[n] == 0]
    order = []
    ind = dict(indegree)
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in adjacency[node]:
            ind[child] -= 1
            if ind[child] == 0:
                ready.append(child)
    depth = {n: 0 for n in node_set}
    for node in order:
        d = depth[node] + 1
        for child in adjacency[node]:
            if depth[child] < d:
                depth[child] = d
    return depth


def _prune_plan_unique(plan):
    """Keep a unique path to every task (in-degree <= 1) at the title level.

    A node reachable by two paths (e.g. A->C and A->B->C) breaks the rule, so
    each task keeps only its DEEPEST prerequisite — this preserves the longest
    chain and does not orphan intermediate tasks. Returns
    (pruned_plan, dropped) where dropped lists (task, removed_dep) pairs.
    """
    titles = [title for title, _prompt, _deps in plan]
    edges = [(dep, title) for title, _prompt, deps in plan for dep in deps]
    depth = _node_depths(titles, edges)
    pruned, dropped = [], []
    for title, prompt, deps in plan:
        if len(deps) > 1:
            keep = max(deps, key=lambda dep: depth.get(dep, 0))
            pruned.append((title, prompt, [keep]))
            for dep in deps:
                if dep != keep:
                    dropped.append((title, dep))
        else:
            pruned.append((title, prompt, list(deps)))
    return pruned, dropped


def _enforce_unique_paths(nodes, edges, task_nodes):
    """Keep a unique path to every task node (in-degree <= 1) at the id level.

    For each node in task_nodes with more than one incoming edge, keep only the
    edge from its DEEPEST parent and drop the rest; non-task nodes are left
    untouched. Returns (kept_edges, dropped) where dropped lists (parent, task)
    id pairs removed, for reporting.
    """
    task_set = set(task_nodes)
    depth = _node_depths(list(nodes), edges)
    parents = {t: [] for t in task_set}
    kept = []
    for source, target in edges:
        if target in task_set:
            parents[target].append(source)
        else:
            kept.append((source, target))
    dropped = []
    for target in task_nodes:
        cand = parents[target]
        if not cand:
            continue
        if len(cand) == 1:
            kept.append((cand[0], target))
            continue
        best = cand[0]
        best_depth = depth.get(best, 0)
        for p in cand[1:]:
            if depth.get(p, 0) > best_depth:
                best = p
                best_depth = depth.get(p, 0)
        kept.append((best, target))
        for p in cand:
            if p != best:
                dropped.append((p, target))
    return kept, dropped


def _env_float(name, default=None):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name, default=None):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _usage_text(response):
    """One-line token usage summary for a ChatResponse (for trace records)."""
    try:
        u = response.usage_metadata
        if u is None:
            return "无用量信息"
        return (f"prompt={getattr(u, 'prompt_token_count', 0)} "
                f"cached={getattr(u, 'cached_content_token_count', 0)} "
                f"candidates={getattr(u, 'candidates_token_count', 0)} "
                f"thoughts={getattr(u, 'thoughts_token_count', 0)} "
                f"total={getattr(u, 'total_token_count', 0)}")
    except Exception:
        return "?"


def _server_root():
    """llama.cpp/LM Studio native endpoints (/slots, /abort) live at the
    SERVER ROOT, not under the OpenAI-compatible /v1 prefix that
    base_url carries — request them with the /v1 suffix stripped."""
    base = str(backend_options().get("base_url") or "").rstrip("/")
    if not base:
        return ""
    p = urllib.parse.urlparse(base)
    return f"{p.scheme}://{p.netloc}" if p.path else base


def _abort_server_request():
    """Best-effort: ask the model server to abort its in-flight request.

    ``response.cancel()`` only stops the SDK-side reader; llama.cpp cannot
    detect a client disconnect mid-prefill, so an aborted request keeps
    burning its slot (a "zombie"). POST /abort (llama.cpp native; LM Studio
    may answer 404) releases it server-side when the endpoint exists.
    """
    root = _server_root()
    if not root:
        return
    for path in ("/abort", "/api/abort"):
        try:
            req = urllib.request.Request(
                root + path, data=b"{}",
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=3) as r:
                if r.status < 400:
                    return
        except Exception:
            continue


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
        self._break_pending = False   # next note_say starts a fresh entry

    def note_user(self, text):
        """Persist a user message so the commander chat survives save/load."""
        self.transcript.append({
            "role": "user", "kind": "text", "text": text,
            "ts": datetime.datetime.now().isoformat(timespec="milliseconds"),
        })

    def note_say(self, text, role="assistant"):
        """Persist one narration event; consecutive same-role says merge into a
        single entry (a logical message streams as many says), say_break forces
        a new one. role="system" notes become kind="status": they replay in the
        轨迹 but stay out of the chat panel."""
        kind = "text" if role in ("user", "assistant") else "status"
        last = self.transcript[-1] if self.transcript else None
        if (last and last.get("kind") == kind and last.get("role") == role
                and not self._break_pending):
            last["text"] = (last.get("text") or "") + text
        else:
            self.transcript.append({
                "role": role, "kind": kind, "text": text,
                "ts": datetime.datetime.now().isoformat(timespec="milliseconds"),
            })
        self._break_pending = False

    def note_break(self):
        self._break_pending = True

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
        # 全局模型调用轨迹：蛋 + 总指挥 + 规划器的每一次模型对话都汇入这里，
        # 前端「🌐 全局」标签按时间顺序流式展示（见 trace.py）。
        self._trace = GlobalTrace()
        self.history_mode = "brief"
        self.session_id = None
        self.session_dir = None
        # A single local model server serialises generation anyway and resets
        # connections under two concurrent streams, so default to one egg at a
        # time there; cloud/other backends can really run eggs in parallel.
        default_conc = "2" if BACKEND == "openai" else "4"
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
        # 新会话目录对应新的全局轨迹文件（旧会话的轨迹留在旧目录）。
        self._trace.set_path(self.session_dir / "global_trace.jsonl")
        self._trace.load()
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
            if not d.is_dir():
                continue
            # Count eggs from the session's canvas.json (best-effort: a
            # partially-written file must not break the picker).
            eggs = 0
            try:
                data = json.loads((d / "canvas.json").read_text(encoding="utf-8"))
                eggs = len(data.get("eggs", []))
            except Exception:
                pass
            # The session id IS the time-based name ("YYYYMMDD-HHMMSS" or
            # "YYYYMMDD-HHMMSS-N" for same-second collisions). Derive a readable
            # creation time from it so the picker shows a real timestamp instead
            # of "undefined".
            created_at = cls._session_time_label(d.name)
            out.append({
                "id": d.name,
                "name": d.name,
                "label": "",
                "created_at": created_at,
                "eggs": eggs,
                "mtime": d.stat().st_mtime,
            })
        return out

    @staticmethod
    def _session_time_label(name):
        """Format a session id ("YYYYMMDD-HHMMSS[-N]") as "YYYY-MM-DD HH:MM"."""
        core = name[:15]  # "YYYYMMDD-HHMMSS"
        try:
            return datetime.datetime.strptime(
                core, "%Y%m%d-%H%M%S").strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return name

    # ---- eggs ----
    async def generate(self, goal):
        """Plan and create a validated dependency DAG for the requested goal."""
        goal = (goal or "").strip()
        plan = []
        if goal and SDK_OK:
            try:
                timeout = _env_float("CANVAS_PLAN_TIMEOUT", 240.0)
                raw_plan = await asyncio.wait_for(self._plan_with_agent(goal), timeout=timeout)
                plan = _normalize_plan_items(raw_plan)
            except Exception as exc:  # noqa: BLE001
                print(f"[canvas] planner unavailable or invalid ({exc}); using seeds")
                plan = []
        if not plan:
            plan = [
                ("方案设计", "调研并设计实现方案（架构/接口/风险），产出规格说明 markdown。", []),
                ("编码实现", "按方案实现可运行的代码，写入工作区并验证。", ["方案设计"]),
                ("评审验收", "审阅方案与代码的实际产出，验证交付物并给出验收意见与改进清单。", ["方案设计", "编码实现"]),
            ]
            if goal:
                plan = [(title, f"{prompt}\n\n项目目标：{goal}", deps)
                        for title, prompt, deps in plan]
        plan = _normalize_plan_items(plan)
        plan, dropped = _prune_plan_unique(plan)
        if dropped:
            detail = "; ".join(f"{task} 移除重复上游 {dep}"
                               for task, dep in dropped)
            print(f"[canvas] 依赖收敛为唯一路径：{detail}")
        created, by_title = [], {}
        for i, (title, prompt, _deps) in enumerate(plan):
            row, col = divmod(i, 3)
            egg = await self.add_egg(
                title, prompt,
                x=120 + col * 380, y=160 + row * 240)
            created.append(egg)
            by_title[title] = egg
        for title, _prompt, deps in plan:
            for dep in deps:
                await self.add_edge(by_title[dep].id, by_title[title].id)
        try:
            parent = models.Task(task_id=f"goal-{uuid.uuid4().hex[:6]}", title="goal",
                                 required=False, status=models.TaskStatus.SUCCESS,
                                 report_submitted=True, confirmed=True)
            parent.acceptance = models.Acceptance(task_id=parent.task_id, criteria=[])
            parent.acceptance.passed = True
            children = [models.Task(task_id=e.id, title=e.title, owner_agent_id=e.id)
                        for e in created]
            probe = Orchestrator(max_concurrency=self.max_concurrency)
            probe.decompose(parent, children)
            for title, _prompt, deps in plan:
                for dep in deps:
                    probe.add_dependency(by_title[title].id, by_title[dep].id,
                                         condition="success")
            probe.topo_order()
        except Exception as exc:  # noqa: BLE001
            new_ids = {egg.id for egg in created}
            for edge_id, edge in list(self.edges.items()):
                if edge["from"] in new_ids or edge["to"] in new_ids:
                    self.edges.pop(edge_id, None)
            for egg in created:
                await self.delete_egg(egg.id)
            raise ValueError(f"任务依赖图无法被调度器接受：{exc}") from exc
        self.save()
        return created

    async def _plan_with_agent(self, goal):
        """Ask a real SDK Agent for a validated JSON task plan.

        response_schema is a no-op on the local OpenAI backend: the harness
        never receives it, so structured_output() always returns None and the
        model's JSON must be parsed from the text stream. We drain the stream
        once per attempt and try both the answer text and the reasoning
        (thoughts) — reasoning models sometimes put the JSON only in
        reasoning_content, leaving content empty.

        If parsing fails, we retry in the SAME agent session with a
        corrective prompt so the model sees its own mistake and can fix it.
        """
        from google.antigravity import Agent, types
        config = build_agent_config(
            """You are the commander of a multi-agent project. Plan the work; do not execute it.
Rules:
1. Decompose the goal into 2–5 concrete tasks, each with a unique, non-empty title, clear prompt, and expected deliverable.
2. Prefer parallel execution for independent tasks.
3. `depends_on` lists the prerequisites that must successfully complete before the task starts; use [] if none.
4. Dependencies use exact task titles and contain no cycles.
5. Each task depends on AT MOST ONE other task: the whole plan must form a single linear chain (or a few independent chains), never a diamond. No task may be reachable from a root by two different paths (e.g. A->C and A->B->C are forbidden — pick one).
6. When a task needs outputs from several upstream tasks, chain them in order (A->B->C) instead of making C depend on both A and B.
7. Never assume planned outputs already exist.
8. Return exactly one JSON object matching the schema, with no Markdown or extra text.""",
            response_schema=PLAN_SCHEMA, tools=(),
            capabilities=types.CapabilitiesConfig(
                enabled_tools=types.BuiltinTools.none(), enable_subagents=False,
                agent_behavior=types.AgentBehavior.MINIMAL))
        max_retries = _env_int("CANVAS_PLAN_RETRIES", 2)
        async with Agent(config) as agent:
            data = None
            for attempt in range(max_retries + 1):
                if attempt == 0:
                    prompt = f"Goal: {goal}"
                else:
                    prompt = (
                        f"Your previous reply could not be parsed as JSON. "
                        f"Reply with EXACTLY one JSON object matching the "
                        f"schema — no Markdown fences, no explanation, no "
                        f"extra text.\nGoal: {goal}")
                response = await agent.chat(prompt)
                if self._trace is not None:
                    # 全局轨迹：规划器每次尝试的模型输入。
                    self._trace.append("planner", "规划器", "user",
                                       "planner_prompt", text=prompt)
                # resolve() drains the stream until the server sends a terminal
                # step; a zombie prefill / dead connection never ends it, so
                # bound the wait. Run it as a shielded task so a timeout can
                # still send response.cancel() (halt_request) while the
                # stream is live, then free the server slot via /abort (the
                # same watchdog egg.py uses).
                resolve_task = asyncio.ensure_future(response.resolve())
                try:
                    raw_chunks = await asyncio.wait_for(
                        asyncio.shield(resolve_task),
                        timeout=_env_float("CANVAS_PLAN_RESOLVE_TIMEOUT", 210.0))
                except asyncio.TimeoutError:
                    try:
                        await response.cancel()
                    except Exception:
                        pass
                    _abort_server_request()
                    resolve_task.cancel()
                    raise
                texts = "".join(
                    c.text for c in raw_chunks
                    if isinstance(c, types.Text))
                thoughts = "".join(
                    c.text for c in raw_chunks
                    if isinstance(c, types.Thought))
                # 诊断日志：打印 chunk 类型分布与 text/thoughts 长度
                from collections import Counter
                type_counts = Counter(type(c).__name__ for c in raw_chunks)
                print(f"[canvas] planner attempt {attempt + 1}: "
                      f"chunks={dict(type_counts)}, "
                      f"text_len={len(texts)}, thoughts_len={len(thoughts)}, "
                      f"stop_reason={response.stop_reason}")
                if self._trace is not None and (texts or thoughts):
                    self._trace.append("planner", "规划器", "assistant",
                                       "planner_reply", text=texts or thoughts)
                parsed = None
                for candidate in (texts, thoughts):
                    if candidate.strip():
                        try:
                            parsed = _extract_json(candidate)
                            break
                        except ValueError:
                            continue
                if isinstance(parsed, dict) and isinstance(parsed.get("tasks"), list):
                    data = parsed
                    break
                # 记录最后一次失败的回复，便于排查
                last_reply = texts or thoughts
                if attempt < max_retries:
                    print(f"[canvas] planner attempt {attempt + 1}: "
                          f"no valid JSON (got {len(last_reply)} chars); "
                          f"retrying with corrective prompt")
            if data is None:
                raise ValueError(
                    f"planner produced no valid JSON after {max_retries + 1} "
                    f"attempts (last reply: {last_reply[:200]!r})")
            if self._trace is not None:
                self._trace.append("planner", "规划器", "system", "turn_end",
                                   text=f"stop_reason={response.stop_reason} usage={_usage_text(response)}")
        return _normalize_plan_items(data["tasks"], min_tasks=2)

    async def add_egg(self, title, prompt, x=None, y=None):
        # Default position: below every existing egg so a manually-added egg
        # never overlaps a generated/parent egg (egg height ≈ 240px + margin).
        if x is None:
            x = 200
        if y is None:
            y = max((e.y for e in self.eggs.values()), default=160) + 280
        eid = f"e{uuid.uuid4().hex[:8]}"
        egg = EggAgent(
            eid, title, prompt, "egg", (x, y),
            session_dir=str(self.session_dir),
            bus=self._bus, store=self._store, state=self._state,
            metrics=self._metrics, audit=self._audit, context=self._context,
            trace=self._trace,
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
        # Name combines ALL source egg titles (not just the first), so the new
        # egg is recognisable as the union of its parts.
        title = "组合：" + " + ".join(s.title for s in srcs)
        # Build a per-egg section: name + original task prompt + produced files
        # + conversation, so the model knows the provenance of each part and
        # what each egg was asked to do (the old prompt only carried the bare
        # assistant answers, which was incomplete context).
        sections = []
        for s in srcs:
            files = s.list_files()
            body = s.history_text()
            file_line = "\n".join(f"  - {f}" for f in files) if files else "  （无）"
            sections.append(
                f"### 子蛋：{s.title}\n"
                f"原始任务：{s.prompt}\n"
                f"产出文件：\n{file_line}\n"
                f"对话内容：\n{body if body else '（该蛋暂无对话产出）'}"
            )
        combined = "\n\n".join(sections)
        prompt = (
            "以下是由多个子蛋产出的内容，请综合整理为一份统一的规格/实现说明，"
            "保留各子蛋的关键结论并消除重复：\n\n"
            + combined
        )
        # Position the combined egg BELOW every existing egg (plus a margin)
        # so it never overlaps a generated/parent egg.
        max_y = max((e.y for e in self.eggs.values()), default=160)
        new = await self.add_egg(title, prompt, x=200, y=max_y + 280)
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

    def _descendants_of(self, eid):
        """Ids of ALL descendant eggs (transitive: edges leaving eid, then theirs)."""
        children = {}
        for v in self.edges.values():
            children.setdefault(v["from"], []).append(v["to"])
        out, stack, seen = [], list(children.get(eid, [])), {eid}
        while stack:
            cur = stack.pop()
            if cur in seen or cur not in self.eggs:
                continue
            seen.add(cur)
            out.append(cur)
            stack.extend(children.get(cur, []))
        return out

    def _peer_dirs(self, egg):
        """Worktree directories this egg may READ: upstream + parallel nodes.

        Replaces the old <worktree>/shared/ copy: instead of copying upstream
        deliverables in, the prompt points the egg at the LIVE directories of
        its upstream (already-complete ancestors) and its parallel peers
        (unrelated branches running concurrently). Downstream descendants are
        NEVER included — their results do not exist yet. Parallel peers may
        legitimately have no evidence yet (they run at the same time).
        """
        upstream = {u for u in self.upstream_of(egg.id) if u in self.eggs}
        downstream = set(self._descendants_of(egg.id))
        peers, seen_paths = [], set()
        for oid, other in self.eggs.items():
            if oid == egg.id or oid in downstream:
                continue
            role = "upstream" if oid in upstream else "parallel"
            path = str(other.worktree)
            if path in seen_paths:
                continue
            seen_paths.add(path)
            peers.append((role, other.title, path))
        return peers

    def _peer_note(self, peers):
        if not peers:
            return ""
        lines = ["【上游与平行节点的工作目录（只读，可直接读取）】"]
        lines += [f"- [{'上游' if role == 'upstream' else '平行'}] {title}: {path}"
                  for role, title, path in peers]
        lines += [
            "说明：",
            "- 上游节点的交付物/证据已就绪，可直接读取。",
            "- 平行节点与本节点并发执行，其目录内的证据可能尚未生成或不完整，"
            "读取时以实际存在为准。",
            "- 上述目录仅供读取，禁止写入或修改；你的交付物必须写到你自己的"
            "工作区根目录。",
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
                orch.add_dependency(v["to"], v["from"], condition="success")
        return orch, task_of

    # ---- ordering via the demo Orchestrator (link.md 二.7) ----
    def _ordered_targets(self, targets):
        """Resolve unique target IDs; dependency expansion/order happens in _run_graph."""
        ids, seen = [], set()
        for target in targets or []:
            if target in self.eggs and target not in seen:
                seen.add(target)
                ids.append(target)
        return [self.eggs[eid] for eid in ids]

    def _topological_ids(self, ids):
        ids = list(dict.fromkeys(i for i in ids if i in self.eggs))
        node_set = set(ids)
        edges = [(edge.get("from"), edge.get("to")) for edge in self.edges.values()
                 if edge.get("from") in node_set and edge.get("to") in node_set]
        return _assert_acyclic(ids, edges)

    def _execution_ids(self, requested_ids):
        """Include every upstream prerequisite and return a stable DAG order."""
        requested = [i for i in dict.fromkeys(requested_ids) if i in self.eggs]
        if not requested:
            return []
        parents = {}
        dangling_parents = {}
        for edge in self.edges.values():
            source, target = edge.get("from"), edge.get("to")
            if target not in self.eggs:
                continue
            if source not in self.eggs:
                dangling_parents.setdefault(target, []).append(source)
            else:
                parents.setdefault(target, []).append(source)
        scope, stack = set(requested), list(requested)
        while stack:
            current = stack.pop()
            if dangling_parents.get(current):
                missing = ", ".join(str(v) for v in dangling_parents[current])
                raise ValueError(f"task {self.eggs[current].title!r} depends on missing egg(s): {missing}")
            for parent in parents.get(current, []):
                if parent not in scope:
                    scope.add(parent)
                    stack.append(parent)
        return self._topological_ids([i for i in self.eggs if i in scope])

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
        """Run targets and their prerequisites; gate downstream tasks on TaskCompleted."""
        requested_ids = {egg.id for egg in eggs if egg.id in self.eggs}
        try:
            ordered_ids = self._execution_ids(requested_ids)
        except ValueError as exc:
            yield {"k": "say", "egg": "commander",
                   "text": f"⛔ 依赖图存在循环，已停止执行：{exc}"}
            return
        if not ordered_ids:
            yield {"k": "say", "egg": "commander", "text": "⚠ 没有可执行的任务。"}
            return
        eggs = [self.eggs[eid] for eid in ordered_ids]

        def instruction_for(egg):
            # Only selected targets receive the user's custom instruction.
            return instruction_fn(egg) if egg.id in requested_ids else egg.prompt

        if not FRAMEWORK_OK:
            completed = set()
            parents = {}
            for edge in self.edges.values():
                if edge.get("from") in self.eggs and edge.get("to") in self.eggs:
                    parents.setdefault(edge["to"], []).append(edge["from"])
            for egg in eggs:
                unmet = []
                for parent_id in parents.get(egg.id, []):
                    parent = self.eggs[parent_id]
                    report = getattr(parent, "last_completion_report", None)
                    if parent_id not in completed or report is None or not getattr(report, "complete", False):
                        unmet.append(parent.title)
                if unmet:
                    yield {"k": "say", "egg": "commander",
                           "text": f"⛔ 跳过「{egg.title}」：前置任务未完成：{', '.join(unmet)}"}
                    continue
                async for ev in self._run_egg(egg, instruction_for(egg), ctx_fn(egg)):
                    yield ev
                report = getattr(egg, "last_completion_report", None)
                if report is not None and getattr(report, "complete", False):
                    completed.add(egg.id)
            return

        q: asyncio.Queue = asyncio.Queue()
        orch, task_of = self._build_orchestrator(ordered_ids, reset=True)

        def _event(name, task, **kw):
            if name in ("started", "retry", "failed", "finished", "timeout", "canceled"):
                try:
                    self._store.save(task)
                except Exception:  # pragma: no cover
                    pass
            q.put_nowait({"k": "framework", "egg": "commander",
                          "text": f"[{name}] {task.task_id}",
                          "event": name, "taskId": task.task_id, **kw})

        orch.set_on_event(_event)

        async def dispatch(task):
            egg = self.eggs.get(task.task_id)
            if egg is None:
                raise RuntimeError(f"task has no matching egg: {task.task_id}")

            # Do not trust scheduler completion alone: every direct dependency
            # must satisfy the same artifact/acceptance/report completion rule.
            for edge in self.edges.values():
                if edge.get("to") != task.task_id:
                    continue
                parent_task = task_of.get(edge.get("from"))
                if parent_task is None:
                    continue
                verdict = completion.evaluate_task_completed(parent_task)
                if not verdict.complete:
                    missing = verdict.missing()
                    reason = ("前置任务未满足完成条件：" + ", ".join(missing)
                              if missing else "前置任务未通过完成验收")
                    task.status = models.TaskStatus.BLOCKED
                    task.metadata["blocked_reason"] = f"{parent_task.title}: {reason}"
                    self._store.save(task)
                    await q.put({"k": "say", "egg": "commander",
                                 "text": f"⛔ 阻止执行「{egg.title}」：前置任务「{parent_task.title}」未完成验收。"})
                    raise RuntimeError(task.metadata["blocked_reason"])

            agent = models.AgentInfo(agent_id=egg.id, name=egg.title,
                                     permissions=[egg.id],
                                     status=models.AgentRunStatus.ONLINE)
            if not infrastructure.can_agent_modify_task(agent, task, "write"):
                task.metadata["blocked_reason"] = "permission denied"
                task.status = models.TaskStatus.BLOCKED
                self._store.save(task)
                await q.put({"k": "say", "egg": "commander",
                             "text": f"⛔ 无权执行「{egg.title}」"})
                raise RuntimeError("permission denied")

            assign = messaging.AgentMessage(
                msg_type=messaging.MessageType.TASK_ASSIGN,
                task_id=task.task_id, agent_id=egg.id, sender="commander",
                status="assigned",
                payload={"title": task.title, "priority": task.priority,
                         "required": task.required,
                         "attempt": int(task.metadata.get("attempt", 1))})
            self._bus.publish(assign)
            egg.current_assign_id = assign.msg_id
            peers = self._peer_dirs(egg)
            await egg.set_read_dirs([path for _role, _title, path in peers])
            ctx = ctx_fn(egg) or ""
            note = self._peer_note(peers)
            if note:
                ctx = f"{ctx}\n\n{note}".strip()
                await q.put({"k": "say", "egg": "commander",
                             "text": f"🔗 {egg.title} 已挂载上游/平行节点目录（只读）"})
            try:
                async for ev in self._run_egg(egg, instruction_for(egg), ctx):
                    await q.put(ev)
            finally:
                egg.current_assign_id = None

            src = getattr(egg, "last_task", None)
            if src is not None:
                task.status = src.status
                task.artifacts = list(src.artifacts)
                task.acceptance = src.acceptance
                task.report_submitted = src.report_submitted
                task.runs = list(src.runs)
                task.metadata.update(src.metadata)
            verdict = completion.evaluate_task_completed(task)
            if not verdict.complete:
                missing = verdict.missing()
                reason = "TaskCompleted 未满足" + (": " + ", ".join(missing) if missing else "")
                task.metadata["blocked_reason"] = reason
                if task.status == models.TaskStatus.SUCCESS:
                    task.status = models.TaskStatus.BLOCKED
                self._store.save(task)
                raise RuntimeError(reason)
            self._store.save(task)

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

        report = completion.global_report(orch.all())
        self._metrics.set_gauge("convergence", 1.0 if report.complete else 0.0)
        self._metrics.inc("graph_runs")
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

    # ---- Commander decision (总指挥 decides what to do with a message) ----
    async def _commander_decide(self, text):
        """Choose an action using current persisted task status and deliverables."""
        from google.antigravity import Agent, types
        egg_lines = []
        for egg in self.eggs.values():
            state, artifacts, report_submitted, blocked_reason, complete = "not run", [], False, "", False
            if FRAMEWORK_OK:
                try:
                    task = self._store.load(egg.id)
                    if task is not None:
                        state = getattr(task.status, "value", str(task.status))
                        artifacts = [a.path for a in (task.artifacts or [])]
                        report_submitted = bool(task.report_submitted)
                        blocked_reason = str(task.metadata.get("blocked_reason") or "")
                        complete = completion.evaluate_task_completed(task).complete
                except Exception as exc:  # noqa: BLE001
                    state = f"state unavailable: {exc}"
            egg_lines.append(
                f"- id={egg.id}; title={egg.title}; status={state}; TaskCompleted={complete}; "
                f"report_submitted={report_submitted}; artifacts={artifacts[:6]}; "
                f"blocked_reason={blocked_reason[:180]}; prompt={egg.prompt[:160]!r}")
        egg_ctx = "\n".join(egg_lines) if egg_lines else "(画布上还没有任何蛋)"
        config = build_agent_config(
            f"""You are the 总指挥 (commander) of an IdeaCanvas multi-agent project.

Your ONLY job is to analyze the operator's message and decide on ONE action by outputting a single JSON object. You do NOT execute anything yourself. You do NOT use any tools. The system will execute your decision after you respond.

Current tasks on the canvas:
{egg_ctx}

Your available actions (pick exactly ONE, output it in the "action" field):
- answer: The operator's message is a question, clarification, or request for information about the plan/status. Answer it directly in the "reply" field. Do NOT run any tasks.
- replan: The operator's message reveals missing work, a gap, or asks to simplify/replace the plan. DESCRIBE the new tasks in the "plan.tasks" field (each with title, prompt, depends_on). Do NOT try to create tasks yourself — just describe them in JSON. The system will create them based on your description. If the operator asks to simplify or replace the plan, also list the old egg titles or IDs to remove in the "remove" field.
- forward: The operator's message is relevant to specific existing eggs. Specify the target egg titles in "targets" and a tailored instruction for each in "instructions".
- run: The operator explicitly asks to execute/run/start the plan. Run the existing eggs with the message as instruction.

CRITICAL rules:
- You are a DECISION-MAKER ONLY. You do NOT use tools, create tasks, or execute anything. Your response is just a JSON object describing what should happen.
- For "replan", simply DESCRIBE new tasks in JSON. Do NOT attempt to call any tool or create anything yourself.
- In depends_on, ALWAYS reference existing eggs by their exact TITLE, never by their id. Only reference titles of tasks you are creating in this same plan, or titles already on the canvas.
- Each new task may depend on AT MOST ONE prerequisite, and the combined graph must keep a UNIQUE path to every node: never create a diamond where a task is reachable two ways (e.g. A->C and A->B->C). Chain prerequisites in order instead.
- Questions like "需要产物吗" or "进度如何" are answer actions, not run actions.
- Never infer completion from a status string alone; use TaskCompleted and artifacts/report fields.
- Include brief reasoning and a concise reply in the JSON.
- Output EXACTLY ONE JSON object matching the schema. No Markdown, no code fences, no explanations outside the JSON.""",
            response_schema=COMMANDER_DECISION_SCHEMA, tools=(),
            policies=[],
            capabilities=types.CapabilitiesConfig(
                enabled_tools=types.BuiltinTools.none(),
                enable_sandbox=False,
                enable_subagents=False,
                agent_behavior=types.AgentBehavior.MINIMAL))
        async with Agent(config) as agent:
            response = await agent.chat(text)
            # structured_output() drains the stream internally — the same
            # hang risk as resolve(); bound it and abort both sides on
            # timeout (see _plan_with_agent).
            structured_task = asyncio.ensure_future(response.structured_output())
            try:
                data = await asyncio.wait_for(
                    asyncio.shield(structured_task),
                    timeout=_env_float("CANVAS_COMMANDER_RESOLVE_TIMEOUT", 250.0))
            except asyncio.TimeoutError:
                try:
                    await response.cancel()
                except Exception:
                    pass
                _abort_server_request()
                structured_task.cancel()
                raise
            if not isinstance(data, dict):
                data = _extract_json(await response.text())
        if not isinstance(data, dict):
            raise ValueError("commander decision must be a JSON object")
        action = data.get("action")
        if action not in {"answer", "replan", "forward", "run"}:
            raise ValueError(f"unsupported commander action: {action!r}")
        data["reasoning"] = str(data.get("reasoning") or "").strip()
        data["reply"] = str(data.get("reply") or "").strip()
        if action == "replan":
            plan_data = data.get("plan") or {}
            if not isinstance(plan_data, dict) or not isinstance(plan_data.get("tasks", []), list):
                raise ValueError("replan action needs plan.tasks list")
            data["plan"] = plan_data
        if action == "forward":
            if not isinstance(data.get("targets", []), list):
                raise ValueError("forward targets must be a list")
            if not isinstance(data.get("instructions", {}), dict):
                raise ValueError("forward instructions must be an object")
            data["targets"] = [str(value).strip() for value in data.get("targets", [])
                               if str(value).strip()]
            data["instructions"] = {
                str(key).strip(): str(value) for key, value in data.get("instructions", {}).items()
                if str(key).strip()
            }
        # 全局轨迹：总指挥的模型回复与决策（action/reasoning）。
        if self._trace is not None:
            self._trace.append("commander", "总指挥", "assistant", "text",
                               text=str(data.get("reply") or ""))
            self._trace.append("commander", "总指挥", "system", "decision",
                               text=(f"action={data.get('action')} "
                                     f"reasoning={str(data.get('reasoning') or '')[:2000]}"))
            self._trace.append("commander", "总指挥", "system", "turn_end",
                               text=f"stop_reason={response.stop_reason} usage={_usage_text(response)}")
        return data

    async def _commander_execute_replan(self, decision, text):
        """Validate a proposed DAG completely before creating any new eggs."""
        plan_data = decision.get("plan") or {}
        raw_tasks = plan_data.get("tasks") or []
        if not raw_tasks:
            yield {"k": "say", "egg": "commander", "text": "⚠ 重新规划未产生新任务。"}
            return
        # Eggs the operator asked to remove are no longer "existing" for
        # collision/dependency validation — a new task can reuse their title.
        remove_set = set()
        for target in (decision.get("remove") or []):
            for egg in self.eggs.values():
                if egg.title == target or egg.id == target:
                    remove_set.add(egg.id)

        existing_title_lists = {}
        for egg in self.eggs.values():
            if egg.id in remove_set:
                continue
            existing_title_lists.setdefault(egg.title, []).append(egg)
        duplicates = {title for title, values in existing_title_lists.items() if len(values) > 1}
        # The commander is shown each egg's id AND title, so it may reference an
        # existing egg by id in depends_on. Resolve those ids to titles here —
        # the validator only understands titles — skipping eggs the operator
        # asked to remove so a dep on a soon-to-be-deleted egg still fails.
        id_to_title = {egg.id: egg.title for egg in self.eggs.values()
                       if egg.id not in remove_set}
        dropped = []   # filled when multi-parent new tasks are pruned below
        try:
            def _resolve_dep_ids(task):
                if not isinstance(task, dict):
                    return task
                deps = task.get("depends_on")
                if not isinstance(deps, list):
                    return task
                resolved, changed = [], False
                for dep in deps:
                    key = str(dep or "").strip()
                    mapped = id_to_title.get(key)
                    if mapped is not None:
                        resolved.append(mapped)
                        changed = True
                    else:
                        resolved.append(key)
                if changed:
                    task = dict(task)
                    task["depends_on"] = resolved
                return task

            raw_tasks = [_resolve_dep_ids(task) for task in raw_tasks]
            normalized = _normalize_plan_items(
                raw_tasks, available_titles=set(existing_title_lists), max_tasks=5)
            collisions = [title for title, _prompt, _deps in normalized
                          if title in existing_title_lists]
            if collisions:
                raise ValueError("新任务标题与现有蛋重复：" + ", ".join(collisions))
            all_deps = {dep for _title, _prompt, deps in normalized for dep in deps}
            ambiguous = sorted(all_deps & duplicates)
            if ambiguous:
                raise ValueError("依赖标题对应多个现有蛋，无法唯一解析：" + ", ".join(ambiguous))

            existing_by_title = {title: values[0] for title, values in existing_title_lists.items()}
            placeholder = {title: f"new:{title}" for title, _prompt, _deps in normalized}
            nodes = list(self.eggs) + list(placeholder.values())
            edges = [(edge.get("from"), edge.get("to")) for edge in self.edges.values()]
            for title, _prompt, deps in normalized:
                for dep in deps:
                    source_id = placeholder[dep] if dep in placeholder else existing_by_title[dep].id
                    edges.append((source_id, placeholder[title]))
            # Enforce a unique path to every new node (in-degree <= 1): a node
            # reachable two ways (A->C and A->B->C) keeps only its deepest
            # parent. Run before the acyclic check so the validated graph is
            # exactly the one we build below.
            edges, dropped = _enforce_unique_paths(
                nodes, edges, list(placeholder.values()))
            _assert_acyclic(nodes, edges)
        except Exception as exc:  # noqa: BLE001
            yield {"k": "say", "egg": "commander",
                   "text": f"⛔ 重新规划校验失败，未修改画布：{exc}"}
            return

        # --- Step 1: remove old eggs if the operator asked to simplify/replace ---
        removed = []
        for target in (decision.get("remove") or []):
            matches = [egg for egg in self.eggs.values()
                       if egg.title == target or egg.id == target]
            if len(matches) == 1:
                await self.delete_egg(matches[0].id)
                removed.append(matches[0].title)
            elif len(matches) > 1:
                yield {"k": "say", "egg": "commander",
                       "text": f"⚠ 无法移除「{target}」：存在多个同名蛋。"}

        # --- Step 2: create new eggs ---
        created_by_title = {}
        for index, (title, prompt, _deps) in enumerate(normalized):
            row, col = divmod(len(self.eggs) + index, 3)
            created_by_title[title] = await self.add_egg(
                title, prompt, x=120 + col * 380, y=160 + row * 240)

        # --- Step 3: wire up dependencies (only the pruned, unique-path edges) ---
        created_id_by_placeholder = {
            placeholder[title]: created_by_title[title].id
            for title in created_by_title}
        for source_id, target_id in edges:
            if target_id in created_id_by_placeholder:
                source_egg_id = created_id_by_placeholder.get(source_id, source_id)
                await self.add_edge(
                    source_egg_id, created_id_by_placeholder[target_id])

        self.save()

        # --- Step 4: notify frontend to re-sync the egg list ---
        yield {"k": "sync_graph"}

        msg = f"✅ 已创建 {len(created_by_title)} 个新蛋"
        if removed:
            msg += f"，移除 {len(removed)} 个旧蛋（{', '.join(removed)}）"
        msg += "；依赖已校验，开始执行新任务。"
        if dropped:
            placeholder_to_title = {v: k for k, v in placeholder.items()}

            def _label(nid):
                return placeholder_to_title.get(nid) or id_to_title.get(nid) or nid

            pruned = "；".join(f"{_label(p)}→{_label(t)}" for p, t in dropped)
            msg += f"\n🔗 已收敛为唯一到达路径（每节点仅一条上游链），移除重复上游边：{pruned}"
        yield {"k": "say", "egg": "commander", "text": msg}

        async for ev in self._run_graph(
                list(created_by_title.values()), lambda egg: text,
                lambda egg: self.neighbor_context(egg.id)):
            yield ev

    async def commander_run(self, text, targets):
        """Route commander chat safely; never execute every egg on decision failure."""
        decision, failure = None, None
        # 全局轨迹：操作员发给总指挥的消息。
        if self._trace is not None and text:
            self._trace.append("commander", "总指挥", "user", "text", text=text)
        if SDK_OK:
            try:
                timeout = _env_float("CANVAS_COMMANDER_DECIDE_TIMEOUT", 260.0)
                decision = await asyncio.wait_for(self._commander_decide(text), timeout=timeout)
            except Exception as exc:  # noqa: BLE001
                failure = exc
        else:
            failure = RuntimeError("SDK 不可用")
        if decision is None:
            reason = str(failure) if failure else "无法得到有效决策"
            yield {"k": "say", "egg": "commander",
                   "text": f"⚠ 总指挥决策失败：{reason}。为避免误执行，未自动派发给所有蛋。请明确指示“运行计划”或使用画布的选中任务运行功能。"}
            yield {"k": "end"}
            return

        action = decision["action"]
        yield {"k": "say", "egg": "commander",
               "text": f"🧠 总指挥分析：{decision.get('reasoning', '')}"}
        reply = decision.get("reply", "")
        if reply:
            yield {"k": "say", "egg": "commander", "text": reply}
        if action == "answer":
            yield {"k": "say", "egg": "commander", "text": "🏁 总指挥已回答，未运行任务。"}
            yield {"k": "end"}
            return
        if action == "replan":
            async for ev in self._commander_execute_replan(decision, text):
                yield ev
            yield {"k": "say", "egg": "commander", "text": "🏁 总指挥重新规划流程结束。"}
            yield {"k": "end"}
            return
        if action == "forward":
            target_titles = decision.get("targets") or []
            instructions = decision.get("instructions") or {}
            target_eggs = []
            for target in target_titles:
                matches = [egg for egg in self.eggs.values()
                           if egg.title == target or egg.id == target]
                if len(matches) == 1 and matches[0] not in target_eggs:
                    target_eggs.append(matches[0])
            if not target_eggs:
                yield {"k": "say", "egg": "commander",
                       "text": "⚠ 未能匹配明确的目标蛋，未派发给所有蛋。请指定蛋标题或 ID。"}
                yield {"k": "end"}
                return
            yield {"k": "say", "egg": "commander",
                   "text": f"→ 向 {len(target_eggs)} 个相关蛋传递信息；前置任务将先行执行并验收。"}
            async for ev in self._run_graph(
                    target_eggs, lambda egg: instructions.get(egg.title, text),
                    lambda egg: self.neighbor_context(egg.id)):
                yield ev
            yield {"k": "say", "egg": "commander", "text": "🏁 信息传递流程结束。"}
            yield {"k": "end"}
            return

        eggs = self._ordered_targets(targets or list(self.eggs.keys()))
        if not eggs:
            yield {"k": "say", "egg": "commander", "text": "⚠ 画布上没有可运行的蛋。"}
            yield {"k": "end"}
            return
        yield {"k": "say", "egg": "commander", "text": f"🚀 执行：{text}"}
        async for ev in self._run_graph(
                eggs, lambda egg: text, lambda egg: self.neighbor_context(egg.id)):
            yield ev
        yield {"k": "say", "egg": "commander", "text": "🏁 运行流程结束。"}
        yield {"k": "end"}

    # ---- one-click run of selected eggs ----
    async def run_selected(self, targets):
        eggs = self._ordered_targets(targets)
        if not eggs:
            yield {"k": "say", "egg": "commander", "text": "⚠ 未选择任何蛋。"}
            yield {"k": "end"}
            return
        yield {"k": "say", "egg": "commander",
               "text": f"🚀 运行所选任务及其前置任务（所选 {len(eggs)} 个，并发上限 {self.max_concurrency}）"}
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

        def _cap(v, n=20000):
            # Tool results carry whole file bodies; keep canvas.json loadable.
            if isinstance(v, str) and len(v) > n:
                return v[:n] + f"…[truncated {len(v) - n} chars]"
            return v

        data = {
            "session_id": self.session_id,
            "eggs": [{**e.spec(), "transcript": [
                {k: _cap(v) for k, v in entry.items()} for entry in e.transcript
            ]} for e in self.eggs.values()],
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
        # 加载该会话的全局模型调用轨迹（JSONL），供前端「全局」标签展示。
        self._trace.set_path(d / "global_trace.jsonl")
        self._trace.load()
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
                trace=self._trace,
            )
            self.eggs[spec["id"]] = egg
            restored = spec.get("transcript") or []
            if restored:
                egg.transcript = restored
                # Transcript already came from disk; skip the SDK history replay
                # in _hydrate_transcript_from_sdk or every step shows up twice.
                egg._hydrated = True
        for e in data.get("edges", []):
            self.edges[e["id"]] = e
        self.commander.transcript = data.get("commander_transcript", [])
        if self._context is not None:
            self._context.load_dict(data.get("context", {}))
        return self.view()
