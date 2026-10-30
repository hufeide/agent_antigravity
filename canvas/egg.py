"""One idea egg = one Antigravity SDK Agent scoped to its own worktree.

The egg holds a long-lived Agent session (history accumulates across turns)
and writes code into ``worktree`` via the SDK's built-in file/command tools.
When a model is connected (cloud key or local OpenAI/LiteRT backend) the egg
streams the model's real responses; if no model is reachable it reports that
explicitly instead of faking a completion.
"""
import asyncio
import datetime
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import time
import uuid
import logging
from pathlib import Path

from .config import (WORKTREE_ROOT, BACKEND, quiet_sdk_start,
                     backend_options, is_corrupt_conversation_error,
                     is_stream_stall_error, is_upstream_stream_error)
from .reasoning import (set_reasoning_capture, reasoning_capture_state,
                        thought_text)
from .. import acceptance, agent_runtime, completion, infrastructure, messaging, models

try:  # pragma: no cover - depends on installed SDK
    from google.antigravity import Agent, CapabilitiesConfig, types
    from google.antigravity.types import RunCommandConfig, HookResult
    from google.antigravity.hooks import hooks as sdk_hooks
    SDK_OK = True
except Exception:  # pragma: no cover
    SDK_OK = False

logger = logging.getLogger(__name__)

# Built-in tools that produce/change files. Used to decide whether an egg with
# no artifacts is legitimately chat-only or actually failed to deliver.
_WRITE_TOOLS = frozenset({"create_file", "edit_file", "write_file", "delete_file"})

# After a FAILED harness start, wait this long before another attempt is
# allowed. Prevents hammering a genuinely-down backend on every command while
# ensuring one transient failure never bricks the egg for the process lifetime.
_START_RETRY_COOLDOWN = 30.0

# A task that names a file (REPORT.md, impl.py, ...), asks to write one, or
# states a "Deliverable:" REQUIRES an artifact: a text-only reply that claims
# "done" must not pass.
_FILE_EXT = r"(?:md|markdown|txt|py|json|ya?ml|csv|tsv|html|css|js|ts|sh|toml|ini|sql|xml)"

# Document-like nouns that, following a producing verb, mean "create a file".
_DOC_WORDS = (
    "file|document|report|script|markdown|deliverable|workbook|"
    "spreadsheet|paper|essay|proposal"
)

# Sent once when a file task produced no file at all.
_NUDGE_PROMPT = (
    "你刚才没有真正创建任何文件。请立刻调用 create_file，把交付物写入工作区"
    "根目录（不要写到 shared/ 或 agent_data/），文件名按任务要求；写完后用 "
    "view_file 确认，再用一句话说明你写了什么。"
)
_ARTIFACT_HINT = re.compile(
    rf"[\w./-]+\.{_FILE_EXT}\b"
    r"|写入|写出|写进|创建文件|生成文件|保存为|保存到|写到"
    r"|deliverable"
    rf"|\b(?:write|create|produce|generate|save|draft|author|compile|build|"
    rf"prepare|formulate|compose)\b[^.\n]{{0,60}}\b(?:{_DOC_WORDS})\b",
    re.IGNORECASE,
)


def expects_artifacts(*texts):
    """True when any text explicitly asks for a file deliverable."""
    return any(bool(t) and bool(_ARTIFACT_HINT.search(str(t))) for t in texts)


# file:///a/b%20c -> /a/b c
_FILE_URL = re.compile(r"file://([^\s\)\]\}>\"']+)")


class _LLMBox:
    """Holds retry policy for model chat() calls.

    Concurrency is governed PER-EGG by ``EggAgent._lock`` (so a single egg never
    runs two model turns at once), which lets DIFFERENT eggs truly run in
    parallel. We deliberately do NOT serialize chat() across all eggs — modern
    OpenAI-compatible backends handle concurrent requests. Failed attempts are
    retried every ``delay`` seconds (see the retry loop in ``EggAgent.run``).
    """

    def __init__(self, max_attempts=6, delay=1.0):
        self.max_attempts = max_attempts
        self.delay = delay


_LLM_BOX = _LLMBox()


def _env_int(name, default=None):
    """Read an optional integer environment variable."""
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return None


async def _aclose_quiet(cm):
    """Close an SDK agent context manager in the background, swallowing errors."""
    try:
        await cm.__aexit__(None, None, None)
    except Exception:
        pass


# Per-turn safety timeout. If a model turn (e.g. a hung tool call / a stream that
# never ends) doesn't finish, abandon it so the box lock is released and the egg
# returns to 空闲 instead of being stuck 运行中 forever. Generous by default;
# override with EGG_TURN_TIMEOUT (seconds).
_TURN_TIMEOUT = float(os.environ.get("EGG_TURN_TIMEOUT", "300"))

# A turn that goes SILENT (no chunk at all for this long) is stalled — the
# model/server stopped streaming. Without this watchdog the egg would keep the
# "运行中" badge for the whole _TURN_TIMEOUT and the panel/trace would look
# frozen. Abort the turn instead so the egg returns to 空闲 promptly.
_STALL_TIMEOUT = float(os.environ.get("EGG_STALL_TIMEOUT", "120"))

# The blind silence watchdog above cannot distinguish "the server is doing a
# LONG prefill (llama.cpp/LM Studio stream nothing while processing the prompt,
# and a 45k-token prompt on a 4B model can take minutes)" from "the stream is
# dead". While waiting for the next chunk we therefore also poll the server's
# /slots endpoint (llama.cpp / LM Studio compat) and decide by server state:
#   - slow_prefill : processing AND progress advancing -> extend the wait
#   - dead_stream  : server idle but no chunk arrived   -> abort immediately
#   - zombie       : processing but progress frozen    -> abort immediately
#   - server_down  : unreachable during the silence    -> abort immediately
#   - timeout      : no /slots data at all             -> old blind behaviour
_STALL_PROBE_INTERVAL = float(os.environ.get("EGG_STALL_PROBE_INTERVAL", "15"))
_ZOMBIE_TIMEOUT = float(os.environ.get("EGG_ZOMBIE_TIMEOUT", "90"))
_STALL_EXTENSIONS = int(os.environ.get("EGG_STALL_EXTENSIONS", "2"))


class _StallDiagnosis(RuntimeError):
    """The silence watchdog fired, carrying a server-aware reason.

    ``reason`` is one of timeout / dead_stream / slow_prefill / zombie /
    server_down. ``retryable=True`` means "the server is alive and working —
    extend the wait" rather than "abort and retry the turn".
    """

    def __init__(self, reason, message, retryable=False):
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.retryable = retryable

# The SDK exposes the model's thinking as standard stream data
# (ChatResponse.thoughts / types.Thought). EggAgent._chat_once records those
# chunks directly, so no separate probe call is needed.

__all__ = ["set_reasoning_capture", "reasoning_capture_state"]


def _chunk_to_text(chunk):
    """Convert an SDK response chunk into display text."""
    if isinstance(chunk, str):
        return chunk
    if isinstance(chunk, types.Text):
        return chunk.text
    if isinstance(chunk, types.Thought):
        return f"💭 {chunk.text}\n"
    if isinstance(chunk, types.ToolCall):
        return f"\n🔧 调用工具：{chunk.name}\n"
    if isinstance(chunk, types.ToolResult):
        return "   ↳ 工具结果已处理\n"
    return ""


class EggAgent:
    def __init__(self, id, title, prompt, type_, position, subagents=None, parent=None,
                 session_dir=None, conversation_id=None, bus=None, store=None,
                 state=None, metrics=None, audit=None, context=None, trace=None):
        self.id = id
        self.title = title
        self.prompt = prompt
        self.type = type_
        self.x, self.y = position
        self.subagents = subagents or []
        self.parent = parent
        # Every egg belongs to a Canvas session; its code worktree and the SDK's
        # conversation persistence (save_dir) live under that session directory so
        # a whole idea project is self-contained under sessions/<timestamp>/.
        self.session_dir = Path(session_dir) if session_dir else WORKTREE_ROOT
        # Per-egg conversation id. None means "not yet established": on first
        # connect we DON'T pass it so the SDK creates a fresh conversation and
        # assigns an id (read back via self._agent.conversation_id). After that we
        # persist + pass this id so the SDK RESUMES the same session across turns
        # and restarts. Passing an id absent from save_dir makes the SDK raise
        # "conversation ... not found ... cannot resume".
        self.conversation_id = conversation_id
        # Top-level eggs (idea eggs) live directly under the session worktrees
        # dir; sub-eggs nest inside their parent's directory as sub-directories.
        self.worktree: Path = (self.session_dir / "worktrees" / parent / id
                               if parent else self.session_dir / "worktrees" / id)
        self.worktree.mkdir(parents=True, exist_ok=True)
        # Extra directories this egg may READ but never write: the worktrees of
        # its upstream + parallel nodes (see Canvas._peer_dirs). Injected into
        # the SDK `workspaces` config (so file tools can reach them) and into
        # the prompt as directory references, replacing the old shared/ copy.
        self.read_dirs: list[str] = []
        self._started_read_dirs: list[str] | None = None
        # touch a placeholder file so "review code" has something to show
        scope = f" (sub-egg of `{parent}`)" if parent else ""
        (self.worktree / "README.md").write_text(
            f"# {title}\n\n> worktree for egg `{id}`{scope}\n\n## Prompt\n{prompt}\n",
            encoding="utf-8",
        )
        self._lock = asyncio.Lock()
        self._cm = None
        self._agent = None
        # Start state machine: stopped -> starting -> ready / failed. A FAILED
        # start becomes retryable after START_RETRY_COOLDOWN seconds instead of
        # being cached for the whole process lifetime; the lock serializes
        # concurrent startups so parallel commands never spawn two harnesses.
        self._start_state = "stopped"
        self._start_failed_at = 0.0
        self._start_lock = asyncio.Lock()
        self._start_error = None
        self._recorded_init = False
        self._si = ""
        # Per-turn tracking so the trace can show REAL tool results when the
        # backend provides them, and add a single honest note only when it cannot
        # (the local openai backend executes tools server-side and returns no
        # ToolResult chunk / does not fire the post_tool_call hook).
        self._turn_tool_calls = 0
        self._got_tool_result = False
        # Run state for the UI + interrupt: busy while a model turn streams, and
        # _task points at the task iterating run() so it can be cancelled.
        self.busy = False
        self._task = None
        # The active SDK ChatResponse while a model turn streams, so interrupt()
        # can send halt_request to the backend (not merely close the local reader).
        self._active_resp = None
        # Whether the CURRENT turn produced a usable assistant payload (text or
        # a structured tool call). A turn with NEITHER is persisted by the SDK as
        # an empty assistant message, which makes the model server reject every
        # later request in this conversation with HTTP 400
        # ("Assistant message must contain either 'content' or 'tool_calls'!").
        # We detect that and drop the conversation before it can poison a turn.
        self._turn_payload = False
        # Per-run flag: the fully-assembled model input (with injected neighbour
        # context) is recorded into the trace only once per run, not per retry.
        self._prompt_recorded = False
        # Chronological conversation trace (user commands, model text/thoughts,
        # system prompt, tool calls/results) — viewable via /api/trace/{egg_id}.
        self.transcript = []
        # ---- demo framework bookkeeping (link.md 三.3 / 五) ----
        # NB: use explicit None checks -- TaskStore defines __len__, so an empty
        # store is falsy and 'store or TaskStore()' would silently replace the
        # shared store with a private one.
        self._bus = bus if bus is not None else messaging.MessageBus()
        self._store = store if store is not None else infrastructure.TaskStore()
        self._state = state if state is not None else infrastructure.StateManager()
        self._metrics = metrics if metrics is not None else infrastructure.Metrics()
        self._audit = audit if audit is not None else infrastructure.AuditLog()
        self._context = context
        # Global model-call trace sink (demo/canvas/trace.py). When set, every
        # transcript record is mirrored into the canvas-wide chronological
        # stream so the frontend 全局 tab can show all LLM traffic.
        self._trace = trace
        # msg_id of the TASK_ASSIGN that started the current run (correlation).
        self.current_assign_id = None
        self.last_task = None
        self.last_completion_report = None
        self._run_failed = False
        self._run_error = None
        self._run_canceled = False
        # ---- run bookkeeping (read by the commander's monitor loop) ----
        # When the CURRENT run started (monotonic) / None when idle.
        self.run_started_at = None
        # When the transcript last grew — used to detect a SILENT (stuck) run.
        self.last_activity = time.monotonic()
        # What the last finished run produced (reply length, duration, …).
        self.last_completion = None
        # How many runs this egg has completed (monotonic counter).
        self.run_seq = 0
        self._run_mark = 0      # transcript index where the current run started
        self._run_started = None
        # SDK-native facts for the last turn (set from ChatResponse / Conversation)
        self._last_stop_reason = None
        # Whether the SDK's persisted step history has been replayed into the trace
        self._hydrated = False
        # ---- per-turn / per-run guard attributes ----
        # Initialised here (not lazily in _chat_once/run) so the SDK tool hooks,
        # which fire during agent.chat(), can never hit an AttributeError if a
        # hook is invoked before the first _chat_once sets them.
        # Cross-surface tool-call dedupe bookkeeping (hook vs chunk stream).
        self._tc_seen = {"hook": {}, "chunk": {}}
        self._last_tc_sig = None
        self._tc_repeat = 0
        self._loop_guard = False
        # Set True once this egg has EVER called a file-writing tool in the
        # current run. Used to tell a genuine chat-only egg from one that tried
        # to produce files and failed. Reset once per RUN (in run()), not per
        # retry attempt, so a retry cannot dodge the artifact requirement by
        # quietly skipping the file tools it failed with on a previous attempt.
        self._used_file_tools = False
        # Stall watchdog counter (no model chunk for _STALL_TIMEOUT).
        self._stalls = 0
        # Cached answer to "does the model server expose /slots?" (llama.cpp /
        # LM Studio do; when not, the server-aware stall probe is disabled and
        # the watchdog falls back to the plain silence timeout).
        self._slots_ok = None
        # The instruction text for the current run (used by _needs_write_nudge
        # and the artifact-requirement heuristic).
        self._last_instruction = ""

    # ---- serialization for the frontend ----
    def spec(self):
        return {
            "id": self.id, "title": self.title, "prompt": self.prompt,
            "type": self.type, "x": self.x, "y": self.y, "parent": self.parent,
            "conversation_id": self.conversation_id,
            "running": self.busy,
        }

    # ---- SDK lifecycle ----
    def _make_hooks(self):
        """Build the real SDK hooks that record tool activity into this trace.

        Agent.__aenter__ registers every hook in ``config.hooks`` on its own
        HookRunner (google/antigravity/agent.py:62-74). ``hook_runner=`` is NOT
        a config field and is silently ignored, so hooks must travel through
        ``hooks=`` for them to fire at all.
        """
        if not SDK_OK:
            return []

        @sdk_hooks.pre_tool_call_decide
        def _tc(ctx: sdk_hooks.HookContext, data: types.ToolCall):
            self._record_tool_call("hook", getattr(data, "name", None),
                                   getattr(data, "args", None))
            return HookResult(allow=True)

        @sdk_hooks.post_tool_call
        def _tr(ctx: sdk_hooks.HookContext, data: types.ToolResult):
            self._got_tool_result = True
            self._record("tool", "tool_result", name=getattr(data, "name", None),
                         result=getattr(data, "result", None),
                         error=getattr(data, "error", None))

        hooks_out = [_tc, _tr]
        try:
            # AuditLog installs a real SDK post_tool_call hook so every tool
            # invocation is persisted with its run_id (link.md 二.8).
            hooks_out.extend(self._audit.install_sdk_hooks())
        except Exception:
            logger.debug("audit hooks unavailable", exc_info=True)
        return hooks_out

    def _config(self, resume=True):
        si = (
"""
You are an autonomous LLM module in the idea egg '{self.title}'.

Task: {self.prompt}
Workspace: {self.worktree}

Rules:
1. **Code tasks:** If programming is genuinely required, implement actual source files in the workspace and run them to verify. Report only what you actually did and observed.
2. **Non-code tasks:** Complete writing, analysis, research, planning, and other general tasks directly. Do not invent code unnecessarily.
3. **Deliverables:** Your final reply must clearly state the result. For code, list files and run instructions; otherwise, present the result directly. Save any written deliverable as a Markdown file in the workspace root, never in `agent_data/`.
4. **Read upstream summaries first:** Before starting, read the completion summaries of your upstream eggs (they are provided in your task context). Base your approach on what they have already delivered; NEVER redo work they have already completed, and NEVER perform work that belongs to another egg. Only do YOUR task.
5. **Write a completion summary:** Before finishing, write your completion summary to `COMPLETION_SUMMARY.md` in your workspace root (never in `agent_data/`). It must cover: (1) what you did; (2) key deliverables and file paths; (3) verification results; (4) decisions you made; (5) caveats or limitations. Your final reply must also state the result and summarize this summary, so the commander and downstream eggs can read it.
6. Coordinate with connected eggs only when instructed.
7. Be concise and truthful.
"""
        )
        # Only keep the minimal SWE toolset (write/run code) — drop the default
        # web/image/subagent tools that bloat the context and are unused here.
        # `finish` is always added by the harness.
        caps = types.CapabilitiesConfig(
            enabled_tools=types.BuiltinTools.minimal(),
            enable_subagents=False,
            agent_behavior=types.AgentBehavior.MINIMAL,
        )
        if BACKEND == "cloud":
            # SDK-native command limits: sandbox + a hard per-command timeout
            # (types.RunCommandConfig.timeout_seconds, types.py:204).
            caps.run_command_config = RunCommandConfig(
                enable_sandbox=True,
                timeout_seconds=float(os.environ.get("EGG_COMMAND_TIMEOUT", "600")),
            )
        self._si = si
        # Session establishment is delegated to the SDK: CREATE_OR_RESUME keeps
        # the egg's history across turns and silently starts a FRESH session when
        # the persisted conversation_id is gone (types.SessionContinuationMode),
        # so the egg no longer has to string-match "cannot resume" errors.
        # CREATE_ONLY is used for the explicit fresh-start / heal path.
        try:
            cont_mode = infrastructure.make_session_continuation(
                "create_or_resume" if resume else "create")
        except Exception:
            cont_mode = None
        return agent_runtime.build_agent_config(
            si,
            **backend_options(),
            workspaces=[str(self.worktree)] + list(self.read_dirs),
            app_data_dir=str(self.worktree / "agent_data"),
            save_dir=str(self.session_dir / "agent_data"),
            # When resuming we pass the stable per-egg id so the SDK restores the
            # full LLM session; when it's gone we omit it (fresh conversation).
            conversation_id=(self.conversation_id if resume else None),
            session_continuation_mode=cont_mode,
            capabilities=caps,
            # link.md 一.6: file tools are scoped to THIS worktree and obviously
            # destructive shell commands are denied; everything else stays
            # autonomous. (Specific denies beat the wildcard allow.)
            policies=infrastructure.build_agent_policies(
                self.worktree, read_dirs=self.read_dirs),
            # Resource caps (link.md 一.6) are OPT-IN: SDK budgets are scoped
            # to the whole agent session, so a small default would silently
            # kill a long-lived egg on its third command. Runaway turns are
            # handled per run by the repeated-tool-call guard in run().
            budget_config=infrastructure.build_budget_config(
                max_total_tokens=_env_int("EGG_MAX_TOTAL_TOKENS"),
                max_tool_calls=_env_int("EGG_MAX_TOOL_CALLS"),
                max_model_calls=_env_int("EGG_MAX_MODEL_CALLS"),
            ),
            # Re-enable the "composed egg inherits its sources as cloud
            # sub-agents" feature: combine() builds SubagentConfig objects and
            # hands them to _spawn -> self.subagents. build_agent_config drops
            # this key for backends that don't accept it, so passing it here is
            # safe everywhere.
            subagents=self.subagents,
            hooks=self._make_hooks(),
            enable_otel=False,
        )

    def _sync_conv_id(self):
        """Adopt the runtime-assigned conversation id (available only after the
        first message) so a recovered/fresh conversation resumes correctly on the
        next app start instead of retrying a stale id forever."""
        try:
            cid = self._agent.conversation_id if self._agent else None
        except Exception:
            cid = None
        if cid and cid != self.conversation_id:
            self.conversation_id = cid

    # ---- SDK-native introspection -------------------------------------
    def _hydrate_transcript_from_sdk(self):
        """Replay the SDK's persisted step history into the UI transcript.

        After a restart the in-memory transcript is empty while the SDK has
        resumed the conversation with its full step history
        (Conversation.history: list[types.Step]). Replaying it keeps 「轨迹」
        and inter-egg context working across restarts.
        """
        if self._hydrated:
            return
        self._hydrated = True
        try:
            steps = list(self._agent.conversation.history)
        except Exception:
            return
        # Conversation.history appends every UPDATE of a step, so one model
        # message shows up as ACTIVE, ACTIVE, ..., DONE entries with the same
        # step_index. Replaying them verbatim repeats every thought/text/tool
        # call 3-4x, so collapse to the LAST update per step_index (the final,
        # complete one) and dedupe tool calls inside it.
        latest = {}
        for step in steps:
            latest[step.step_index] = step
        replay = []
        for step in (latest[i] for i in sorted(latest)):
            if step.thinking:
                replay.append(self._entry("assistant", "thought",
                                          text=step.thinking))
            seen_calls = set()
            for call in step.tool_calls or []:
                key = (getattr(call, "id", None)
                       or (getattr(call, "name", None),
                           json.dumps(getattr(call, "args", None),
                                      sort_keys=True, default=str)))
                if key in seen_calls:
                    continue
                seen_calls.add(key)
                replay.append(self._entry(
                    "assistant", "tool_call",
                    name=getattr(call, "name", None),
                    args=getattr(call, "args", None)))
            if step.content:
                if step.source == types.StepSource.USER:
                    replay.append(self._entry("user", "text", text=step.content))
                elif step.type in (types.StepType.TEXT_RESPONSE,
                                   types.StepType.FINISH):
                    replay.append(self._entry("assistant", "text",
                                              text=step.content))
        if replay:
            self.transcript[0:0] = replay
            # Keep the current run's mark pointing at the same live entries.
            self._run_mark += len(replay)

    def _needs_write_nudge(self, files_before, instruction):
        """True when a file-producing task ended without creating any file.

        Small local models often *claim* they wrote a file without calling a
        tool. Rather than failing the task immediately, ask the same
        conversation once to actually write it (run() does that).
        """
        if self._run_failed or self._run_canceled:
            return False
        if not expects_artifacts(self.prompt, instruction):
            return False
        return not (set(self.list_files()) - set(files_before))

    def _record_sandbox_status(self):
        """Surface the SDK's OS-sandbox status in the trace (Agent.sandbox_status)."""
        try:
            status = self._agent.sandbox_status
        except Exception:
            return
        if status is None:
            return
        text = ("available" if status.available
                else f"unavailable: {status.unavailable_reason or 'unknown'}")
        self._record("system", "sandbox", text=text)

    def _usage_snapshot(self):
        """Return SDK token usage for the last turn + whole session.

        Uses Conversation.last_turn_usage / total_usage (types.UsageMetadata),
        which is the SDK's canonical accounting surface (Step.usage_metadata is
        deprecated in its favour, types.py:1161).
        """
        try:
            conv = self._agent.conversation if self._agent else None
        except Exception:
            return {}
        if conv is None:
            return {}

        def dump(usage):
            if usage is None:
                return None
            return {
                "prompt": usage.prompt_token_count,
                "cached": usage.cached_content_token_count,
                "candidates": usage.candidates_token_count,
                "thoughts": usage.thoughts_token_count,
                "total": usage.total_token_count,
            }

        try:
            last_turn = dump(conv.last_turn_usage)
            session = dump(conv.total_usage)
            reported = any((v or 0) for d in (last_turn, session)
                           for v in (d or {}).values())
            return {
                "reported": reported,
                "last_turn": last_turn,
                "session": session,
                "turns": conv.turn_count,
            }
        except Exception:
            return {}

    def _stop_reason_value(self):
        """Return the SDK stop reason, or None when the backend never reported one.

        Local OpenAI-compatible harnesses (e.g. a GGUF server) do not send a
        finish reason, so the SDK leaves StopReason.UNSPECIFIED. Reporting
        "UNSPECIFIED" in the trace looks like a failure, so treat it as absent.
        """
        reason = self._last_stop_reason
        value = getattr(reason, "value", reason)
        if value is None:
            return None
        if str(value).upper() in ("UNSPECIFIED", "STOPREASON.UNSPECIFIED"):
            return None
        return value

    async def _open_agent(self, resume=True):
        """Build + enter an Agent, cleaning up if entering fails."""
        with quiet_sdk_start():
            cm = Agent(self._config(resume=resume))
            try:
                agent = await cm.__aenter__()
                return cm, agent
            except Exception:
                try:
                    await cm.__aexit__(None, None, None)
                except Exception:
                    pass
                raise

    async def start(self):
        if not SDK_OK:
            return
        try:
            self._cm, self._agent = await self._open_agent(resume=True)
            self._started_read_dirs = list(self.read_dirs)
            return
        except Exception as ex:
            # A stale conversation id — the session's agent_data was cleared, or
            # the egg is resumed under a fresh session dir — makes the SDK refuse
            # to resume ("... not found ... (cannot resume)"). Recover by starting
            # a FRESH conversation instead of leaving the egg permanently offline.
            if is_corrupt_conversation_error(ex):
                try:
                    self._cm, self._agent = await self._open_agent(resume=False)
                    self._started_read_dirs = list(self.read_dirs)
                    try:
                        self.conversation_id = (self._agent.conversation_id
                                                or self.conversation_id)
                    except Exception:
                        self.conversation_id = uuid.uuid4().hex
                    logger.info("[egg %s] stale conversation -> started fresh "
                                "(id=%s)", self.id, self.conversation_id)
                    return
                except Exception as ex2:
                    ex = ex2
            logger.warning("[egg %s] agent start failed: %s", self.id, ex)
            self._cm = None
            self._agent = None
            self._start_error = ex

    async def stop(self):
        if self._cm:
            try:
                await self._cm.__aexit__(None, None, None)
            except Exception:
                pass
        self._cm = None
        self._agent = None
        self._start_state = "stopped"
        self._start_failed_at = 0.0

    async def set_read_dirs(self, dirs):
        """Point this egg at extra READ-ONLY directories (upstream + parallel nodes).

        The `workspaces` list is baked into the Agent config at creation, so if
        the agent is already running with a different read set we restart it
        (resume preserves the conversation) to apply the new scope. No-op when
        the set is unchanged.
        """
        new = [str(d) for d in (dirs or []) if d]
        if new == self.read_dirs:
            return
        self.read_dirs = new
        if self._agent is not None and self._started_read_dirs != new:
            await self.stop()

    # ---- interactions ----
    async def _ensure_started(self):
        """Lazily connect to the model on first use.

        State machine: stopped -> starting -> ready / failed. A FAILED start
        is retried after ``_START_RETRY_COOLDOWN`` seconds — one transient
        failure (harness spawn hiccup, model slot busy with an aborted
        prefill) must never brick the egg for the process lifetime.
        Concurrent callers share one startup via ``_start_lock`` so parallel
        commands cannot each spawn their own harness.
        """
        if self._agent is not None:
            return True
        async with self._start_lock:
            if self._agent is not None:
                return True
            if (self._start_state == "failed"
                    and time.monotonic() - self._start_failed_at
                    < _START_RETRY_COOLDOWN):
                return False
            self._start_state = "starting"
            self._start_error = None
            await self.start()
            # Recover from a missing persisted conversation: if resume failed
            # because the conversation_id isn't in save_dir, drop it and
            # reconnect so the SDK starts a FRESH conversation instead of
            # hard-failing every turn.
            if (self._agent is None and self._start_error is not None
                    and self.conversation_id is not None
                    and is_corrupt_conversation_error(self._start_error)):
                self.conversation_id = None
                self._start_error = None
                await self.start()
            if self._agent is None:
                self._start_state = "failed"
                self._start_failed_at = time.monotonic()
                return False
            # If the SDK created a brand-new conversation (we did NOT pass a
            # conversation_id to resume), capture the id it assigned so later
            # turns / process restarts resume THIS session instead of failing
            # with "conversation ... not found ... cannot resume".
            if self.conversation_id is None:
                cid = getattr(self._agent, "conversation_id", None)
                if cid:
                    self.conversation_id = cid
            self._start_state = "ready"
            if not self._recorded_init:
                self._recorded_init = True
                # Replay persisted SDK history first, then capture the FULL system
                # prompt and tool set so the trace shows prompts + tools + history.
                self._hydrate_transcript_from_sdk()
                self._record("system", "system_prompt", text=self._si)
                tools = []
                for t in types.BuiltinTools.minimal():
                    tools.append(getattr(t, "name", str(t)))
                tool_line = "enabled: " + (", ".join(tools) or "（无）") + " (+ finish)"
                if BACKEND == "cloud":
                    tool_line += " [cloud: run_command sandbox]"
                self._record("system", "tools", text=tool_line)
                self._record_sandbox_status()
            return True

    def _offline_msg(self, action):
        msg = f"⚠ 该蛋的 LLM 未连接，无法{action}。"
        if self._start_error:
            err = str(self._start_error)
            msg += f"\n原因：{err}"
            # A recoverable session-state problem (persisted conversation_id missing
            # in save_dir) should NOT be misreported as a backend/model outage.
            if is_corrupt_conversation_error(self._start_error):
                msg += "\n（会话状态文件丢失；下次对话将自动以新会话开始。）"
                return msg
        msg += "\n请确认 IDEA_BACKEND 与本地/云端模型可用（openai 后端需 :9000 服务在跑）。"
        return msg

    # ---- conversation trace ----
    @property
    def transcript_len(self):
        """Total character length of the conversation trace.

        The commander's monitor compares this across ticks to answer
        "会话有没有变长" (is the egg still making progress?).
        """
        return sum(len(r.get("text") or "") for r in self.transcript)

    @staticmethod
    def _entry(role, kind, text=None, name=None, args=None,
               result=None, error=None):
        """Build one transcript entry (used by live recording + hydration)."""
        return {
            "ts": datetime.datetime.now().isoformat(timespec="milliseconds"),
            "role": role,
            "kind": kind,
            "text": text,
            "name": name,
            "args": args,
            "result": result,
            "error": error,
        }

    def _record(self, role, kind, text=None, name=None, args=None,
                result=None, error=None):
        self.last_activity = time.monotonic()
        self.transcript.append(self._entry(role, kind, text=text, name=name,
                                           args=args, result=result, error=error))
        if self._trace is not None:
            # Mirror into the canvas-wide global trace: every model-call
            # dialogue (prompts, thoughts, text, tools, status) in one
            # time-ordered stream.
            self._trace.append(self.id, self.title, role, kind, text=text,
                               name=name, args=args, result=result, error=error)

    def _record_text(self, text):
        """Append to the last assistant text entry to avoid per-token spam."""
        if (self.transcript and self.transcript[-1]["role"] == "assistant"
                and self.transcript[-1]["kind"] == "text"):
            self.transcript[-1]["text"] += text
            if self._trace is not None:
                self._trace.merge_last(self.id, "text", text)
        else:
            self._record("assistant", "text", text=text)

    def _record_thought(self, text):
        """Append to the last thought entry so the model's thinking STREAMS as
        one growing block instead of one entry per chunk."""
        if (self.transcript and self.transcript[-1]["role"] == "assistant"
                and self.transcript[-1]["kind"] == "thought"):
            self.transcript[-1]["text"] += text
            if self._trace is not None:
                self._trace.merge_last(self.id, "thought", text)
        else:
            self._record("assistant", "thought", text=text)

    async def _consume_reasoning(self, nonblocking=False):
        """Record the model's reasoning captured by the maas proxy.

        The openai backend streams through the local harness (a Go binary)
        which silently drops the model's ``reasoning_content`` — the SDK
        never surfaces a Thought chunk (verified: delta.reasoning / delta.
        thinking / extra_content.* all produce steps with empty thinking).
        The maas proxy mirrors the reasoning to
        ``sessions/.reasoning/<egg_id>.log`` — flushed SEGMENT-wise right
        before the first body-text chunk so the egg can record thoughts IN
        STREAM ORDER (before the body text they belong to).

        With nonblocking=True (called inside the chunk loop) read whatever
        the proxy has already flushed without waiting; otherwise poll briefly
        for the trailing segment the proxy lands at stream end.
        """
        path = (Path(__file__).resolve().parent.parent / "sessions"
                / ".reasoning" / f"{self.id}.log")
        if nonblocking:
            try:
                text = path.read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                return
            if text:
                try:
                    # Consume: truncate so the next read only sees new text.
                    path.write_text("", encoding="utf-8")
                except Exception:
                    pass
                self._record_thought(text)
            return
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            try:
                text = path.read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                await asyncio.sleep(0.05)
                continue
            if text:
                try:
                    path.write_text("", encoding="utf-8")
                except Exception:
                    pass
                self._record_thought(text)
            return

    def _record_tool_call(self, source, name, args):
        """Record a tool call once, whichever SDK surface reported it first.

        The hook and the chunk stream describe the same invocation but share
        neither id namespace (harness call id vs step id) nor arg spelling
        (canonical snake_case vs the model's raw keys). Identity is therefore
        the tool NAME: an event pairs with, and is suppressed by, the oldest
        unmatched event of the same name from the other surface. Parallel or
        repeated calls still each get an entry because every event queues once.
        """
        if (name or "").lower() in _WRITE_TOOLS:
            self._used_file_tools = True
        try:
            sig = (name, json.dumps(args, sort_keys=True, default=str))
        except Exception:
            sig = (name, repr(args))
        if sig == self._last_tc_sig:
            self._tc_repeat += 1
        else:
            self._last_tc_sig, self._tc_repeat = sig, 1
        if self._tc_repeat >= 4:
            self._loop_guard = True
        other = self._tc_seen["chunk" if source == "hook" else "hook"]
        pending = other.get(name) or []
        if pending:
            pending.pop(0)   # already recorded from the other surface
            return
        self._tc_seen[source].setdefault(name, []).append(True)
        self._turn_tool_calls += 1
        self._record("assistant", "tool_call", name=name, args=args)

    def _record_chunk(self, chunk):
        thought = thought_text(chunk)
        if thought is not None:
            self._record_thought(thought)
            return
        if isinstance(chunk, str):
            self._turn_payload = True
            self._record_text(chunk)
        elif isinstance(chunk, types.Text):
            self._turn_payload = True
            self._record_text(chunk.text)
        elif isinstance(chunk, types.ToolCall):
            # A structured tool call IS a valid assistant payload (the API
            # accepts an assistant message that carries tool_calls).
            self._turn_payload = True
            self._record_tool_call("chunk", chunk.name, chunk.args)
        elif isinstance(chunk, types.ToolResult):
            self._got_tool_result = True
            self._record("tool", "tool_result", name=chunk.name,
                         result=chunk.result, error=chunk.error)

    async def halt(self, timeout: float = 3.0) -> None:
        """Ask the backend to stop the in-flight turn (SDK halt_request).

        ChatResponse.cancel() sends halt_request to the localharness
        (local_connection.py:637), which is the only thing that actually stops
        the model. Closing the local reader (aclose) just detaches us — the
        harness keeps generating, which is exactly what a "stopped" egg that
        still calls the LLM looks like.
        """
        resp = self._active_resp
        if resp is None:
            return
        try:
            await asyncio.wait_for(resp.cancel(), timeout)
        except (asyncio.CancelledError, Exception):  # noqa: BLE001 - best effort
            pass

    def interrupt(self):
        """Cancel this egg's in-flight model turn (used by the stop button).

        Order matters: the halt_request is sent FIRST and only then is the
        local streaming task cancelled. Cancelling first unwinds the generator,
        which clears _active_resp/closes the stream while the harness keeps the
        model running.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:  # e.g. shutdown from another thread
            t = self._task
            if t is not None and not t.done():
                t.cancel()
            return
        loop.create_task(self._stop_turn())

    async def _stop_turn(self) -> None:
        await self.halt()
        t = self._task
        if t is not None and not t.done():
            t.cancel()

    async def _chat_once(self, text, context=None):
        """One chat attempt: stream the FULL semantic chunk stream.

        Iterate ``resp.chunks`` (NOT ``async for chunk in resp``) — the latter's
        ``__aiter__`` filters to ``Text`` only and would DROP ``Thought``,
        ``ToolCall`` and ``ToolResult``. Using the unfiltered stream lets the
        trace capture 💭 thoughts and 🔧 tool calls/results.
        """
        if context:
            # Prepend the connected eggs' conversation history so this egg can
            # reference what its neighbours have actually done/decided. The SDK's
            # compaction keeps the transcript from ballooning across turns.
            text = (
                "【参考上下文：以下是在本次任务中相关的其他蛋的会话历史/观点，"
                "仅供了解背景】\n"
                f"{context}\n"
                "【以上为参考上下文。以下是用户给你的指令】\n"
                f"{text}"
            )
            if not self._prompt_recorded:
                self._prompt_recorded = True
                # 记录实际发送给模型的完整输入（含注入的邻居上下文）——轨迹必须
                # 反映模型真实看到的交互内容，而不仅是用户敲下的那句指令。
                self._record("system", "prompt", text=text)
                if self._trace is not None:
                    self._trace.append(self.id, self.title, "user", "prompt",
                                       text=text)
        self._turn_tool_calls = 0
        self._got_tool_result = False
        self._tc_seen = {"hook": {}, "chunk": {}}
        # NB: _used_file_tools is deliberately NOT reset per chat attempt here —
        # it is reset once per RUN in run(). A retry must not dodge the artifact
        # requirement by quietly skipping the file tools it failed with on a
        # previous attempt.
        self._last_instruction = ""
        # Per-run guard against a model that repeats the same tool call forever
        # (observed with small local models: 10+ identical create_file retries).
        self._last_tc_sig = None
        self._tc_repeat = 0
        self._loop_guard = False
        resp = await self._agent.chat(text)
        self._active_resp = resp
        try:
            async for chunk in resp.chunks:
                # The proxy lands each reasoning segment right before the
                # body text it belongs to — consume it BEFORE processing the
                # chunk so thought entries appear in stream order (before the
                # body text), not after the whole turn.
                await self._consume_reasoning(nonblocking=True)
                self._record_chunk(chunk)
                t = _chunk_to_text(chunk)
                if t:
                    yield t
            # The local openai backend executes tools server-side and returns no
            # ToolResult chunk (nor fires post_tool_call), so the client never
            # sees the result. Say so ONCE per turn — but only when there really
            # was a tool call AND no real result was recorded (cloud backends DO
            # surface results via the hook, so they are never penalised here).
            if self._turn_tool_calls > 0 and not self._got_tool_result:
                self._record("tool", "tool_result",
                             result="（本后端不回传工具结果：工具在服务端执行，"
                                    "结果直接进入模型上下文；产物见本蛋工作目录）")
            # The harness drops the model's reasoning; the maas proxy mirrors
            # it to a side-channel log — consume it into the transcript now.
            await self._consume_reasoning()
        except asyncio.CancelledError:
            # Cancelled from the OUTSIDE (orchestrator cancel_all, shutdown,
            # graph interruption): still tell the backend to halt, otherwise the
            # harness finishes the turn in the background after we've stopped.
            await self.halt()
            raise
        finally:
            self._active_resp = None
            try:
                # ChatResponse.stop_reason (types.py:1582) reports why the turn
                # ended (normal / MAX_MODEL_CALLS_EXCEEDED / ...).
                self._last_stop_reason = resp.stop_reason
            except Exception:
                self._last_stop_reason = None

    def _find_corrupt_turns(self):
        """Step indices of persisted assistant turns with neither content nor
        tool_calls — the exact shape the model server rejects with HTTP 400.

        Verifying the poison exists BEFORE rebuilding avoids blindly nuking a
        healthy conversation on an unrelated 400 (oversized request, malformed
        tool schema, ...).
        """
        bad = []
        if self._agent is None:
            return bad
        try:
            steps = list(self._agent.conversation.history)
        except Exception:
            return bad
        # history appends every UPDATE of a step; keep the final update per index.
        latest = {}
        for step in steps:
            latest[step.step_index] = step
        for i in sorted(latest):
            s = latest[i]
            try:
                if s.source == types.StepSource.USER:
                    continue
            except Exception:
                pass
            if not s.content and not s.tool_calls:
                bad.append(i)
        return bad

    async def _heal_conversation(self, err, verify=True):
        """Reset a corrupt/invalid SDK conversation so the next chat() starts fresh.

        The SDK persists the LLM session under ``save_dir`` keyed by
        ``conversation_id``. A turn that produced neither ``content`` nor
        ``tool_calls`` (e.g. a reasoning-only turn with empty content) gets
        recorded as an empty assistant message; on the NEXT ``chat()`` the API
        rejects the whole history with HTTP 400
        ("Assistant message must contain either 'content' or 'tool_calls'").
        Dropping ``conversation_id`` forces a brand-new conversation that does not
        replay the broken history, so the egg recovers instead of erroring forever
        (which otherwise leaves it stuck "运行中" with an empty box).

        With ``verify=True`` the persisted history is scanned for the poisoned
        turn first; if no corrupt turn is found the 400 has another cause and
        the conversation is left untouched (returns False). Pass
        ``verify=False`` when the caller has direct evidence of an incomplete
        turn (e.g. a turn that streamed nothing).
        """
        e = str(err)
        if not is_corrupt_conversation_error(e):
            return False
        if verify:
            bad = self._find_corrupt_turns()
            if not bad:
                logger.warning("[egg %s] 400 looked corrupt but no empty "
                               "assistant turn found; keeping conversation",
                               self.id)
                return False
            logger.info("[egg %s] confirmed corrupt turn(s) at steps %s",
                        self.id, bad)
        # Close the OLD session in the BACKGROUND: `Agent.__aexit__` tears down
        # the harness conversation and can BLOCK for a while. Doing it inline
        # would hold the egg's lock (and the WS event stream) hostage — the egg
        # would sit in "运行中" with no output. Detach it and let it finish on
        # its own task.
        old_cm = self._cm
        self._cm = None
        self._agent = None
        self.conversation_id = None
        self._start_state = "stopped"
        self._start_failed_at = 0.0
        if old_cm is not None:
            asyncio.ensure_future(_aclose_quiet(old_cm))
        try:
            self._cm, self._agent = await self._open_agent(resume=False)
            if self._agent is not None:
                cid = getattr(self._agent, "conversation_id", None)
                self.conversation_id = cid or uuid.uuid4().hex
                self._start_state = "ready"
            else:
                self.conversation_id = uuid.uuid4().hex
                self._start_state = "failed"
                self._start_failed_at = time.monotonic()
        except Exception as reopen_exc:
            self._cm = None
            self._agent = None
            self._start_state = "failed"
            self._start_failed_at = time.monotonic()
            self._start_error = reopen_exc
        return True

    def _server_root(self):
        """llama.cpp/LM Studio native endpoints (/slots, /abort) live at the
        SERVER ROOT, not under the OpenAI-compatible /v1 prefix that
        base_url carries — request them with the /v1 suffix stripped.
        """
        base = str(backend_options().get("base_url") or "").rstrip("/")
        if not base:
            return ""
        p = urllib.parse.urlparse(base)
        return f"{p.scheme}://{p.netloc}" if p.path else base

    async def _probe_model_server(self):
        """Classify model-server health after a stall/timeout.

        Distinguishes "we canceled waiting" from "the server is stuck":
        llama.cpp streams nothing during prefill and cannot detect a client
        disconnect mid-prefill, so an aborted request may keep burning its
        slot. Returns "ok" (reachable, idle slot), "busy" (reachable, all
        slots processing — likely the zombie request), "down" (unreachable),
        or "unknown" (no base_url configured).
        """
        base = str(backend_options().get("base_url") or "").rstrip("/")
        if not base:
            return "unknown"

        def _get(url):
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=3) as r:
                return json.loads(r.read().decode())

        try:
            try:
                slots = await asyncio.to_thread(_get, self._server_root() + "/slots")
            except Exception:
                # llama.cpp serves /slots; other servers may not — /models is
                # the OpenAI-standard liveness endpoint.
                await asyncio.to_thread(_get, base + "/models")
                return "ok"
            if any(s.get("is_processing") for s in slots):
                return "busy"
            return "ok"
        except Exception:
            return "down"

    def _sample_slots(self):
        """One /slots snapshot: (any_processing, total_progress_tokens).

        Raises urllib.error.HTTPError on non-2xx (so 404 = "this server has no
        /slots" can be distinguished from "server unreachable").
        """
        root = self._server_root()
        if not root:
            raise RuntimeError("no base_url")
        req = urllib.request.Request(root + "/slots")
        with urllib.request.urlopen(req, timeout=3) as r:
            slots = json.loads(r.read().decode())
        processing = any(s.get("is_processing") for s in slots)
        progress = sum(
            int(s.get("n_prompt_tokens_processed") or 0)
            + int(s.get("n_tokens") or 0)
            for s in slots)
        return processing, progress

    def _abort_server_request(self):
        """Best-effort: ask the server to abort its in-flight request.

        ``resp.cancel()`` only stops the SDK-side reader; llama.cpp cannot
        detect a client disconnect mid-prefill, so an aborted request keeps
        burning its slot (a "zombie"). POST /abort (llama.cpp native; LM Studio
        may answer 404) releases it server-side when the endpoint exists.
        """
        root = self._server_root()
        if not root:
            return
        for path in ("/abort", "/api/abort"):
            try:
                req = urllib.request.Request(
                    root + path, data=b"{}",
                    headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=3) as r:
                    if r.status < 400:
                        logger.info("[egg %s] aborted in-flight server request "
                                    "via %s", self.id, path)
                        return
            except Exception:
                continue

    async def _slots_probe_loop(self, state):
        """Sample /slots every _STALL_PROBE_INTERVAL while a chunk wait runs."""
        while not state.get("stop"):
            await asyncio.sleep(_STALL_PROBE_INTERVAL)
            if state.get("stop"):
                return
            try:
                processing, progress = await asyncio.to_thread(self._sample_slots)
            except urllib.error.HTTPError as he:
                if he.code == 404:
                    # Server reachable but has no /slots: stop probing; the
                    # watchdog falls back to the plain silence timeout.
                    state["unsupported"] = True
                    self._slots_ok = False
                    return
                # Reachable but not serving /slots right now (e.g. 503 while
                # the model is loading / a request is in flight): the server
                # is alive, we simply have no progress data for this sample.
                state["down_since"] = None
                continue
            except Exception:
                state["down_since"] = state.get("down_since") or time.monotonic()
                continue
            state["down_since"] = None
            state["saw_processing"] = state["saw_processing"] or processing
            if not processing:
                state["saw_idle"] = True
            if progress != state["progress"]:
                state["progress"] = progress
                state["saw_progress"] = True
                state["last_progress_at"] = time.monotonic()

    async def _next_chunk(self, agen):
        """Wait for the next stream chunk with a SERVER-AWARE stall watchdog.

        Replaces the blind ``wait_for(anext, _STALL_TIMEOUT)``: while the
        stream is silent we poll the model server's /slots so we can tell a
        legitimate long prefill apart from a dead/zombied stream and abort —
        or extend the wait — immediately instead of after a fixed timeout.
        """
        if self._slots_ok is None:
            try:
                await asyncio.to_thread(self._sample_slots)
                self._slots_ok = True
            except urllib.error.HTTPError as he:
                # 404 = no /slots endpoint on this server; any other HTTP
                # status (e.g. 503 while loading) means the endpoint exists.
                self._slots_ok = he.code != 404
            except Exception:
                # First sample unreachable: assume the server may still be
                # coming up; keep the plain silence timeout this round.
                self._slots_ok = False
        if not self._slots_ok:
            # No /slots on this server (or unreachable at start): fall back
            # to the plain silence timeout — but still report it as a stall
            # diagnosis so the caller's abort/retry logic stays uniform.
            try:
                return await asyncio.wait_for(agen.__anext__(),
                                              timeout=_STALL_TIMEOUT)
            except asyncio.TimeoutError:
                raise _StallDiagnosis(
                    "timeout",
                    f"模型流静默超过 {_STALL_TIMEOUT:g}s，已中止本次尝试。")

        state = {"progress": None, "last_progress_at": time.monotonic(),
                 "saw_processing": False, "saw_idle": False,
                 "saw_progress": False, "down_since": None, "stop": False}
        probe = asyncio.ensure_future(self._slots_probe_loop(state))
        try:
            return await asyncio.wait_for(agen.__anext__(), timeout=_STALL_TIMEOUT)
        except asyncio.TimeoutError:
            now = time.monotonic()
            if state["down_since"] is not None:
                raise _StallDiagnosis(
                    "server_down",
                    "模型服务不可达（静默期间多次探测失败），已中止。")
            if state["saw_idle"] and not state["saw_processing"]:
                raise _StallDiagnosis(
                    "dead_stream",
                    "服务端已空闲但本轮没有任何 chunk（连接已死），已中止。")
            if state["saw_processing"]:
                # No progress advance for _ZOMBIE_TIMEOUT while the server
                # claims to be processing = stuck/zombie prefill.
                frozen_for = now - state["last_progress_at"]
                if frozen_for < _ZOMBIE_TIMEOUT:
                    raise _StallDiagnosis(
                        "slow_prefill",
                        "服务端正在处理（prefill 推进中），静默属正常，延长等待。",
                        retryable=True)
                raise _StallDiagnosis(
                    "zombie",
                    f"服务端槽位被僵尸请求冻结（进度停滞 {frozen_for:.0f}s），"
                    f"已中止并尝试释放。")
            raise _StallDiagnosis(
                "timeout",
                f"模型流静默超过 {_STALL_TIMEOUT:g}s，已中止本次尝试。")
        finally:
            state["stop"] = True
            probe.cancel()

    async def _wait_for_slot_release(self, budget=30.0):
        """Poll until the server has an idle slot (or the budget expires).

        Bounded on purpose: if the server stays busy we report rather than
        pile more requests onto it. We never restart the server ourselves —
        that decision belongs to the operator. Returns "ok" / "down" /
        "busy" / "zombie" (a processing slot whose progress is frozen for the
        whole budget will never release itself).
        """
        deadline = time.monotonic() + budget
        last_progress = None
        frozen_since = None
        while time.monotonic() < deadline:
            state = await self._probe_model_server()
            if state != "busy":
                return state
            try:
                _, progress = await asyncio.to_thread(self._sample_slots)
                if progress != last_progress:
                    last_progress = progress
                    frozen_since = None
                elif frozen_since is None:
                    frozen_since = time.monotonic()
                elif (time.monotonic() - frozen_since
                      > min(_ZOMBIE_TIMEOUT, budget) * 0.75):
                    return "zombie"
            except Exception:
                pass
            await asyncio.sleep(2.0)
        return "busy"

    async def run(self, text, context=None):
        """Stream the model's real response for a command inside this egg.

        The chat runs under the shared box lock (one model request at a time)
        and is retried every ``_LLM_BOX.delay`` seconds on failure.
        """
        self._record("user", "text", text=text)
        self._publish(messaging.MessageType.PROGRESS, status="running",
                      payload={"instruction": (text or "")[:500]})
        self._prompt_recorded = False
        self._last_tc_sig, self._tc_repeat, self._loop_guard = None, 0, False
        self._stalls = 0
        self._last_instruction = text
        # Reset once per RUN (not per retry attempt): see the comment in
        # _chat_once. Accumulates across attempts so a retry that skips file
        # tools cannot erase evidence that an earlier attempt tried them.
        self._used_file_tools = False
        self._run_failed = False
        self._run_error = None
        self._run_canceled = False
        self._last_stop_reason = None
        if not SDK_OK:
            msg = "⚠ 该蛋的 LLM SDK 不可用（未安装 google.antigravity）。"
            self._record("system", "status", text=msg)
            self._run_failed = True
            self._run_error = msg
            self._run_started = time.monotonic()
            self._finish_run()
            yield msg
            return
        # Mark busy + capture the task BEFORE connecting/starting so the stop
        # button can interrupt even during agent startup (first run) or while
        # waiting for the box lock — not only during the active model stream.
        self.busy = True
        self._task = asyncio.current_task()
        # Remember where this run's transcript starts so the completion facts
        # (reply length) can be computed when it finishes.
        self._run_mark = len(self.transcript)
        self._run_started = time.monotonic()
        self.run_started_at = self._run_started
        try:
            if not await self._ensure_started():
                msg = self._offline_msg("执行命令")
                self._record("system", "status", text=msg)
                self._run_failed = True
                self._run_error = msg
                yield msg
                return
            async with self._lock:
                files_before = set(self.list_files())
                for attempt in range(1, _LLM_BOX.max_attempts + 1):
                    try:
                        self._turn_payload = False
                        chat = self._chat_once(text, context=context)
                        got_text = False
                        # Whole-turn safety: the stall watchdog below only catches a
                        # SILENT stream (no chunk for _STALL_TIMEOUT). A turn that keeps
                        # trickling chunks past _TURN_TIMEOUT must still be abandoned so
                        # the egg never sits 运行中 forever — this arms the advertised
                        # _TURN_TIMEOUT (which was previously dead code).
                        async with asyncio.timeout(_TURN_TIMEOUT):
                            try:
                                # Stream with a STALL watchdog: if no chunk arrives
                                # for _STALL_TIMEOUT the model/server may have gone
                                # quiet (often a truncated/aborted upstream turn).
                                # _next_chunk additionally polls the server's /slots
                                # during the silence, so a legitimate long prefill
                                # (llama.cpp streams nothing while processing) gets
                                # extra time while a dead/zombied stream aborts
                                # immediately.
                                agen = chat.__aiter__()
                                while True:
                                    try:
                                        t = await self._next_chunk(agen)
                                    except StopAsyncIteration:
                                        break
                                    except _StallDiagnosis as diag:
                                        self._stalls += 1
                                        if diag.retryable and self._stalls <= _STALL_EXTENSIONS:
                                            # The server is still working (prefill
                                            # progress observed): the silence is
                                            # legitimate, not a dead stream —
                                            # extend the wait instead of aborting.
                                            yield ("\n⏳ 服务端正在处理请求（prefill "
                                                   "推进中），静默看门狗延长等待…\n")
                                            continue
                                        yield f"\n⚠ {diag.message}\n"
                                        resp = self._active_resp
                                        if resp is not None:
                                            try:
                                                await resp.cancel()
                                            except Exception:
                                                pass
                                        self._abort_server_request()
                                        if self._stalls > 2:
                                            self._run_failed = True
                                            self._run_error = (
                                                f"model stream stalled: {diag.reason}")
                                            return
                                        raise RuntimeError(
                                            f"model stream stalled: {diag.reason}")
                                    stripped = t.lstrip()
                                    if not (t.startswith("💭")
                                            or stripped.startswith("🔧")
                                            or stripped.startswith("↳")):
                                        got_text = True
                                    yield t
                                    if self._loop_guard:
                                        # The model keeps issuing the exact
                                        # same tool call: stop the turn instead
                                        # of burning the whole time budget.
                                        self._run_failed = True
                                        self._run_error = (
                                            "repeated identical tool call")
                                        yield ("\n⚠ 检测到模型重复调用同一工具，"
                                               "已中止本轮。\n")
                                        resp = self._active_resp
                                        if resp is not None:
                                            try:
                                                await resp.cancel()
                                            except Exception:
                                                pass
                                        break
                            finally:
                                # stop the model call + free locks on interrupt
                                try:
                                    await chat.aclose()
                                except Exception:  # pragma: no cover
                                    pass
                        if not got_text and not self._turn_payload:
                            # The turn produced NOTHING (no text, no tool call) —
                            # the response was truncated or rejected upstream.
                            # Say so instead of silently "succeeding" with an
                            # empty answer.
                            yield ("\n⚠ 本轮没有收到任何模型输出（可能被截断或"
                                   "上游中断），可重试。\n")
                        # An EMPTY turn (no text, no structured tool call) is
                        # persisted by the SDK as an assistant message with
                        # neither content nor tool_calls — which makes the model
                        # server reject every later request in this conversation
                        # (HTTP 400 "Assistant message must contain either
                        # 'content' or 'tool_calls'!"). Drop the conversation NOW
                        # so the NEXT command starts from a clean one instead of
                        # failing forever.
                        if not self._turn_payload:
                            logger.warning(
                                "[egg %s] empty model turn -> resetting "
                                "conversation to avoid HTTP 400", self.id)
                            # The abort may have left a zombie prefill burning
                            # a server slot; wait for release before reopening
                            # the harness (its startup ping would stall too).
                            state = await self._probe_model_server()
                            if state == "busy":
                                yield ("\n⏳ 服务端仍在处理被中止的请求，"
                                       "等待槽位释放…\n")
                                state = await self._wait_for_slot_release()
                            if state == "down":
                                yield ("\n⚠ 模型服务不可达，会话保留；"
                                       "请恢复 :9000 服务后重试。\n")
                            else:
                                # Direct evidence of an incomplete turn: the
                                # stream produced nothing, so skip the scan.
                                await self._heal_conversation(
                                    "Assistant message must contain either "
                                    "'content' or 'tool_calls'!",
                                    verify=False)
                        # A file task answered with text only is a HALLUCINATED
                        # completion: nudge the SAME conversation once to
                        # actually write the deliverable before giving up.
                        if self._needs_write_nudge(files_before, text):
                            yield ("\n⚠ 本轮没有创建任何文件，正在要求模型补写"
                                   "交付物…\n")
                            if self._trace is not None:
                                # 补写也是一次独立的模型调用：记录其输入。
                                self._trace.append(self.id, self.title, "user",
                                                   "prompt", text=_NUDGE_PROMPT)
                            try:
                                async with asyncio.timeout(_TURN_TIMEOUT):
                                    async for tok in self._chat_once(_NUDGE_PROMPT):
                                        yield tok
                            except asyncio.TimeoutError:
                                self._run_error = "write nudge timeout"
                            except Exception as exc:  # noqa: BLE001
                                self._run_error = f"write nudge failed: {exc}"
                        self._sync_conv_id()   # persist the real conversation id
                        return
                    except asyncio.CancelledError:
                        self._run_canceled = True
                        raise
                    except asyncio.TimeoutError:
                        self._run_failed = True
                        self._run_error = "turn timeout"
                        yield f"\n⚠ 本次运行超时（{_TURN_TIMEOUT:g}s），已停止。\n"
                        # Distinguish "we canceled waiting" from "server is
                        # stuck": an aborted prefill keeps its slot busy.
                        state = await self._probe_model_server()
                        if state == "busy":
                            yield "⏳ 服务端仍在处理被中止的请求，等待槽位释放…\n"
                            state = await self._wait_for_slot_release()
                        if state == "down":
                            yield "⚠ 模型服务不可达，请检查 :9000 服务。\n"
                        return
                    except Exception as ex:
                        # A stream-stall abort can leave a zombie prefill on the
                        # server; probing before retrying avoids piling requests
                        # onto a busy server (which re-triggers the watchdog).
                        if is_stream_stall_error(ex):
                            state = await self._probe_model_server()
                            if state == "busy":
                                yield ("\n⏳ 服务端仍在处理被中止的请求，"
                                       "等待槽位释放…\n")
                                state = await self._wait_for_slot_release()
                            if state == "zombie":
                                # A processing slot whose progress never moves
                                # will never release itself; only an operator
                                # restart frees it. Report instead of retrying.
                                self._run_failed = True
                                self._run_error = "zombie request holds server slot"
                                yield ("\n⚠ 服务端槽位被僵尸请求冻结（进度长期"
                                       "停滞），无法自动恢复；请重启 :9000 "
                                       "模型服务后重试。\n")
                                return
                            if state == "down":
                                self._run_failed = True
                                self._run_error = str(ex)
                                yield ("\n⚠ 模型服务不可达，已停止；"
                                       "请检查 :9000 服务后重试。\n")
                                return
                        # A corrupt/invalid conversation (e.g. an empty assistant
                        # message -> HTTP 400) cannot be retried in place; heal it
                        # (fresh conversation) so the egg recovers instead of
                        # erroring forever and staying stuck "运行中". Healing
                        # first VERIFIES the poisoned turn actually exists.
                        healed = await self._heal_conversation(ex)
                        if healed and self._agent is None:
                            yield f"\n⚠ 无法重建会话，已停止：{ex}\n"
                            return
                        if attempt >= _LLM_BOX.max_attempts:
                            self._run_failed = True
                            self._run_error = str(ex)
                            yield f"\n⚠ 请求多次失败，已停止：{ex}\n"
                            return
                        yield (f"\n⚠ 请求失败（{ex}），{_LLM_BOX.delay:g}s 后重试 "
                               f"（{attempt}/{_LLM_BOX.max_attempts}）…\n")
                        await asyncio.sleep(_LLM_BOX.delay)
        finally:
            self._finish_run()

    def _finish_run(self):
        """Close out a run and publish the facts the commander's monitor reads.

        ``last_completion`` lets the commander tell "this egg finished" from
        "this egg is still working" without waiting on the run's own task.
        """
        self.busy = False
        self._task = None
        now = time.monotonic()
        started = self._run_started or now
        self.run_started_at = None
        reply_len = 0
        for r in self.transcript[self._run_mark:]:
            if r.get("kind") == "text" and r.get("role") == "assistant":
                reply_len += len(r.get("text") or "")
        self.last_completion = {
            "ts": time.time(),
            "seq": self.run_seq,
            "reply_len": reply_len,
            "transcript_len": self.transcript_len,
            "duration": round(now - started, 1),
            "stop_reason": self._stop_reason_value(),
            "usage": self._usage_snapshot(),
        }
        self.run_seq += 1
        if self._trace is not None:
            # 全局轨迹：本轮模型调用的结束记录（停止原因 / 用量 / 耗时）。
            self._trace.append(
                self.id, self.title, "system", "turn_end",
                text=(f"stop_reason={self._stop_reason_value()} "
                      f"reply_len={reply_len} "
                      f"duration={round(now - started, 1)}s "
                      f"usage={json.dumps(self._usage_snapshot(), ensure_ascii=False)}"))
        self._record_task_run(reply_len, now)

    # ---- framework messaging helper -------------------------------------
    def _publish(self, msg_type, *, status="", artifacts=None, error=None,
                 payload=None, require_ack=False, correlation_id=None):
        """Publish a structured AgentMessage (link.md 一.4).

        REPORT / DELIVER are correlated with the TASK_ASSIGN that started the
        run and request an acknowledgement, so the commander can prove the
        reply belongs to that assignment and that it was received.
        """
        try:
            self._bus.publish(messaging.AgentMessage(
                msg_type=msg_type, task_id=self.id, agent_id=self.id,
                sender=self.id, status=status,
                artifacts=list(artifacts or []), error=error,
                payload=dict(payload or {}),
                correlation_id=(correlation_id if correlation_id is not None
                                else self.current_assign_id),
                require_ack=require_ack))
        except Exception:
            logger.debug("message publish failed", exc_info=True)

    def context_summary(self, limit=800):
        """Latest assistant text, used as the versioned shared context."""
        parts = [
            r.get("text") for r in self.transcript
            if r.get("role") == "assistant" and r.get("kind") == "text"
        ]
        text = "\n".join(p for p in parts[-3:] if p)
        # Model replies contain markdown links like
        # [x](file:///.../shared/%E7%BC%96%E7%A0%81/x.py). Downstream eggs copy
        # those verbatim and then fail on the percent-encoded path, so hand
        # them plain, decoded paths instead.
        text = _FILE_URL.sub(lambda m: urllib.parse.unquote(m.group(1)), text)
        return text[-limit:]

    def _record_task_run(self, reply_len, now):
        """Mirror this run into the demo framework (link.md 三.3 / 五).

        The completion rule is ENFORCED here, not merely reported:
        report_submitted is set only when execution finished, artifacts are
        valid and acceptance passed. Permission is checked before anything is
        recorded (link.md 一.6), and the outcome is published on the message
        bus as REPORT plus DELIVER / BLOCKED / CANCEL (link.md 一.4).
        """
        status = models.ExecutionStatus.FINISHED
        task_status = models.TaskStatus.SUCCESS
        report_status = "done"
        error = self._run_error
        if self._run_canceled:
            status = models.ExecutionStatus.CANCELED
            task_status = models.TaskStatus.CANCELED
            report_status = "canceled"
        elif self._run_failed:
            status = models.ExecutionStatus.ERROR
            task_status = models.TaskStatus.FAILED
            report_status = "failed"

        agent = models.AgentInfo(
            agent_id=self.id, name=self.title, permissions=[self.id],
            status=models.AgentRunStatus.ONLINE)
        run = models.TaskRun(
            task_id=self.id, agent_id=self.id, status=status,
            start_time=self._run_started, end_time=now, error=error)
        # Load the previous run's task BEFORE building the fresh one so state
        # accumulated across runs survives a DIRECT egg command (the UI "command"
        # panel calls egg.run() without going through the orchestrator's
        # dispatch, which is the only path that otherwise re-merges runs back).
        previous = self._store.load(self.id) if self._store is not None else None
        task = models.Task(
            task_id=self.id, title=self.title, description=self.prompt,
            owner_agent_id=self.id, status=models.TaskStatus.PLANNED,
            metadata={"worktree": str(self.worktree)})
        if previous is not None:
            # Preserve the commander's confirmation across direct commands.
            task.confirmed = previous.confirmed
            # Keep every finished run from history; drop only the RUNNING
            # placeholder (the current run is appended below).
            task.runs.extend(
                r for r in previous.runs
                if r.status != models.ExecutionStatus.RUNNING)
        task.runs.append(run)

        # Permission gate (link.md 一.6): an egg may only write its own task.
        if not infrastructure.can_agent_modify_task(agent, task, "write"):
            task_status = models.TaskStatus.BLOCKED
            report_status = "blocked"
            error = error or "permission denied"
            task.metadata["blocked_reason"] = "agent lacks write permission"

        # Version high-water marks survive a task reset (they live in metadata
        # because _reset_task clears the artifact list for the next attempt).
        versions: dict[str, int] = {}
        if previous is not None:
            for path, ver in (previous.metadata.get("artifact_versions") or {}).items():
                versions[path] = max(versions.get(path, 0), int(ver))
            for art in previous.artifacts:
                versions[art.path] = max(versions.get(art.path, 0), art.version)
        for rel in self.list_files():
            if rel == "README.md" or rel.startswith("agent_data/"):
                continue
            # Not deliverables: files copied in from upstream eggs, and
            # byte-code caches produced by running the code.
            if rel.startswith("shared/") or "__pycache__/" in rel:
                continue
            if rel.endswith((".pyc", ".pyo")):
                continue
            path = str(self.worktree / rel)
            ver = versions.get(path, 0) + 1
            versions[path] = ver
            task.artifacts.append(models.Artifact(
                task_id=self.id, path=path, kind="file", version=ver))
        if versions:
            # Persist the high-water marks so a later run (or a reset) keeps
            # counting up instead of restarting at v1.
            task.metadata["artifact_versions"] = dict(versions)
        if task.artifacts:
            criteria = ["artifacts exist", "non-empty"]
        elif self._used_file_tools or expects_artifacts(
                self.prompt, self._last_instruction):
            # The task names a file to produce, or the egg called file tools,
            # yet nothing landed in its worktree (e.g. it edited a read-only
            # upstream copy, made a directory named like a file, or simply
            # claimed success without calling a tool). A reply alone must not
            # count as delivery.
            def artifact_required(_task):
                return False, "task requires a file deliverable but no artifact was produced"
            criteria = [artifact_required]
        else:
            # Genuinely chat-only egg: the reply itself is the deliverable.
            def reply_non_empty(_task):
                return reply_len > 0, f"reply_len={reply_len}"
            criteria = [reply_non_empty]
        task.acceptance = models.Acceptance(task_id=self.id, criteria=criteria)

        with infrastructure.bind_run(run.run_id):
            if self._state is not None:
                self._state.try_transition(task, models.TaskStatus.RUNNING)
            if task.status != models.TaskStatus.RUNNING:
                task.status = models.TaskStatus.RUNNING

            if task_status == models.TaskStatus.SUCCESS:
                # Validate artifacts FIRST so they reach GENERATED before
                # evaluate_acceptance -> mark_artifacts moves them to ACCEPTED.
                # Otherwise mark_artifacts sees NOT_GENERATED (its whitelist
                # excludes it) and skips them, leaving artifacts stuck at
                # GENERATED after evaluate_task_completed re-validates.
                acceptance.validate_artifacts(task.artifacts, task)
                passed = acceptance.evaluate_acceptance(task.acceptance, task)
                if not passed:
                    task_status = models.TaskStatus.FAILED
                    report_status = "acceptance_failed"
                    task.metadata["blocked_reason"] = "acceptance failed"
            else:
                passed = False
                task.acceptance.passed = False
                task.acceptance.evidence.append(
                    f"not evaluated: {task_status.value}")
                if error:
                    task.acceptance.evidence.append(f"run error: {error}")

            if self._state is not None:
                self._state.try_transition(task, task_status)
            if task.status != task_status:
                task.status = task_status

            task.report_submitted = bool(
                task_status == models.TaskStatus.SUCCESS and passed)
            # Marker: 已运行并上报总指挥 — set only when the egg ran and its
            # report passed the completion rule. Used by _run_graph to skip
            # already-run eggs on a re-run of the DAG. Cleared by _reset_task.
            if task.report_submitted:
                task.metadata["ran_and_reported"] = True
            if not task.report_submitted and "blocked_reason" not in task.metadata:
                task.metadata["blocked_reason"] = (
                    task.acceptance.evidence[-1]
                    if task.acceptance.evidence else "acceptance not passed")

            report = completion.evaluate_task_completed(task)
            self.last_task = task
            self.last_completion_report = report
            usage = self._usage_snapshot()
            try:
                self._store.save(task)
                self._metrics.inc("egg_runs")
                self._metrics.observe(
                    "egg_duration", max(0.0, now - (self._run_started or now)))
                self._metrics.inc("artifacts_total", len(task.artifacts))
                if self._run_failed:
                    self._metrics.inc("egg_failures")
                if not report.complete:
                    self._metrics.inc("egg_incomplete")
                tokens = ((usage.get("last_turn") or {}).get("total") or 0)
                if tokens:
                    self._metrics.inc("tokens_total", tokens)
                self._audit.record(
                    "egg_run_finished", task_id=self.id,
                    status=task.status.value, complete=report.complete,
                    acceptance=task.acceptance.passed, missing=report.missing())
                self._publish(
                    messaging.MessageType.REPORT, status=report_status,
                    artifacts=[a.path for a in task.artifacts], error=error,
                    require_ack=True,
                    payload={"complete": report.complete,
                             "missing": report.missing(),
                             "reason": report.reason,
                             "reply_len": reply_len,
                             "stop_reason": self._stop_reason_value(),
                             "usage": usage})
                if report.complete:
                    self._publish(messaging.MessageType.DELIVER,
                                  status="delivered",
                                  artifacts=[a.path for a in task.artifacts],
                                  payload={"complete": True}, require_ack=True)
                elif task.status == models.TaskStatus.CANCELED:
                    self._publish(messaging.MessageType.CANCEL,
                                  status="canceled", error=error)
                elif report.blocked or task.status == models.TaskStatus.BLOCKED:
                    self._publish(messaging.MessageType.BLOCKED,
                                  status="blocked", error=error,
                                  payload={"reason": report.reason})
                if self._context is not None:
                    self._context.put(
                        self.id, self.context_summary(), source=self.title)
            except Exception:
                logger.debug("framework bookkeeping failed", exc_info=True)

    async def compress(self):
        """Return a concise summary of the egg's conversation (real model)."""
        self._record("user", "text",
                     text="（压缩会话）请将到目前为止的对话压缩为规格说明。")
        if not SDK_OK:
            self._record("system", "status", text="⚠ 该蛋的 LLM SDK 不可用。")
            return "⚠ 该蛋的 LLM SDK 不可用。"
        if not await self._ensure_started():
            msg = self._offline_msg("压缩会话")
            self._record("system", "status", text=msg)
            return msg
        # Mark busy + capture the task BEFORE waiting for the lock — same as
        # run(): a compress queued behind an in-flight run must show 运行中 and
        # stay cancellable via the stop button, not look 空闲.
        self.busy = True
        self._task = asyncio.current_task()
        try:
            async with self._lock:
                summary = None
                prompt = ("将我们到目前为止的对话压缩为一段简洁的规格说明"
                          "（markdown），只保留关键决策与产出。")
                # 压缩实际发送的提示词与 user 记录措辞不同，如实记入轨迹。
                self._record("system", "prompt", text=prompt)
                for attempt in range(1, _LLM_BOX.max_attempts + 1):
                    try:
                        resp = await self._agent.chat(prompt)
                        self._active_resp = resp
                        try:
                            summary = await resp.text()
                        finally:
                            self._active_resp = None
                        if self._trace is not None:
                            self._trace.append(self.id, self.title, "user",
                                               "prompt", text=prompt)
                            self._trace.append(self.id, self.title, "assistant",
                                               "text", text=summary or "")
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as ex:
                        if await self._heal_conversation(ex) and self._agent is None:
                            summary = "⚠ 压缩失败：无法重建会话。"
                            break
                        if attempt >= _LLM_BOX.max_attempts:
                            summary = "⚠ 压缩失败：模型多次无响应。"
                            break
                        await asyncio.sleep(_LLM_BOX.delay)
                summary = summary or ""
                self._record("assistant", "text", text=summary)
                return summary
        finally:
            self.busy = False
            self._task = None

    # ---- conversation history (used when combining eggs) ----
    def history_text(self, mode="brief", limit=None):
        """Render this egg's conversation for PASSING ON to another agent.

        ONLY the assistant's own answers are transmitted. The 💭 thinking, the
        🔧 tool calls / ↳ results and the status entries stay in this egg's
        local 轨迹 — they are process noise that must never enter another
        egg's or the commander's context (it confuses the receiver and burns
        its context window).

        ``mode`` is kept for call-site compatibility; it no longer widens the
        payload (there is no "thinking + tools" transfer any more).
        ``limit`` caps the output to its last N characters (keeps recent turns).
        """
        lines = []
        for r in self.transcript:
            if r.get("kind") != "text":
                continue
            if r.get("role") != "assistant":
                continue
            text = (r.get("text") or "").strip()
            if not text:
                continue
            lines.append(f"助手: {text}")
        out = "\n".join(lines)
        if limit and len(out) > limit:
            out = "…（更早的历史略去）\n" + out[-limit:]
        return out

    # ---- worktree inspection (used by the "review code" panel) ----
    def list_files(self):
        """Files the user can inspect in the 「审阅代码」 panel.

        The worktree root is the review panel's root, so a deliverable written
        here is directly visible/openable by the user. ``agent_data/`` holds the
        SDK's internal app data (conversation db, logs) and is hidden — except
        for markdown files, which may be deliverables and must never stay
        invisible to the user.
        """
        out = []
        for p in self.worktree.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(self.worktree)
            if rel.parts and rel.parts[0] in ("agent_data",):
                if p.suffix.lower() != ".md":
                    continue
            out.append(str(rel))
        return sorted(out)

    def read_file(self, path):
        fp = (self.worktree / path).resolve()
        root = self.worktree.resolve()
        if fp != root and root not in fp.parents:
            raise ValueError("path escapes worktree")
        if not fp.is_file():
            return None
        return fp.read_text(errors="replace")
