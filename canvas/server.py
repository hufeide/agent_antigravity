"""FastAPI server: serves the canvas UI, exposes a WebSocket control channel,
and REST endpoints for inspecting each egg's worktree (review code)."""
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.responses import FileResponse

from .graph import Canvas
from .egg import reasoning_capture_state

logger = logging.getLogger(__name__)

FRONTEND = Path(__file__).resolve().parent / "index.html"

# In-flight produce() tasks (one per "command"/"commander" run). Kept in a set so
# fire-and-forget tasks aren't garbage-collected before they finish.
TASKS = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    canvas = Canvas()
    await canvas.start()
    app.state.canvas = canvas
    yield
    await canvas.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    # no-store: a reload must ALWAYS pick up the current frontend. Without it
    # the browser may serve a cached copy of index.html, so a freshly deployed
    # UI change appears to do nothing.
    return FileResponse(FRONTEND,
                        headers={"Cache-Control": "no-store, no-cache, max-age=0"})


@app.get("/api/version")
async def app_version():
    """Build stamp of the served frontend.

    The page polls it and reloads itself when the stamp changes, so a tab that
    was opened BEFORE a frontend edit picks the new code up on its own instead
    of silently keeping the old behaviour.
    """
    try:
        st = FRONTEND.stat()
        build = f"{int(st.st_mtime)}-{st.st_size}"
    except Exception:
        build = "0"
    return {"build": build}


@app.get("/api/egg/{egg_id}/files")
async def list_files(egg_id: str):
    canvas: Canvas = app.state.canvas
    egg = canvas.eggs.get(egg_id)
    if not egg:
        raise HTTPException(404)
    return {"files": egg.list_files()}


@app.get("/api/egg/{egg_id}/file")
async def read_file(egg_id: str, path: str = Query(...)):
    canvas: Canvas = app.state.canvas
    egg = canvas.eggs.get(egg_id)
    if not egg:
        raise HTTPException(404)
    try:
        content = egg.read_file(path)
    except ValueError:
        raise HTTPException(400, "invalid path")
    if content is None:
        raise HTTPException(404)
    return {"path": path, "content": content}


@app.get("/api/trace/commander")
async def trace_commander():
    """Chronological coordinator trace for the Commander.

    Declared BEFORE the parameterized route so 'commander' is not captured as an
    egg id by /api/trace/{egg_id}.
    """
    canvas: Canvas = app.state.canvas
    return {
        "connected": canvas.commander._agent is not None,
        "busy": canvas.commander.busy,
        "transcript": getattr(canvas.commander, "transcript", []),
    }


@app.get("/api/trace/{egg_id}")
async def trace_egg(egg_id: str):
    """Chronological conversation trace for one egg (user/model/tool records)."""
    canvas: Canvas = app.state.canvas
    egg = canvas.eggs.get(egg_id)
    if not egg:
        raise HTTPException(404)
    return {
        "eggId": egg_id,
        "title": egg.title,
        "connected": egg._agent is not None,
        "transcript": getattr(egg, "transcript", []),
    }


@app.post("/api/interrupt")
async def interrupt_egg(egg_id: str = Query(...)):
    """Interrupt a running egg's in-flight model turn (the stop button).

    Uses HTTP (not the WS channel) because the WS read-loop is busy streaming
    while an egg runs, so an interrupt must arrive on a separate request.

    ``egg_id='commander'`` interrupts the 总指挥 itself: its model turn is
    halted and the task streaming it is cancelled.
    """
    canvas: Canvas = app.state.canvas
    if egg_id == "commander":
        canvas.interrupt_commander()
        return {"ok": True, "eggId": egg_id, "busy": canvas.commander.busy}
    egg = canvas.eggs.get(egg_id)
    if not egg:
        raise HTTPException(404)
    egg.interrupt()
    return {"ok": True, "eggId": egg_id, "busy": egg.busy}


@app.get("/api/sessions")
async def list_sessions():
    """List all saved canvas sessions (newest first)."""
    return {"sessions": Canvas.list_sessions()}


@app.get("/api/completion")
async def completion_report():
    """Global convergence report for the current canvas (link.md 二.12)."""
    canvas: Canvas = app.state.canvas
    return canvas.completion_report()


@app.get("/api/tasks")
async def list_tasks():
    """Per-task runtime records (status, attempts, artifacts, blocked reason)."""
    canvas: Canvas = app.state.canvas
    return {"tasks": canvas.runtime_view()["tasks"]}


@app.get("/api/metrics")
async def metrics():
    """Counters / gauges / histograms (link.md 二.11)."""
    canvas: Canvas = app.state.canvas
    return canvas.runtime_view()["metrics"]


@app.get("/api/runtime")
async def runtime_view():
    """One-call runtime view: tasks + running eggs + metrics + approvals."""
    canvas: Canvas = app.state.canvas
    return canvas.runtime_view()


@app.get("/api/approvals")
async def list_approvals():
    """All approval requests, including expired ones (link.md 二.10)."""
    canvas: Canvas = app.state.canvas
    return {"approvals": canvas.approvals()}


@app.post("/api/approval/request")
async def request_approval(egg_id: str = Query(...), reason: str = ""):
    canvas: Canvas = app.state.canvas
    if egg_id not in canvas.eggs:
        raise HTTPException(404)
    approval = canvas.request_approval(egg_id, reason)
    return {"approval": approval, "approvals": canvas.approvals()}


@app.post("/api/approval/decide")
async def decide_approval(
    approval_id: str = Query(...),
    approved: bool = Query(...),
    note: str = "",
):
    canvas: Canvas = app.state.canvas
    try:
        approval = canvas.decide_approval(approval_id, approved, note)
    except KeyError:
        raise HTTPException(404)
    return {"approval": approval, "approvals": canvas.approvals()}


async def _send_event(ws: WebSocket, canvas, ev: dict):
    """Translate ONE orchestration event into a WS message.

    Shared by the commander (advice/control) and the one-click run:
      say      -> stream into the commander chat (schedule / narration)
      instruct -> push an instruction into an egg's chat
      egg      -> stream an egg's reply tokens
      end_egg  -> finalize that egg's stream
      end      -> finalize the commander stream

    ``canvas`` must be passed in: it is a local of the app factory (L25),
    NOT a module global — referencing it here without the parameter raises
    NameError on the first say event.
    """
    k = ev.get("k")
    if k == "say":
        # role="system" = a process note (stage header / error / failure summary):
        # it belongs in the 轨迹, NOT in the chat panel, which shows the
        # commander's own words only.
        canvas.commander.note_say(ev["text"], ev.get("role", "assistant"))
        await ws.send_json({"type": "token", "eggId": "commander",
                            "text": ev["text"], "role": ev.get("role", "assistant")})
    elif k == "say_break":
        # Start a NEW commander message (the previous one is finalized).
        canvas.commander.note_break()
        await ws.send_json({"type": "sayBreak", "eggId": "commander"})
    elif k == "instruct":
        await ws.send_json({"type": "msg", "eggId": ev["egg"], "role": "user",
                            "text": ev["text"]})
    elif k == "egg":
        await ws.send_json({"type": "token", "eggId": ev["egg"], "text": ev["text"]})
    elif k == "plan":
        await ws.send_json({"type": "plan", "steps": ev["steps"],
                            "round": ev.get("round", 0)})
    elif k == "status":
        await ws.send_json({"type": "status", "eggId": ev["egg"],
                            "status": ev["status"]})
    elif k == "end_egg":
        await ws.send_json({"type": "done", "eggId": ev["egg"]})
    elif k == "framework":
        # Orchestrator lifecycle (started/finished/failed/timeout/canceled).
        await ws.send_json({"type": "framework", "event": ev.get("event"),
                            "taskId": ev.get("taskId"), "text": ev.get("text")})
    elif k == "completion":
        await ws.send_json({"type": "completion", "report": ev["report"]})
    elif k == "sync_graph":
        # The commander created/removed eggs (e.g. replan) — tell the frontend
        # to re-sync the egg list so new eggs appear on the canvas.
        await ws.send_json({"type": "eggs", **canvas.view()})
    elif k == "end":
        # The commander panel is idle again -> its button is restored.
        await ws.send_json({"type": "status", "eggId": "commander", "status": "idle"})
        await ws.send_json({"type": "done", "eggId": "commander"})


def _run_commander_panel(ws: WebSocket, canvas: Canvas, kind: str,
                         agen_factory):
    """Run a 总指挥 run in a DEDICATED task and stream it to the client.

    The run (a coordination turn or the one-click run of the selected eggs) is
    executed in its own task — exactly like an egg's command — so the
    websocket read-loop stays free and the stop button can interrupt the run
    without killing the connection.

    ``kind`` is 'chat' or 'run'; it tells ``interrupt_commander()`` whether the
    eggs started by this run must be stopped as well.
    """
    gen = None

    async def produce():
        nonlocal gen
        task = asyncio.current_task()
        canvas.commander.begin_run(kind, task)
        try:
            await ws.send_json({"type": "status", "eggId": "commander",
                                "status": "running"})
            gen = agen_factory()
            async for ev in gen:
                await _send_event(ws, canvas, ev)
        except asyncio.CancelledError:
            # Interrupted by the stop button: tell the user, then let the
            # cancellation propagate so the workflow's finally blocks clean up.
            try:
                await ws.send_json({"type": "system", "eggId": "commander",
                                    "text": "⏹ 已打断总指挥运行。"})
            except Exception:  # pragma: no cover
                pass
            raise
        except Exception as ex:
            try:
                await ws.send_json({"type": "error",
                                    "message": f"总指挥运行失败：{ex}"})
            except Exception:  # pragma: no cover
                pass
        finally:
            if gen is not None:
                try:
                    await gen.aclose()
                except BaseException:  # pragma: no cover
                    pass
            # The run is over -> the UI restores the send button.
            canvas.commander.end_run(task)
            try:
                canvas.save()
            except Exception:  # pragma: no cover
                pass
            try:
                await ws.send_json({"type": "status", "eggId": "commander",
                                    "status": "idle"})
                await ws.send_json({"type": "done", "eggId": "commander"})
            except Exception:  # pragma: no cover
                pass

    task = asyncio.create_task(produce())
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)
    return task


async def _dispatch(msg: dict, ws: WebSocket, canvas: Canvas):
    t = msg.get("type")

    if t == "sync":
        await ws.send_json({"type": "eggs", **canvas.view()})

    elif t == "generate":
        created = await canvas.generate(msg.get("goal", ""))
        await ws.send_json({"type": "eggs", **canvas.view()})
        # A generate opens a BRAND-NEW session: wipe any stale commander history
        # shown in the panel so the user doesn't see the previous idea's chat.
        # ALSO clear the server-side transcript so /api/trace/commander returns
        # empty — otherwise the 轨迹 modal keeps showing the previous idea.
        canvas.commander.transcript.clear()
        await ws.send_json({"type": "clearHistory", "eggId": "commander"})
        for e in created:
            await ws.send_json(
                {"type": "system", "eggId": e.id,
                 "text": "（已根据目标由 LLM 生成该蛋的提示词）"}
            )
        # The generate opened a NEW dated session; refresh the picker + mark active.
        await ws.send_json({"type": "sessions", "sessions": Canvas.list_sessions()})
        await ws.send_json({"type": "session", "sessionId": canvas.session_id})

    elif t == "addEgg":
        await canvas.add_egg(msg.get("title", "新蛋"), msg.get("prompt", ""))
        await ws.send_json({"type": "eggs", **canvas.view()})

    elif t == "command":
        eid = msg.get("eggId")
        egg = canvas.eggs.get(eid)
        if not egg:
            return
        # Reference the connected eggs' session histories so this egg can build on
        # what its neighbours have actually done/decided.
        ctx = canvas.neighbor_context(eid)

        async def produce():
            await ws.send_json({"type": "status", "eggId": eid, "status": "running"})
            gen = egg.run(msg.get("text", ""), context=ctx)
            try:
                async for tok in gen:
                    await ws.send_json({"type": "token", "eggId": eid, "text": tok})
            except asyncio.CancelledError:
                try:
                    await ws.send_json({"type": "system", "eggId": eid,
                                        "text": "⏹ 已打断运行。"})
                except Exception:  # pragma: no cover
                    pass
            finally:
                # aclose releases the egg's locks (box + per-egg) immediately.
                try:
                    await gen.aclose()
                except Exception:  # pragma: no cover
                    pass
                try:
                    await ws.send_json({"type": "status", "eggId": eid, "status": "idle"})
                    await ws.send_json({"type": "done", "eggId": eid})
                    canvas.save()
                except Exception:  # pragma: no cover
                    pass

        # Fire-and-forget: run in a dedicated task so the websocket read-loop is
        # NOT blocked while this egg streams. Two eggs commanded "in parallel"
        # each get their own task and truly run concurrently (the stop button's
        # egg.interrupt cancels only this task, never the connection).
        task = asyncio.create_task(produce())
        TASKS.add(task)
        task.add_done_callback(TASKS.discard)

    elif t == "interrupt":
        eid = msg.get("eggId")
        if eid == "commander":
            canvas.interrupt_commander()
            return
        egg = canvas.eggs.get(eid)
        if egg:
            egg.interrupt()

    elif t == "listSessions":
        await ws.send_json({"type": "sessions", "sessions": Canvas.list_sessions()})

    elif t == "loadSession":
        sid = msg.get("sessionId")
        if sid:
            try:
                view = await canvas.load(sid)
                await ws.send_json({"type": "eggs", **view})
                # Fresh session => clear any stale displayed commander/egg histories
                # BEFORE replaying the loaded transcripts (otherwise the old chat
                # would be appended to).
                await ws.send_json({"type": "clearHistory", "eggId": "commander"})
                for e in canvas.eggs.values():
                    await ws.send_json({"type": "clearHistory", "eggId": e.id})
                # Replay each egg's (and the commander's) stored transcript so the
                # UI shows the prior conversation. The SDK resumes the LLM session
                # on the next message via the saved conversation_id + save_dir.
                for e in canvas.eggs.values():
                    await ws.send_json({"type": "history", "eggId": e.id,
                                        "transcript": e.transcript})
                await ws.send_json({"type": "history", "eggId": "commander",
                                    "transcript": canvas.commander.transcript})
            except Exception as ex:
                await ws.send_json({"type": "error", "message": f"load failed: {ex}"})

    elif t == "newSession":
        # Persist the current session (if any) then open a fresh empty session.
        try:
            canvas.save()
        except Exception:
            pass
        for e in list(canvas.eggs.values()):
            await e.stop()
        canvas.eggs.clear()
        canvas.edges.clear()
        sid = canvas._new_session()
        # Rebind durable stores to the new session directory so tasks/approvals
        # of the previous idea do not leak into the fresh one.
        canvas._bind_session(sid, canvas.session_dir, recover=False)
        if getattr(canvas, "_context", None) is not None:
            canvas._context.clear()
        await ws.send_json({"type": "eggs", **canvas.view()})
        # A new (empty) session starts with a clean commander chat.
        # ALSO clear the server-side transcript so /api/trace/commander returns
        # empty — otherwise the 轨迹 modal keeps showing the previous session.
        canvas.commander.transcript.clear()
        await ws.send_json({"type": "clearHistory", "eggId": "commander"})
        await ws.send_json({"type": "sessions", "sessions": Canvas.list_sessions()})
        await ws.send_json({"type": "session", "sessionId": sid})

    elif t == "compress":
        egg = canvas.eggs.get(msg.get("eggId"))
        if not egg:
            return

        async def do_compress():
            await ws.send_json({"type": "status", "eggId": egg.id, "status": "running"})
            try:
                summary = await egg.compress()
                await ws.send_json({"type": "compressed", "eggId": egg.id, "summary": summary})
            except asyncio.CancelledError:
                try:
                    await ws.send_json({"type": "system", "eggId": egg.id,
                                        "text": "⏹ 已取消压缩。"})
                except Exception:  # pragma: no cover
                    pass
            finally:
                try:
                    await ws.send_json({"type": "status", "eggId": egg.id, "status": "idle"})
                    canvas.save()
                except Exception:  # pragma: no cover
                    pass

        # Fire-and-forget: run in a dedicated task so the websocket read-loop is
        # NOT blocked while this egg compresses (the stop button's egg.interrupt
        # cancels only this task, never the connection — compress sets
        # self._task, so interrupt() reaches it).
        task = asyncio.create_task(do_compress())
        TASKS.add(task)
        task.add_done_callback(TASKS.discard)

    elif t == "combine":
        new = await canvas.combine(msg.get("ids", []))
        await ws.send_json({"type": "eggs", **canvas.view()})
        if new:
            await ws.send_json(
                {"type": "system", "eggId": new.id,
                 "text": "（已将所选蛋组合为新蛋）"}
            )

    elif t == "deleteEgg":
        await canvas.delete_egg(msg.get("id"))
        await ws.send_json({"type": "eggs", **canvas.view()})

    elif t == "deleteEdge":
        await canvas.delete_edge(msg.get("id"))
        await ws.send_json({"type": "eggs", **canvas.view()})

    elif t == "addEdge":
        await canvas.add_edge(msg.get("from"), msg.get("to"))
        await ws.send_json({"type": "eggs", **canvas.view()})

    elif t == "move":
        await canvas.move(msg.get("id"), msg.get("x"), msg.get("y"))

    elif t == "commander":
        targets = msg.get("targets") or (
            [msg.get("target")] if msg.get("target") else []
        )
        text = msg.get("text", "")
        if text:
            canvas.commander.note_user(text)
        # Run in a dedicated task so the stop button can interrupt the run
        # (and the websocket read-loop stays free) — same as an egg's command.
        _run_commander_panel(
            ws, canvas, "chat",
            lambda: canvas.commander_run(text, targets),
        )

    elif t == "run":
        targets = msg.get("targets") or []
        # Same as above: the one-click run is interruptible from the 总指挥
        # panel (its button turns into a stop square while it runs).
        _run_commander_panel(
            ws, canvas, "run",
            lambda: canvas.run_selected(targets),
        )

    elif t == "setHistoryMode":
        # 传递会话消息时永远只传 assistant 的正文：思考(💭)与工具调用(🔧)只留在
        # 本地轨迹，不再有任何"全部历史（含思考/工具）"的传递模式。
        canvas.history_mode = "brief"
        await ws.send_json({"type": "historyMode", "mode": canvas.history_mode})

    elif t == "completion":
        await ws.send_json({"type": "completion", "report": canvas.completion_report()})

    elif t == "runtime":
        await ws.send_json({"type": "runtime", **canvas.runtime_view()})

    elif t == "tasks":
        await ws.send_json({"type": "tasks", "tasks": canvas.runtime_view()["tasks"]})

    elif t == "metrics":
        await ws.send_json({"type": "metrics", "metrics": canvas.runtime_view()["metrics"]})

    elif t == "approvalRequest":
        approval = canvas.request_approval(msg.get("eggId"), msg.get("reason", ""))
        await ws.send_json({"type": "approvals", "approvals": canvas.approvals(),
                            "requested": approval})

    elif t == "approvalDecide":
        try:
            canvas.decide_approval(
                msg.get("approvalId"), bool(msg.get("approved")), msg.get("note", ""))
        except KeyError as ex:
            await ws.send_json({"type": "error", "message": str(ex)})
        await ws.send_json({"type": "approvals", "approvals": canvas.approvals()})

    elif t == "setReasoning":
        from .egg import set_reasoning_capture
        set_reasoning_capture(bool(msg.get("on")))
        await ws.send_json({"type": "reasoning", **reasoning_capture_state()})

    else:
        await ws.send_json({"type": "error", "message": f"unknown type: {t}"})


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    canvas: Canvas = app.state.canvas
    try:
        while True:
            msg = await websocket.receive_json()
            try:
                await _dispatch(msg, websocket, canvas)
            except Exception as ex:  # keep the socket alive on per-message errors
                await websocket.send_json({"type": "error", "message": str(ex)})
    except Exception as ex:  # client disconnected
        logger.debug("[ws] closed: %s", ex)
