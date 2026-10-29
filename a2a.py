"""A2A network layer (link.md 四.第二层 Agent 执行与协作 的网络扩展 / 一.4 消息通信).

SDK status: 是 — 官方 A2A Python SDK（a2a 包）真实使用：

  * AgentExecutor / DefaultRequestHandler / InMemoryTaskStore ....... a2a.server
  * create_jsonrpc_routes / create_rest_routes / create_agent_card_routes
    add_a2a_routes_to_fastapi ..................................... a2a.server.routes
  * ClientFactory / ClientConfig / BaseClient ...................... a2a.client
  * A2ACardResolver ................................................ a2a.client.card_resolver
  * AgentCard / AgentInterface / AgentCapabilities / Message / Part
    Role / Artifact / TaskStatus / TaskState / SendMessageRequest ... a2a.types

agent_runtime.py 把单个 Agent 跑在进程内；本模块把同一套执行器（真实的
agent_runtime.run_task 或模拟执行）包装为 A2A agent 服务暴露给网络，并提供
官方客户端调用任意远程 A2A agent。这就是 link.md 一.4「消息通信与复命协议」
的跨网络实现：任务分配（TASK_ASSIGN -> A2A Message）与子任务复命
（REPORT <- A2A 结果回写 Task）。

本模块提供：

  * build_agent_card ......... 构建 AgentCard（对应 agent_runtime.build_agent_config）
  * A2ATaskExecutor .......... 把本地执行回调包装为 A2A AgentExecutor
  * A2AServer ................. 把执行器暴露为 A2A 1.0 服务（JSONRPC + REST）
  * A2AClient ................. 官方客户端封装：流式调用 / 查询 Task
  * a2a_dispatch .............. Orchestrator 的 dispatch 回调：经 A2A 远程执行
                                并把结果写回 Task（复命），可挂 MessageBus / AuditLog
  * make_sdk_executor ......... 把 agent_runtime.run_task 包装成 A2A 执行回调

演示（无需 SDK 运行时二进制）：
    python -m demo.a2a
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from . import messaging, models

logger = logging.getLogger(__name__)

# --- Guarded A2A SDK import ---------------------------------------------------
try:
    import httpx
    from fastapi import FastAPI
    import uvicorn

    from a2a.client import ClientCallContext, ClientConfig, ClientFactory
    from a2a.client.base_client import BaseClient
    from a2a.client.card_resolver import A2ACardResolver
    from a2a.server.agent_execution import AgentExecutor
    from a2a.server.request_handlers import DefaultRequestHandler
    from a2a.server.routes import (
        add_a2a_routes_to_fastapi,
        create_agent_card_routes,
        create_jsonrpc_routes,
        create_rest_routes,
    )
    from a2a.server.tasks import InMemoryTaskStore
    from a2a.types import (
        AgentCapabilities,
        AgentCard,
        AgentInterface,
        AgentSkill,
        Artifact,
        Message,
        Part,
        Role,
        SendMessageRequest,
        Task,
        TaskArtifactUpdateEvent,
        TaskStatus,
        TaskStatusUpdateEvent,
        TaskState,
    )

    A2A_SDK_AVAILABLE = True
    _A2A_ERROR: Exception | None = None
except Exception as _err:  # pragma: no cover - environment dependent
    A2A_SDK_AVAILABLE = False
    _A2A_ERROR = _err
    logger.warning("A2A SDK not fully importable: %s", _err)


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------
def build_agent_card(
    name: str,
    description: str,
    url: str,
    *,
    protocol_binding: str = "JSONRPC",
    protocol_version: str = "1.0",
    version: str = "0.1.0",
    streaming: bool = True,
    input_modes: tuple[str, ...] = ("text",),
    output_modes: tuple[str, ...] = ("text",),
) -> AgentCard:
    """Build a real A2A AgentCard advertising one interface.

    The card is the agent's identity and capability contract: clients read it
    to discover how to reach and talk to the agent (link.md 一.4).
    """
    return AgentCard(
        name=name,
        description=description,
        supported_interfaces=[
            AgentInterface(
                url=url,
                protocol_binding=protocol_binding,
                protocol_version=protocol_version,
            )
        ],
        version=version,
        capabilities=AgentCapabilities(streaming=streaming),
        default_input_modes=list(input_modes),
        default_output_modes=list(output_modes),
    )


def text_from_parts(parts: Any) -> str:
    """Flatten the text parts of an A2A message/artifact into one string."""
    return "".join(p.text for p in parts if p.text)


def _new_message(text: str, role: int = Role.ROLE_AGENT) -> Message:
    return Message(
        message_id=f"msg-{uuid.uuid4().hex[:8]}",
        role=role,
        parts=[Part(text=text)],
    )


async def _enqueue_status(
    event_queue: Any, task_id: str, context_id: str, state: int, text: str
) -> None:
    await event_queue.enqueue_event(
        TaskStatusUpdateEvent(
            task_id=task_id,
            context_id=context_id,
            status=TaskStatus(state=state, message=_new_message(text)),
        )
    )


async def _enqueue_artifact(
    event_queue: Any,
    task_id: str,
    context_id: str,
    text: str,
    *,
    append: bool = False,
    last_chunk: bool = True,
) -> None:
    await event_queue.enqueue_event(
        TaskArtifactUpdateEvent(
            task_id=task_id,
            context_id=context_id,
            artifact=Artifact(
                artifact_id=f"art-{uuid.uuid4().hex[:8]}",
                name="result",
                parts=[Part(text=text)],
            ),
            append=append,
            last_chunk=last_chunk,
        )
    )


async def _resolve(value: Any) -> Any:
    """Await awaitable executor results; pass through plain values."""
    if inspect.isawaitable(value):
        return await value
    return value


# ---------------------------------------------------------------------------
# Server side: wrap a local executor as an A2A agent
# ---------------------------------------------------------------------------
class A2ATaskExecutor(AgentExecutor):
    """Wrap a local execution callback as a real A2A AgentExecutor.

    executor (`Callable[[str], str | Awaitable[str]]`):
        Receives the flattened user prompt and returns the execution result.
        For real runs, wrap agent_runtime.run_task with make_sdk_executor().

    The executor streams one Task lifecycle to the caller: SUBMITTED ->
    WORKING -> (artifact) -> COMPLETED / FAILED / CANCELED. Task state is
    managed by the SDK's InMemoryTaskStore via DefaultRequestHandler, so the
    remote client can poll get_task for the current status.
    """

    def __init__(
        self,
        executor: Callable[[str], str | Awaitable[str]],
        *,
        audit: Callable[..., None] | None = None,
    ) -> None:
        super().__init__()
        self._executor = executor
        self._audit = audit
        self._cancelled: set[str] = set()

    async def execute(self, context: Any, event_queue: Any) -> None:
        task_id = context.task_id
        context_id = context.context_id
        prompt = text_from_parts(context.message.parts) if context.message else ""
        if not prompt:
            prompt = context.get_user_input()

        # The protocol requires a Task object to be enqueued before any
        # TaskStatusUpdateEvent (active_task.py:347). context.current_task is
        # None for a brand-new task, so build one from the IDs that
        # RequestContext already generated; the Task carries SUBMITTED.
        task = context.current_task or Task(
            id=task_id,
            context_id=context_id,
            status=TaskStatus(
                state=TaskState.TASK_STATE_SUBMITTED,
                message=_new_message("Task submitted"),
            ),
            history=[context.message] if context.message else [],
        )
        await event_queue.enqueue_event(task)
        await _enqueue_status(
            event_queue, task_id, context_id,
            TaskState.TASK_STATE_WORKING, "Agent is working",
        )

        try:
            result = await _resolve(self._executor(prompt))
        except asyncio.CancelledError:
            await _enqueue_status(
                event_queue, task_id, context_id,
                TaskState.TASK_STATE_CANCELED, "Task canceled",
            )
            raise
        except Exception as exc:  # noqa: BLE001 - report the failure as a Task state
            await _enqueue_status(
                event_queue, task_id, context_id,
                TaskState.TASK_STATE_FAILED, f"Execution failed: {exc}",
            )
            if self._audit is not None:
                self._audit("a2a_task_failed", task_id=task_id, error=str(exc))
            return

        if task_id in self._cancelled:
            self._cancelled.discard(task_id)
            await _enqueue_status(
                event_queue, task_id, context_id,
                TaskState.TASK_STATE_CANCELED, "Task canceled",
            )
            return

        await _enqueue_artifact(event_queue, task_id, context_id, str(result))
        await _enqueue_status(
            event_queue, task_id, context_id,
            TaskState.TASK_STATE_COMPLETED, "Task completed",
        )
        if self._audit is not None:
            self._audit("a2a_task_finished", task_id=task_id, text_len=len(str(result)))

    async def cancel(self, context: Any, event_queue: Any) -> None:
        if context.task_id:
            self._cancelled.add(context.task_id)
        await _enqueue_status(
            event_queue,
            context.task_id,
            context.context_id,
            TaskState.TASK_STATE_CANCELED,
            "Task canceled",
        )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class A2AServer:
    """Serve a local executor as an A2A 1.0 agent (JSONRPC + REST + card).

    The SDK's create_jsonrpc_routes / create_rest_routes build the real
    endpoints; add_a2a_routes_to_fastapi mounts them, and create_agent_card_routes
    exposes the card at /.well-known/agent-card.json so any A2A client can
    discover this agent (link.md 一.4 discovery).

    start() launches uvicorn on a background thread and returns the URL once
    the server is accepting requests; stop() shuts it down.
    """

    def __init__(
        self,
        executor: Callable[[str], str | Awaitable[str]],
        card: AgentCard | None = None,
        *,
        name: str = "a2a-agent",
        description: str = "A2A agent served by the demo framework",
        host: str = "127.0.0.1",
        port: int = 0,
        audit: Callable[..., None] | None = None,
    ) -> None:
        if not A2A_SDK_AVAILABLE:
            raise RuntimeError(f"A2A SDK unavailable: {_A2A_ERROR}")
        self._host = host
        self._port = port or _free_port()
        self._card = card or build_agent_card(
            name, description, f"http://{host}:{self._port}"
        )
        executor_obj = A2ATaskExecutor(executor, audit=audit)
        request_handler = DefaultRequestHandler(
            executor_obj, InMemoryTaskStore(), self._card
        )
        app = FastAPI()
        add_a2a_routes_to_fastapi(
            app,
            agent_card_routes=create_agent_card_routes(self._card),
            jsonrpc_routes=create_jsonrpc_routes(
                request_handler, rpc_url="/"
            ),
            rest_routes=create_rest_routes(request_handler),
        )
        self._app = app
        self._uvicorn = uvicorn.Server(
            uvicorn.Config(app, host=host, port=self._port, log_level="warning")
        )
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}"

    @property
    def card(self) -> AgentCard:
        return self._card

    def start(self) -> str:
        self._thread = threading.Thread(target=self._uvicorn.run, daemon=True)
        self._thread.start()
        deadline = time.time() + 10
        while not self._uvicorn.started:
            if time.time() > deadline:
                raise RuntimeError("A2A server did not start in 10s")
            time.sleep(0.02)
        logger.info(
            "A2A agent %r listening on %s (card: %s)",
            self._card.name, self.url, "/.well-known/agent-card.json",
        )
        return self.url

    def stop(self) -> None:
        self._uvicorn.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Client side: call remote A2A agents with the official client
# ---------------------------------------------------------------------------
@dataclass
class A2AReply:
    """Aggregated result of one A2A send_message round-trip."""

    text: str = ""
    state: int | None = None  # final TaskState value
    task_id: str = ""
    context_id: str = ""
    artifacts: list[str] = field(default_factory=list)


async def _resolve_card(url: str) -> AgentCard:
    async with httpx.AsyncClient(timeout=10.0) as http:
        return await A2ACardResolver(http, url).get_agent_card()


class A2AClient:
    """Official A2A client wrapper.

    Send prompts to a remote agent, streaming the protocol's
    message / artifact_update / status_update / task events back into one
    aggregated A2AReply. Pass a card (e.g. fetched from the orchestrator's
    registry) or a base URL; the card is resolved from
    /.well-known/agent-card.json in the latter case.
    """

    def __init__(
        self,
        card: AgentCard | None = None,
        *,
        url: str | None = None,
        client: BaseClient | None = None,
    ) -> None:
        if client is None:
            if card is None:
                if url is None:
                    raise ValueError("A2AClient requires a card, a url, or a client")
                card = asyncio.run(_resolve_card(url))
            client = ClientFactory(
                ClientConfig(streaming=True, polling=False)
            ).create(card)
        self._client = client
        self.card = card

    @classmethod
    async def connect(cls, url: str, **kwargs: Any) -> "A2AClient":
        """Resolve the agent card over HTTP (works inside a running loop)."""
        card = await _resolve_card(url)
        return cls(card, **kwargs)

    @property
    def name(self) -> str:
        return self.card.name

    async def send(
        self,
        prompt: str,
        *,
        context_id: str | None = None,
        task_id: str | None = None,
        timeout: float | None = None,
    ) -> A2AReply:
        """Send one message and aggregate the streamed response.

        A fresh task is created when neither context_id nor task_id is given;
        pass the reply's context_id back to continue the same remote
        conversation. ``timeout`` (seconds) is forwarded to the JSON-RPC call
        as a ClientCallContext; real LLM backends (e.g. local llama.cpp via
        the Antigravity runtime) may need a longer budget than the default.
        """
        message = Message(
            message_id=f"msg-{uuid.uuid4().hex[:8]}",
            role=Role.ROLE_USER,
            parts=[Part(text=prompt)],
        )
        if context_id:
            message.context_id = context_id
        if task_id:
            message.task_id = task_id

        request = SendMessageRequest(message=message)
        chunks: list[str] = []
        artifacts: list[str] = []
        state: int | None = None
        reply_context = context_id or ""
        reply_task = task_id or ""

        call_ctx = ClientCallContext(timeout=timeout) if timeout else None
        async for response in self._client.send_message(request, context=call_ctx):
            kind = response.WhichOneof("payload")
            if kind == "message":
                reply_context = response.message.context_id or reply_context
                reply_task = response.message.task_id or reply_task
                chunks.extend(text_from_parts(response.message.parts))
            elif kind == "artifact_update":
                update = response.artifact_update
                reply_context = update.context_id or reply_context
                reply_task = update.task_id or reply_task
                artifacts.append(update.artifact.artifact_id)
                chunks.extend(text_from_parts(update.artifact.parts))
            elif kind == "status_update":
                update = response.status_update
                reply_context = update.context_id or reply_context
                state = update.status.state
            elif kind == "task":
                reply_context = response.task.context_id or reply_context
                reply_task = response.task.id or reply_task
                state = response.task.status.state
                for artifact in response.task.artifacts:
                    artifacts.append(artifact.artifact_id)
                    chunks.extend(text_from_parts(artifact.parts))

        return A2AReply(
            text="".join(chunks),
            state=state,
            task_id=reply_task,
            context_id=reply_context,
            artifacts=artifacts,
        )

    async def get_task(self, task_id: str) -> Any:
        from a2a.types import GetTaskRequest

        return await self._client.get_task(GetTaskRequest(id=task_id))

    async def aclose(self) -> None:
        await self._client.close()

    async def __aenter__(self) -> "A2AClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()


# ---------------------------------------------------------------------------
# Framework integration: remote execution + reporting (复命)
# ---------------------------------------------------------------------------
def make_sdk_executor(
    config: Any, *, agent_id: str = "remote-a2a"
) -> Callable[[str], Awaitable[str]]:
    """Wrap agent_runtime.run_task as an A2A execution callback.

    The remote A2A server then executes real SDK agents: the prompt becomes a
    Task whose title drives agent_runtime.run_task (agent_runtime.py:237).
    """
    from .agent_runtime import run_task  # deferred: avoids circular imports

    async def _execute(prompt: str) -> str:
        task = models.Task(title=prompt, owner_agent_id=agent_id)
        return await run_task(task, prompt, config)

    return _execute


async def a2a_dispatch(
    client: A2AClient,
    task: "models.Task",
    *,
    sender: str = "cmd",
    bus: "messaging.MessageBus | None" = None,
    audit: Callable[..., None] | None = None,
    context_id: str | None = None,
    timeout: float | None = None,
) -> str:
    """Execute one task on a remote A2A agent and write the result back.

    Designed as an Orchestrator dispatch callback (orchestrator.py:70): the
    task's title+description become the A2A prompt, the aggregated reply is
    stored as the task's report payload and a message artifact, and a REPORT
    AgentMessage is published on the bus so the commander sees the
    复命 (link.md 一.2 / 一.4). The reply's context_id is remembered so the
    next dispatch continues the same remote conversation.

    Returns the remote agent's reply text.
    """
    prompt = f"{task.title}\n{task.description}".strip() or task.title
    reply = await client.send(
        prompt,
        context_id=context_id or task.metadata.get("a2a_context_id") or None,
        timeout=timeout,
    )

    state_name = (
        TaskState.Name(reply.state) if reply.state else "TASK_STATE_UNSPECIFIED"
    )
    task.metadata["a2a"] = {
        "state": state_name,
        "context_id": reply.context_id,
        "task_id": reply.task_id,
    }
    if reply.context_id:
        task.metadata["a2a_context_id"] = reply.context_id
    task.artifacts.append(
        models.Artifact(
            task_id=task.task_id,
            kind="message",
            path="a2a://result",
            metadata={"text": reply.text},
        )
    )
    task.report_payload = {"text": reply.text, "state": state_name}
    task.report_submitted = True
    task.status = models.TaskStatus.SUCCESS

    if bus is not None:
        bus.publish(
            messaging.AgentMessage(
                msg_type=messaging.MessageType.REPORT,
                task_id=task.task_id,
                agent_id=task.owner_agent_id,
                sender=sender,
                status="success",
                payload={
                    "a2a_state": state_name,
                    "context_id": reply.context_id,
                },
            )
        )
    if audit is not None:
        audit("a2a_task_finished", task_id=task.task_id, state=state_name,
              text_len=len(reply.text))
    return reply.text


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
async def _simulated_executor(prompt: str) -> str:
    logger.info("[remote-agent] received prompt: %.60s", prompt)
    await asyncio.sleep(0.05)
    title = prompt.splitlines()[0] if prompt else "task"
    return f"模拟远程 agent 已完成：{title}"


async def demo() -> None:
    if not A2A_SDK_AVAILABLE:
        raise RuntimeError(f"A2A SDK unavailable: {_A2A_ERROR}")

    audit_events: list[dict[str, Any]] = []

    def _audit(event: str, **kwargs: Any) -> None:
        audit_events.append({"event": event, **kwargs})

    from . import orchestrator

    server = A2AServer(
        _simulated_executor,
        name="sim-agent",
        description="simulated A2A agent for the demo",
        audit=_audit,
    )
    url = server.start()
    try:
        bus = messaging.MessageBus()

        async with await A2AClient.connect(url) as client:
            logger.info("connected to A2A agent %r at %s", client.name, url)

            # 1) Streamed round-trip: status updates + artifact are aggregated.
            reply = await client.send("请生成模块 A 的实现")
            logger.info(
                "reply: %r state=%s context=%s artifacts=%s",
                reply.text, reply.state, reply.context_id, reply.artifacts,
            )

            # 2) Same-conversation continuation via the context_id.
            reply2 = await client.send(
                "请补充模块 A 的单元测试",
                context_id=reply.context_id,
            )
            logger.info(
                "continued conversation context=%s state=%s",
                reply2.context_id, reply2.state,
            )
            assert reply2.context_id == reply.context_id, "context must continue"

            # 3) Orchestrator integration: dispatch a task over A2A and
            #    let a2a_dispatch write the report back (复命).
            orch = orchestrator.Orchestrator(max_concurrency=2)
            remote_task = models.Task(
                task_id="task-remote",
                title="通过 A2A 远程执行",
                description="任务描述随 A2A 消息发给远程 agent",
                owner_agent_id="sim-agent",
            )
            orch.register(remote_task)
            await orch.arun(lambda t: a2a_dispatch(client, t, sender="cmd", bus=bus))
            logger.info(
                "remote task status=%s report=%r a2a=%s",
                remote_task.status.value,
                remote_task.report_payload.get("text"),
                remote_task.metadata.get("a2a"),
            )

            # 4) Reporting protocol on the bus (link.md 一.4 复命).
            for msg in bus.history(task_id="task-remote"):
                logger.info(
                    "bus: %s %s -> %s (%s)",
                    msg.msg_type.value, msg.sender, msg.status,
                    msg.payload.get("a2a_state", ""),
                )

        logger.info("audit events: %s", audit_events)
    finally:
        server.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(demo())
