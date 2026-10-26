"""Inter-agent message / reporting protocol (link.md section 一.4).

SDK status: 否. The SDK has no structured inter-agent message type
(task_id / agent_id / status / artifacts / error). The closest SDK construct is
Step.trajectory_id / parent_trajectory_id / depth / status / error (types.py:1136),
which only does trajectory bookkeeping for sub-agents, not a message contract.

This module provides the message contract plus an in-process bus with:

  * deduplication by msg_id
  * correlation tracking (request/reply) and async wait_for
  * acknowledgement with deadline (require_ack / ack / expire_acks)
  * write-permission control via an optional authorize callback
  * wildcard subscriptions and full history

Replace the bus with a durable broker once the SDK exposes an inter-agent
transport; the AgentMessage contract is transport-independent.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)


class MessageType(enum.Enum):
    TASK_ASSIGN = "task_assign"
    PROGRESS = "progress"
    BLOCKED = "blocked"
    DELIVER = "deliver"
    REPORT = "report"  # 复命
    CANCEL = "cancel"


@dataclass
class AgentMessage:
    """Structured message exchanged between agents / orchestrator."""

    msg_type: MessageType
    task_id: str
    agent_id: str
    status: str = ""
    artifacts: list[str] = field(default_factory=list)
    error: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    msg_id: str = field(default_factory=lambda: f"msg-{uuid.uuid4().hex[:8]}")
    correlation_id: str | None = None  # links reply to a prior message
    sender: str = ""
    require_ack: bool = False
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "msg_id": self.msg_id,
            "type": self.msg_type.value,
            "task_id": self.task_id,
            "agent_id": self.agent_id,
            "sender": self.sender,
            "status": self.status,
            "artifacts": list(self.artifacts),
            "error": self.error,
            "payload": dict(self.payload),
            "correlation_id": self.correlation_id,
            "require_ack": self.require_ack,
            "timestamp": self.timestamp,
        }


class MessageBus:
    """In-process message bus with dedupe / correlation / ack / timeout.

    TODO(platform): replace with a durable broker (e.g. pub/sub) once the SDK
    exposes an inter-agent transport. SDK provides none today (link.md 六.4).
    """

    def __init__(
        self,
        authorize: Callable[[AgentMessage], bool] | None = None,
        max_history: int = 2000,
    ) -> None:
        self._subscribers: dict[str, list[tuple[str, Callable[[AgentMessage], Any]]]] = (
            defaultdict(list)
        )
        self._seen: set[str] = set()
        self._pending: dict[str, tuple[AgentMessage, float]] = {}
        self._expired: dict[str, float] = {}
        self._history: list[AgentMessage] = []
        self._by_correlation: dict[str, list[AgentMessage]] = defaultdict(list)
        self._ack_waiters: dict[str, list[asyncio.Future]] = defaultdict(list)
        self._msg_waiters: dict[str, list[tuple[str | None, asyncio.Future]]] = (
            defaultdict(list)
        )
        self.authorize = authorize
        self.max_history = max_history
        self._lock = threading.RLock()

    # -- subscriptions ------------------------------------------------------
    def subscribe(self, agent_id: str, handler: Callable[[AgentMessage], Any]) -> str:
        """Subscribe to messages addressed to agent_id ('*' = every message)."""
        sid = f"sub-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[agent_id].append((sid, handler))
        return sid

    def unsubscribe(self, subscription_id: str) -> bool:
        with self._lock:
            for agent_id, entries in list(self._subscribers.items()):
                for sid, _ in list(entries):
                    if sid == subscription_id:
                        entries.remove((sid, _))
                        if not entries:
                            self._subscribers.pop(agent_id, None)
                        return True
        return False

    def subscriber_count(self, agent_id: str | None = None) -> int:
        if agent_id is None:
            return sum(len(v) for v in self._subscribers.values())
        return len(self._subscribers.get(agent_id, []))

    # -- publish ------------------------------------------------------------
    def publish(self, msg: AgentMessage, timeout: float = 30.0) -> bool:
        """Publish a message. Returns True if accepted (not a duplicate)."""
        with self._lock:
            if msg.msg_id in self._seen:  # dedupe
                return False
            if self.authorize is not None and not self.authorize(msg):
                raise PermissionError(
                    f"agent {msg.sender or msg.agent_id!r} may not publish "
                    f"{msg.msg_type.value} for task {msg.task_id!r}"
                )
            self._seen.add(msg.msg_id)
            self._history.append(msg)
            if len(self._history) > self.max_history:
                del self._history[: len(self._history) - self.max_history]
            if msg.correlation_id:
                self._by_correlation[msg.correlation_id].append(msg)
            if msg.require_ack:
                self._pending[msg.msg_id] = (msg, time.time() + timeout)
            handlers = list(self._subscribers.get(msg.agent_id, [])) + list(
                self._subscribers.get("*", [])
            )

        for _, handler in handlers:
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        asyncio.run(result)
                    else:
                        loop.create_task(result)
            except Exception:  # noqa: BLE001 - one bad subscriber must not stop others
                logger.exception("subscriber failed for %s", msg.msg_id)

        self._resolve_waiters(msg)
        return True

    async def apublish(self, msg: AgentMessage, timeout: float = 30.0) -> bool:
        """Publish and, when require_ack is set, wait for acknowledgement."""
        fut: asyncio.Future | None = None
        if msg.require_ack:
            fut = asyncio.get_running_loop().create_future()
            self._ack_waiters[msg.msg_id].append(fut)
        try:
            if not self.publish(msg, timeout=timeout):
                return False
            if fut is None:
                return True
            try:
                await asyncio.wait_for(fut, timeout=timeout)
                return True
            except asyncio.TimeoutError:
                self.expire_acks()
                return False
        finally:
            if fut is not None:
                self._ack_waiters.pop(msg.msg_id, None)

    # -- acknowledgement ----------------------------------------------------
    def ack(self, msg_id: str, agent_id: str = "") -> bool:
        with self._lock:
            entry = self._pending.pop(msg_id, None)
        if entry is None:
            return False
        self._resolve_ack(msg_id)
        logger.debug("ack %s by %s", msg_id, agent_id or "?")
        return True

    def pending_acks(self) -> list[str]:
        with self._lock:
            return list(self._pending)

    def expired_acks(self) -> list[str]:
        with self._lock:
            return list(self._expired)

    def expire_acks(self, now: float | None = None) -> list[str]:
        """Move timed-out acknowledgements to the expired set."""
        now = time.time() if now is None else now
        expired: list[str] = []
        with self._lock:
            for msg_id, (_, deadline) in list(self._pending.items()):
                if deadline <= now:
                    self._pending.pop(msg_id, None)
                    self._expired[msg_id] = now
                    expired.append(msg_id)
        return expired

    def _resolve_ack(self, msg_id: str) -> None:
        waiters = self._ack_waiters.pop(msg_id, [])
        for fut in waiters:
            if not fut.done():
                fut.set_result(True)

    # -- correlation --------------------------------------------------------
    def _resolve_waiters(self, msg: AgentMessage) -> None:
        self._resolve_ack(msg.msg_id)
        if not msg.correlation_id:
            return
        for msg_type, fut in list(self._msg_waiters.get(msg.correlation_id, [])):
            if fut.done():
                continue
            if msg_type is None or msg_type == msg.msg_type.value:
                fut.set_result(msg)
        self._msg_waiters.pop(msg.correlation_id, None)

    def correlate(self, correlation_id: str) -> list[AgentMessage]:
        with self._lock:
            return list(self._by_correlation.get(correlation_id, []))

    async def wait_for(
        self,
        correlation_id: str,
        msg_type: str | MessageType | None = None,
        timeout: float = 30.0,
    ) -> AgentMessage | None:
        """Wait for a reply correlated with a prior message."""
        wanted = msg_type.value if isinstance(msg_type, MessageType) else msg_type
        for msg in self.correlate(correlation_id):
            if wanted is None or msg.msg_type.value == wanted:
                return msg
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._msg_waiters[correlation_id].append((wanted, fut))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            entries = self._msg_waiters.get(correlation_id, [])
            self._msg_waiters[correlation_id] = [
                (mt, f) for mt, f in entries if f is not fut
            ]

    def reply(
        self,
        request: AgentMessage,
        msg_type: MessageType,
        *,
        status: str = "",
        artifacts: list[str] | None = None,
        error: str | None = None,
        payload: dict[str, Any] | None = None,
        agent_id: str | None = None,
        require_ack: bool = False,
    ) -> AgentMessage:
        """Build a reply correlated with request (targets its sender)."""
        return AgentMessage(
            msg_type=msg_type,
            task_id=request.task_id,
            agent_id=agent_id or request.sender or request.agent_id,
            sender=request.agent_id,
            status=status,
            artifacts=list(artifacts or []),
            error=error,
            payload=dict(payload or {}),
            correlation_id=request.msg_id,
            require_ack=require_ack,
        )

    # -- history ------------------------------------------------------------
    def history(
        self,
        agent_id: str | None = None,
        msg_type: MessageType | str | None = None,
        task_id: str | None = None,
    ) -> list[AgentMessage]:
        wanted = msg_type.value if isinstance(msg_type, MessageType) else msg_type
        with self._lock:
            return [
                m
                for m in self._history
                if (agent_id is None or m.agent_id == agent_id)
                and (wanted is None or m.msg_type.value == wanted)
                and (task_id is None or m.task_id == task_id)
            ]

    def clear(self) -> None:
        with self._lock:
            self._subscribers.clear()
            self._seen.clear()
            self._pending.clear()
            self._expired.clear()
            self._history.clear()
            self._by_correlation.clear()
