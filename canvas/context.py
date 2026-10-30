"""Versioned shared-context store (link.md 二.9).

The SDK has no cross-agent knowledge base (only StateStore + HookContext, which
are scoped to one agent session). Eggs therefore need an explicit policy for
what is shared, with which version, and for how long:

  * every entry is versioned per task (v1, v2, ...)
  * entries can expire (TTL) so stale context does not mislead a new agent
  * the snapshot is size-capped, so N eggs cannot blow up the prompt
  * callers choose which tasks to include (exclude self by default)

This is deliberately small: it is the shared/isolated boundary, not a vector
database.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class ContextEntry:
    task_id: str
    text: str
    version: int = 1
    source: str = ""
    updated_at: float = field(default_factory=time.time)
    ttl: float | None = None

    def expired(self, now: float | None = None) -> bool:
        if not self.ttl:
            return False
        return (time.time() if now is None else now) >= self.updated_at + self.ttl

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "text": self.text,
            "version": self.version,
            "source": self.source,
            "updated_at": self.updated_at,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContextEntry":
        return cls(
            task_id=data.get("task_id", ""),
            text=data.get("text", ""),
            version=int(data.get("version", 1)),
            source=data.get("source", ""),
            updated_at=float(data.get("updated_at") or time.time()),
            ttl=data.get("ttl"),
        )


class ContextStore:
    """In-memory, versioned, TTL-aware shared context."""

    def __init__(self, ttl: float | None = None, max_chars: int = 8000) -> None:
        self.ttl = ttl
        self.max_chars = max_chars
        self._entries: dict[str, ContextEntry] = {}
        self._lock = threading.RLock()

    # -- writes -------------------------------------------------------------
    def put(
        self,
        task_id: str,
        text: str,
        source: str = "",
        ttl: float | None = None,
    ) -> ContextEntry:
        with self._lock:
            previous = self._entries.get(task_id)
            entry = ContextEntry(
                task_id=task_id,
                text=(text or "").strip(),
                version=(previous.version + 1) if previous else 1,
                source=source,
                ttl=self.ttl if ttl is None else ttl,
            )
            self._entries[task_id] = entry
            return entry

    def drop(self, task_id: str) -> bool:
        with self._lock:
            return self._entries.pop(task_id, None) is not None

    def prune(self, now: float | None = None) -> list[str]:
        with self._lock:
            dead = [k for k, v in self._entries.items() if v.expired(now)]
            for key in dead:
                self._entries.pop(key, None)
            return dead

    # -- reads --------------------------------------------------------------
    def get(self, task_id: str) -> ContextEntry | None:
        with self._lock:
            entry = self._entries.get(task_id)
            if entry is not None and entry.expired():
                self._entries.pop(task_id, None)
                return None
            return entry

    def all(self) -> list[ContextEntry]:
        self.prune()
        with self._lock:
            return list(self._entries.values())

    def snapshot(
        self,
        exclude: Iterable[str] = (),
        include: Iterable[str] | None = None,
        max_chars: int | None = None,
    ) -> str:
        """Render a versioned, size-capped context block."""
        excluded = set(exclude)
        allowed = set(include) if include is not None else None
        entries = [
            e for e in self.all()
            if e.task_id not in excluded
            and (allowed is None or e.task_id in allowed)
            and e.text
        ]
        entries.sort(key=lambda e: e.updated_at, reverse=True)
        budget = self.max_chars if max_chars is None else max_chars
        blocks: list[str] = []
        used = 0
        for entry in entries:
            header = f"[{entry.source or entry.task_id} v{entry.version}]"
            block = f"{header}\n{entry.text}"
            if used + len(block) > budget:
                remaining = budget - used
                if remaining > 200:
                    blocks.append(block[:remaining] + " …")
                break
            blocks.append(block)
            used += len(block)
        return "\n\n".join(blocks)

    # -- persistence --------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {k: v.to_dict() for k, v in self._entries.items()}

    def load_dict(self, data: dict[str, Any]) -> int:
        with self._lock:
            for key, payload in (data or {}).items():
                entry = ContextEntry.from_dict({**payload, "task_id": key})
                if not entry.expired():
                    self._entries[key] = entry
            return len(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
