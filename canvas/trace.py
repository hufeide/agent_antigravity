"""Global chronological trace of EVERY model call on the canvas.

One GlobalTrace instance per Canvas: eggs, the commander (总指挥) and the
planner all append their model-call dialogue here, so the frontend can show
the whole system's LLM traffic in one time-ordered stream. The per-egg 轨迹
tabs keep their own transcripts; this is the merged view.

Entries are capped in memory (last ``max_entries``) and appended to a JSONL
file under the session directory, so a reloaded session can rebuild the view
(``load()``) and a fresh session starts clean (``set_path`` + ``load``).
"""
import datetime
import json
from pathlib import Path

MAX_TEXT = 8000     # cap a single text/reply payload in the trace
MAX_PAYLOAD = 4000  # cap serialized tool args / results
MAX_ENTRIES = 2000  # in-memory ring size


def _cap(v, n):
    if isinstance(v, str) and len(v) > n:
        return v[:n] + f"…[截断 {len(v) - n} 字符]"
    return v


def _cap_payload(v):
    """Serialize tool args / results to a capped string (trace only)."""
    if v is None:
        return None
    if isinstance(v, str):
        return _cap(v, MAX_PAYLOAD)
    try:
        return _cap(json.dumps(v, ensure_ascii=False, default=str), MAX_PAYLOAD)
    except Exception:
        return _cap(repr(v), MAX_PAYLOAD)


class GlobalTrace:
    def __init__(self, path=None):
        self._path = Path(path) if path else None
        self._entries = []
        self._seq = 0

    def set_path(self, path):
        self._path = Path(path) if path else None

    def load(self):
        """(Re)build the in-memory trace from the session's JSONL file."""
        self._entries = []
        self._seq = 0
        p = self._path
        if not p or not p.exists():
            return
        try:
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                e = json.loads(line)
                self._entries.append(e)
                self._seq = max(self._seq, int(e.get("id", 0)))
            if len(self._entries) > MAX_ENTRIES:
                self._entries = self._entries[-MAX_ENTRIES:]
        except Exception:
            pass

    def append(self, agent, agent_title, role, kind, text=None, name=None,
               args=None, result=None, error=None, **meta):
        """Append one trace entry; returns the entry dict."""
        self._seq += 1
        e = {
            "id": self._seq,
            "ts": datetime.datetime.now().isoformat(timespec="milliseconds"),
            "agent": agent,
            "agentTitle": agent_title or agent,
            "role": role,
            "kind": kind,
        }
        if text is not None:
            e["text"] = _cap(text, MAX_TEXT)
        if name is not None:
            e["name"] = name
        if args is not None:
            e["args"] = _cap_payload(args)
        if result is not None:
            e["result"] = _cap_payload(result)
        if error is not None:
            e["error"] = _cap(str(error), MAX_PAYLOAD)
        for k, v in meta.items():
            e[k] = _cap(v, MAX_TEXT) if isinstance(v, str) else v
        self._entries.append(e)
        if len(self._entries) > MAX_ENTRIES:
            del self._entries[:len(self._entries) - MAX_ENTRIES]
        self._persist(e)
        return e

    def merge_last(self, agent, kind, text):
        """Stream a delta into the most recent entry of agent+kind.

        Used for per-token text/thought accumulation, so a long reply stays
        ONE entry instead of one per token. Falls back to append when no
        matching entry exists (the first token of a new block).
        """
        for e in reversed(self._entries):
            if (e.get("agent") == agent and e.get("kind") == kind
                    and e.get("text") is not None):
                e["text"] = (e.get("text") or "") + text
                return
        self.append(agent, None, "assistant", kind, text=text)

    def since(self, last_id):
        return [e for e in self._entries if e["id"] > last_id]

    def all(self):
        return list(self._entries)

    def last_id(self):
        return self._seq

    def clear(self):
        self._entries.clear()
        self._seq = 0
        p = self._path
        if p and p.exists():
            try:
                p.unlink()
            except OSError:
                pass

    def _persist(self, e):
        p = self._path
        if not p:
            return
        try:
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        except OSError:
            pass
