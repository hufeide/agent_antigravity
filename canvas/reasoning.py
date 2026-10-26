"""Reasoning (💭) capture built on the SDK's standard streaming API.

The Antigravity SDK already exposes the model's thinking as first-class
stream data:

  * ChatResponse.thoughts -> async iterator of reasoning deltas
  * types.Thought         -> the same deltas inside the unified chunks stream

The canvas therefore does not re-ask the model over raw HTTP any more. The egg
records Thought chunks from the standard stream; this module keeps the
enable/disable switch used by the UI plus a small helper for callers that
prefer the SDK's thoughts view.
"""
import logging

logger = logging.getLogger(__name__)

try:  # guarded: the compiled runtime binary is optional at import time
    from google.antigravity import types as _sdk_types
except Exception:  # pragma: no cover - environment dependent
    _sdk_types = None

_capture_enabled = True


def set_reasoning_capture(on: bool) -> None:
    global _capture_enabled
    _capture_enabled = bool(on)


def reasoning_enabled() -> bool:
    return _capture_enabled


def reasoning_capture_state() -> dict:
    return {"enabled": _capture_enabled}


def thought_text(chunk):
    """Return the text of an SDK Thought chunk, or None for any other chunk."""
    if _sdk_types is not None:
        if isinstance(chunk, _sdk_types.Thought):
            return chunk.text
        return None
    # No SDK: fall back to duck typing so the canvas still imports cleanly.
    if type(chunk).__name__ == "Thought":
        return getattr(chunk, "text", None)
    return None


async def capture_reasoning(response):
    """Yield reasoning deltas from a standard SDK ChatResponse.

    response.thoughts is the SDK's documented reasoning stream (see
    examples/getting_started/streaming.py). This is a no-op when capture is
    disabled or when the response exposes no thoughts.
    """
    if not _capture_enabled or response is None:
        return
    try:
        async for thought in response.thoughts:
            if thought:
                yield thought
    except Exception as ex:
        logger.debug("reasoning capture skipped: %s", ex)
