"""Backend configuration for IdeaCanvas.

One place to pick where the LLM runs (mirrors antigravity-sdk examples/me/
local_models.py: the LiteRT model path is only needed if you explicitly choose
`litert`; the default is `openai`, a local OpenAI-compatible server, so no
`.litertlm` file is required):

  IDEA_BACKEND=openai  -> local OpenAI-compatible server (Ollama / LM Studio /
                         vLLM). DEFAULT. Needs a server at OPENAI_BASE_URL.
  IDEA_BACKEND=litert  -> on-device LiteRT-LM (.litertlm, run `litert-lm import`
                         first; only then is GEMMA4_MODEL_PATH used).
  IDEA_BACKEND=cloud   -> Google Antigravity cloud (GEMINI_API_KEY or Vertex env).

All modules call :func:`build_agent_config` instead of constructing a config
class directly, so switching backends is a single env var.
"""
import contextlib
import logging
import os
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent

# Each egg gets its own worktree directory under here.
WORKTREE_ROOT = WORKSPACE / "worktrees"
WORKTREE_ROOT.mkdir(parents=True, exist_ok=True)

# Each Canvas "session" (one idea project) is a self-contained dated directory
# holding canvas.json, agent_data/ (SDK conversation persistence) and the eggs'
# worktrees/. Listing this dir == the user's saved idea sessions.
SESSIONS_DIR = WORKSPACE / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ---- Backend selection ----
# Default to the local OpenAI-compatible server (no .litertlm file required).
BACKEND = os.environ.get("IDEA_BACKEND", "openai").lower()


@contextlib.contextmanager
def quiet_sdk_start():
    """Suppress the SDK's own ERROR traceback when an agent fails to start.

    The SDK logs a verbose stack trace (even during exception unwinding) when a
    connection/model is unavailable; we only want our one-line status instead,
    so disable all logging briefly. ``logging.disable`` blocks every logger
    regardless of the SDK's own logger/handler setup.
    """
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)

# cloud (Google Antigravity)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
USE_VERTEX = os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "").lower() in ("1", "true")
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
CLOUD_MODEL = os.environ.get("ANTIGRAVITY_MODEL", "")

# litert (on-device)
GEMMA4_MODEL_PATH = os.environ.get(
    "GEMMA4_MODEL_PATH",
    os.path.expanduser("~/.litert-lm/models/gemma4-26b/model.litertlm"),
)

# local OpenAI-compatible
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://localhost:9000/v1")


def _detect_openai_model(base_url, timeout=2.0):
    """Best-effort: use the first model a local OpenAI server actually serves.

    The default model name varies per deployment, so when OPENAI_MODEL is unset
    we ask the server's /v1/models endpoint instead of guessing.
    """
    import json
    import urllib.request
    try:
        url = base_url.rstrip("/") + "/models"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for m in data.get("data", []):
            name = m.get("id") or m.get("model") or m.get("name")
            if name:
                return name
    except Exception:
        return None
    return None


OPENAI_MODEL = (
    os.environ.get("OPENAI_MODEL")
    or _detect_openai_model(OPENAI_BASE_URL)
    or "gemma-4-26B-A4B-it"
)


def is_corrupt_conversation_error(err):
    """True when the error means the SDK's persisted conversation is unusable.

    The model server validates every assistant message in the request history:
    it must carry either ``content`` or ``tool_calls``. A model turn that
    produced neither (a reasoning-only / empty turn, or a tool call the harness
    could not map onto structured ``tool_calls``) gets persisted by the SDK, so
    EVERY later request in that conversation is rejected with HTTP 400
    ``"Assistant message must contain either 'content' or 'tool_calls'!"``.

    The only recovery is to drop the conversation and start a fresh one.
    """
    if err is None:
        return False
    text = str(err)
    low = text.lower()
    # Empty assistant message -> HTTP 400 (the model server rejects the whole
    # history once a turn persisted neither content nor tool_calls).
    if "must contain either" in text:
        return True
    if "invalid_request" in low:
        return True
    # A missing/stale persisted conversation: the SDK refuses to resume. Match
    # the combination rather than bare "not found" (which also fires for
    # missing files, missing models, etc. and would misclassify unrelated
    # errors as a corrupt conversation).
    if "cannot resume" in low:
        return True
    if "conversation" in low and "not found" in low:
        return True
    return False


def is_stream_stall_error(err):
    """True when the client-side stream watchdog aborted the model turn.

    The harness aborts a turn after ~120s of stream silence ("model stream
    stalled" / "模型流静默超过120s"). This is distinct from "the server is
    down": llama.cpp streams nothing during prefill, and cannot detect the
    client disconnect mid-prefill, so the aborted request may keep burning a
    slot. Callers should probe the server and wait for slot release instead
    of blindly retrying or rebuilding the harness.
    """
    if err is None:
        return False
    low = str(err).lower()
    return "stalled" in low or "静默" in low or "stream timeout" in low


def compaction_config():
    """Per-backend compaction policy (keeps small context windows healthy)."""
    from google.antigravity import types

    if BACKEND == "litert":
        return types.CompactionConfig(
            token_threshold=15360,
            checkpoint_interval_tokens=10240,
            max_context_tokens=40960,
        )
    if BACKEND == "openai":
        return types.CompactionConfig(
            token_threshold=35360,
            checkpoint_interval_tokens=30240,
            max_context_tokens=50960,
        )
    return None


def backend_options():
    """Backend-specific kwargs for demo.agent_runtime.build_agent_config."""
    return {
        "backend": BACKEND,
        "model": OPENAI_MODEL if BACKEND == "openai" else (CLOUD_MODEL or None),
        "base_url": OPENAI_BASE_URL if BACKEND == "openai" else None,
        "model_path": (os.path.abspath(GEMMA4_MODEL_PATH)
                       if BACKEND == "litert" else None),
        "vertex": USE_VERTEX if BACKEND == "cloud" else None,
        "project": PROJECT if BACKEND == "cloud" else None,
        "location": LOCATION if BACKEND == "cloud" else None,
        "api_key": GEMINI_API_KEY if BACKEND == "cloud" else None,
        "compaction_config": compaction_config(),
    }


def build_agent_config(
    system_instructions,
    *,
    tools=None,
    policies=None,
    capabilities=None,
    subagents=None,
    response_schema=None,
    conversation_id=None,
    workspaces=None,
    app_data_dir=None,
    save_dir=None,
    hook_runner=None,
):
    """Compatibility shim: delegate to demo.agent_runtime.build_agent_config."""
    from .. import agent_runtime

    return agent_runtime.build_agent_config(
        system_instructions,
        **backend_options(),
        tools=tools or (),
        policies=policies,
        capabilities=capabilities,
        subagents=subagents or (),
        response_schema=response_schema,
        conversation_id=conversation_id,
        workspaces=workspaces,
        app_data_dir=app_data_dir,
        save_dir=save_dir,
        hook_runner=hook_runner,
        enable_otel=False,
    )
