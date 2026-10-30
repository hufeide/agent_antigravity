#!/usr/bin/env python3
"""Local OpenAI-compatible proxy for the iFlytek Spark MaaS API.

Why this exists
---------------
The SDK's ``LocalOpenAIAgentConfig`` (proto ``GemmaEndpoint``) accepts only a
``base_url`` — it has NO api_key field and the harness does not read an
authentication env var for the OpenAI backend. The Spark MaaS API at
``https://maas-api.cn-huabei-1.xf-yun.com/v2/chat/completions`` requires
``Authorization: Bearer <APIKey>``. This tiny proxy sits between the SDK
harness and the upstream: it injects the Bearer header, pins the model name
and relays streaming SSE responses verbatim.

The demo (demo/canvas/config.py) then needs NO code changes:

  IDEA_BACKEND=openai
  OPENAI_BASE_URL=http://localhost:9000/v1
  OPENAI_MODEL=spark-x2.6

Endpoints
---------
  POST /v1/chat/completions  -> forward to MAAS_UPSTREAM with auth, relay stream
  GET  /v1/models            -> list the configured model (SDK auto-detect probe)
  GET  /health               -> liveness

Environment
-----------
  MAAS_UPSTREAM      full chat completions URL (default: the Spark MaaS v2 URL)
  MAAS_API_KEY       Bearer token (required)
  MAAS_MODEL         model name pinned into every request (default spark-x2.6)
  MAAS_PROXY_HOST    default 127.0.0.1
  MAAS_PROXY_PORT    default 9000
"""
import asyncio
import json
import os
from pathlib import Path

from aiohttp import web

try:
    from dotenv import load_dotenv

    # demo/.env holds MAAS_API_KEY etc. Real env vars win (no override).
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

UPSTREAM = os.environ.get(
    "MAAS_UPSTREAM",
    "https://maas-api.cn-huabei-1.xf-yun.com/v2/chat/completions",
).rstrip()
API_KEY = os.environ.get("MAAS_API_KEY", "")
MODEL = os.environ.get("MAAS_MODEL", "spark-x2.6")
HOST = os.environ.get("MAAS_PROXY_HOST", "127.0.0.1")
PORT = int(os.environ.get("MAAS_PROXY_PORT", "9000"))

# Fields some OpenAI-compatible gateways reject; dropped from the forwarded
# body only if the upstream answers 400 (unknown-field errors), not upfront.
_STRIP_ON_400 = ("stream_options",)

# In-flight request tracking, exposed via /slots so the demo's server-aware
# stall watchdog (egg.py) can tell "long generation in progress" from
# "dead stream" / "server down" when talking to this proxy.
_INFLIGHT = 0
_PROGRESS = 0


def _slots_snapshot() -> dict:
    return {
        "id": 0,
        "n_ctx": 0,
        "is_processing": _INFLIGHT > 0,
        "n_prompt_tokens": 0,
        "n_prompt_tokens_processed": _PROGRESS,
        "n_tokens": 0,
    }


def _auth_headers(stream: bool) -> dict:
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if stream else "application/json",
    }


async def handle_chat(request: web.Request) -> web.StreamResponse:
    import aiohttp

    raw = await request.read()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return web.json_response(
            {"error": {"message": "invalid JSON body"}}, status=400)

    payload["model"] = MODEL
    stream = bool(payload.get("stream"))

    # The ClientSession must stay open until the streamed response has been
    # fully relayed — closing it early (e.g. returning from a helper that
    # owns the session) truncates the upstream stream after the first chunk.
    timeout = aiohttp.ClientTimeout(total=None, connect=10.0,
                                    sock_connect=10.0)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async def _send():
            return await sess.post(
                UPSTREAM,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers=_auth_headers(stream),
            )

        try:
            up = await _send()
        except Exception as exc:  # noqa: BLE001
            return web.json_response(
                {"error": {"message": f"upstream unreachable: {exc}"}},
                status=502)

        # Upstream rejected the payload: try stripping gateway-unfriendly
        # fields once before giving up.
        if up.status == 400:
            text = await up.text()
            await up.release()
            if any(k in payload for k in _STRIP_ON_400):
                for k in _STRIP_ON_400:
                    payload.pop(k, None)
                try:
                    up = await _send()
                except Exception:  # noqa: BLE001
                    return web.json_response(
                        {"error": {"message": "upstream unreachable"}},
                        status=502)
            else:
                return web.Response(
                    status=400, text=text,
                    content_type="application/json")

        status = up.status
        if status >= 400:
            text = await up.text()
            await up.release()
            return web.Response(status=status, text=text,
                                content_type="application/json")

        if stream:
            global _INFLIGHT, _PROGRESS
            resp = web.StreamResponse(
                status=status,
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            )
            await resp.prepare(request)
            _INFLIGHT += 1
            _PROGRESS += 1
            # Heartbeat: keep /slots progress advancing while a request is in
            # flight, so the watchdog treats long silent gaps during upstream
            # generation as slow_prefill (extend) rather than zombie (abort).
            hb = asyncio.ensure_future(_heartbeat())
            try:
                async for chunk in up.content.iter_any():
                    _PROGRESS += 1
                    try:
                        await resp.write(chunk)
                    except Exception:
                        # Client disconnected mid-stream (watchdog abort /
                        # user halt): stop relaying gracefully instead of
                        # logging a traceback.
                        break
            finally:
                hb.cancel()
                _INFLIGHT -= 1
            try:
                await up.release()
            except Exception:  # noqa: BLE001
                pass
            try:
                await resp.write_eof()
            except Exception:  # noqa: BLE001
                pass
            return resp

        data = await up.read()
        await up.release()
        return web.Response(status=status, body=data,
                            content_type="application/json")


async def _heartbeat():
    """Advance the /slots progress counter every 5s while a stream is active."""
    while _INFLIGHT > 0:
        await asyncio.sleep(5.0)
        global _PROGRESS
        _PROGRESS += 1


async def handle_slots(request: web.Request) -> web.Response:
    return web.json_response([_slots_snapshot()])


async def handle_models(request: web.Request) -> web.Response:
    return web.json_response({
        "object": "list",
        "data": [{
            "id": MODEL,
            "object": "model",
            "created": 0,
            "owned_by": "maas",
            "capabilities": ["chat-completions"],
        }],
    })


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "upstream": UPSTREAM,
                              "model": MODEL})


def main() -> None:
    if not API_KEY:
        raise SystemExit("MAAS_API_KEY is required")
    app = web.Application()
    app.router.add_post("/v1/chat/completions", handle_chat)
    app.router.add_get("/slots", handle_slots)
    app.router.add_get("/v1/models", handle_models)
    app.router.add_get("/health", handle_health)
    print(f"[maas-proxy] {HOST}:{PORT} -> {UPSTREAM} (model={MODEL})", flush=True)
    web.run_app(app, host=HOST, port=PORT, print=None)


if __name__ == "__main__":
    main()
