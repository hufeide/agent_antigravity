"""独立部署的 Antigravity A2A 服务。

把 agent_runtime.run_task（经 llama.cpp 9000 的真实 LLM）经 make_sdk_executor
包装，用 A2AServer 暴露为一个 A2A 1.0 agent，供 a2a_graph.py 等客户端跨网络
调用。与 a2a_test.py 的区别在于：这是一个常驻服务，而非一次性测试。

运行：
    python -m demo.a2a_antigravity_server [--port 9998]

前置条件：
    1. llama.cpp server 在 127.0.0.1:9000 提供 OpenAI 兼容 /v1 接口
       （例如：llama-server --port 9000 -m <model.gguf>）
    2. google.antigravity SDK 可用（本环境 SDK_AVAILABLE=True）

card：name="Antigravity-Assistant"，JSONRPC 于 /，协议 1.0，流式。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import tempfile
from pathlib import Path

# Allow running both as `python -m demo.a2a_antigravity_server` and as a plain
# script (`python demo/a2a_antigravity_server.py`). A plain script has no parent
# package, so we establish the demo package context and make the repo root
# importable first. We also drop the script dir (demo/) from sys.path: it
# shadows the installed top-level `a2a` SDK package with demo/a2a.py. Importing
# demo also runs demo/__init__.py (_prefer_installed_sdk), which makes the
# installed google.antigravity SDK win over this repo's source tree.
if __package__ in (None, ""):
    _here = Path(__file__).resolve().parent        # demo/
    _repo_root = _here.parent                        # repo root
    sys.path[:] = [
        p for p in sys.path
        if (Path(p) if p else Path.cwd()).resolve() != _here
    ]
    if str(_repo_root) not in sys.path:
        sys.path.insert(0, str(_repo_root))
    __package__ = "demo"

import httpx

from .a2a import A2AServer, A2A_SDK_AVAILABLE, make_sdk_executor
from .agent_runtime import SDK_AVAILABLE, build_agent_config

logger = logging.getLogger(__name__)

LLAMA_BASE = "http://127.0.0.1:9000/v1"
API_KEY = "llama-cpp"
SYSTEM_PROMPT = "你是一个简洁的技术助手，请始终用中文简洁回答。"


def _discover_model() -> str:
    """从 llama.cpp /v1/models 自动发现模型名。"""
    with httpx.Client(timeout=5.0) as http:
        resp = http.get(LLAMA_BASE + "/models")
        resp.raise_for_status()
        payload = resp.json()
    entries = payload.get("data") or payload.get("models") or []
    if not entries:
        raise RuntimeError("llama.cpp 未在 /v1/models 暴露任何模型")
    return entries[0].get("id") or entries[0].get("name")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Antigravity A2A 1.0 server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9998)
    parser.add_argument("--model", default=None, help="覆盖自动发现的模型名")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if not A2A_SDK_AVAILABLE:
        logger.error("A2A SDK 不可用，无法启动服务。")
        return 1
    if not SDK_AVAILABLE:
        logger.error("google.antigravity SDK 不可用，无法构建真实 agent。")
        return 1

    try:
        model = args.model or _discover_model()
        logger.info("llama.cpp 模型: %s", model)
    except Exception as exc:  # noqa: BLE001
        logger.error("无法连接 llama.cpp (%s)：%s", LLAMA_BASE, exc)
        logger.error("请先启动 llama-server --port 9000 -m <model.gguf>")
        return 1

    config = build_agent_config(
        SYSTEM_PROMPT,
        backend="openai",
        base_url=LLAMA_BASE,
        model=model,
        api_key=API_KEY,
        enable_sandbox=False,
    )
    # 持久化 SDK 会话：A2A context_id 映射的 LLM 对话须落盘，跨请求保留记忆。
    conversation_save_dir = tempfile.mkdtemp(prefix="a2a-antigravity-conv-")
    logger.info("LLM 会话持久化目录: %s", conversation_save_dir)

    server = A2AServer(
        make_sdk_executor(
            config,
            agent_id="antigravity-agent",
            conversation_save_dir=conversation_save_dir,
        ),
        name="Antigravity-Assistant",
        description="Antigravity(llama.cpp) technical assistant exposed over A2A 1.0.",
        host=args.host,
        port=args.port,
    )
    url = server.start()
    logger.info(
        "Antigravity A2A 服务已启动: %s (card=%s)",
        url, "/.well-known/agent-card.json",
    )
    logger.info("按 Ctrl+C 退出。")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("收到退出信号，停止 Antigravity A2A 服务。")
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
