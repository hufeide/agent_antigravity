"""测试 a2a.py —— 对接 llama.cpp（端口 9000）的真实 LLM。

运行：
    python -m demo.a2a_test

前置条件：
    1. llama.cpp server 在 127.0.0.1:9000 提供 OpenAI 兼容 /v1 接口
       （例如：llama-server --port 9000 -m <model.gguf>）
    2. google.antigravity SDK 可用（本环境 SDK_AVAILABLE=True）

测试覆盖（全部经真实 LLM，非模拟执行器）：
    1. 端到端往返：A2AServer 用 make_sdk_executor 包装 agent_runtime.run_task，
       A2AClient 调用，验证真实 LLM 回答经 A2A 协议回传。
    2. A2A context_id 延续 + LLM 会话记忆：同一远程会话连续两轮，context_id
       保持一致，且第二轮回复真实含第一轮告知的数字（会话被续接而非每次新建）。
    3. Orchestrator 远程分派 + 复命：经 a2a_dispatch 把任务发给远程 A2A agent，
       结果写回 Task（report_payload）并发布 REPORT 消息到总线。
    4. 流式事件聚合：reply.state == COMPLETED、reply.artifacts 非空。
"""

from __future__ import annotations

import asyncio
import logging
import sys
import tempfile
from pathlib import Path

# Allow running both as `python -m demo.a2a_test` and as a plain script
# (`python demo/a2a_test.py`). A plain script has no parent package, so we
# establish the demo package context and make the repo root importable first.
# Crucially we also drop the script dir (demo/) from sys.path: it shadows the
# installed top-level `a2a` SDK package with demo/a2a.py (same basename),
# which then fails its own relative imports. Importing demo also runs
# demo/__init__.py (_prefer_installed_sdk), which makes the installed
# google.antigravity SDK win over this repo's source tree.
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

from . import messaging, models, orchestrator
from .a2a import (
    A2AClient,
    A2AServer,
    A2A_SDK_AVAILABLE,
    a2a_dispatch,
    make_sdk_executor,
)
from .agent_runtime import SDK_AVAILABLE, build_agent_config

logger = logging.getLogger(__name__)

LLAMA_BASE = "http://127.0.0.1:9000/v1"
API_KEY = "llama-cpp"
SYSTEM_PROMPT = "你是一个简洁的助手，请始终用中文简洁回答。"
# 真实 LLM 后端（antigravity 运行时 spawn + llama.cpp 推理）较慢，需长超时。
REQUEST_TIMEOUT = 120.0

# TaskState 枚举值（与 a2a.types.TaskState 一致）
_TASK_STATE_COMPLETED = 3


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


async def _build_config(model: str):
    """构建指向 llama.cpp 的真实 SDK 配置（OpenAI 兼容后端）。

    基础 config 不绑定 conversation_id；A2A 的 context_id 由 make_sdk_executor
    经 agent_runtime.with_conversation 逐调用映射为 SDK conversation_id，并配合
    CREATE_OR_RESUME + 固定 save_dir 让 LLM 真正续接同一会话（保持记忆）。
    """
    return build_agent_config(
        SYSTEM_PROMPT,
        backend="openai",
        base_url=LLAMA_BASE,
        model=model,
        api_key=API_KEY,
        enable_sandbox=False,
    )


class _Runner:
    """收集测试结果并打印汇总。"""

    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []

    def record(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append((name, ok, detail))
        if ok:
            logger.info("✓ %s %s", name, detail)
        else:
            logger.error("✗ %s — %s", name, detail)

    def summary(self) -> bool:
        passed = sum(1 for _, ok, _ in self.results if ok)
        failed = len(self.results) - passed
        logger.info("=" * 60)
        logger.info("测试汇总：%d 通过 / %d 失败 / 共 %d", passed, failed, len(self.results))
        for name, ok, detail in self.results:
            mark = "✓" if ok else "✗"
            logger.info("  %s %s %s", mark, name, detail if not ok else "")
        logger.info("=" * 60)
        return failed == 0


async def _test_round_trip(client: A2AClient, runner: _Runner) -> None:
    """测试 1+4：真实 LLM 端到端往返 + 流式事件聚合。"""
    prompt = "用一句话说明你是什么模型。"
    reply = await client.send(prompt, timeout=REQUEST_TIMEOUT)
    logger.info("LLM 回复: %r", reply.text)
    runner.record(
        "1.端到端往返(真实LLM)",
        bool(reply.text.strip()),
        reply.text.strip()[:60],
    )
    runner.record(
        "4.流式事件聚合(COMPLETED+artifact)",
        reply.state == _TASK_STATE_COMPLETED and len(reply.artifacts) > 0,
        f"state={reply.state} artifacts={reply.artifacts}",
    )


async def _test_context_continuation(client: A2AClient, runner: _Runner) -> None:
    """测试 2+2b：同一远程会话的 context_id 延续 + LLM 真实会话记忆。

    第一轮让 LLM 记住数字 42；第二轮带同一 context_id 追问。协议层验证
    context_id 保持一致，LLM 层验证第二轮回复真的含 "42"（会话被续接，
    而非每次新建会话导致失忆）。
    """
    first = await client.send("记住这个数字：42。", timeout=REQUEST_TIMEOUT)
    logger.info("第一轮 context_id: %s | 回复: %r", first.context_id, first.text.strip()[:50])
    assert first.context_id, "第一轮应返回 context_id"

    second = await client.send(
        "我刚才让你记住的数字是多少？只回答那个数字。",
        context_id=first.context_id,
        timeout=REQUEST_TIMEOUT,
    )
    logger.info("第二轮 context_id: %s | 回复: %r", second.context_id, second.text.strip()[:80])

    runner.record(
        "2.context_id 延续(协议层)",
        second.context_id == first.context_id and bool(second.text.strip()),
        f"first={first.context_id[:8]}.. second={second.context_id[:8]}..",
    )
    runner.record(
        "2b.LLM会话记忆(真实续接)",
        "42" in second.text,
        second.text.strip()[:60],
    )


async def _test_orchestrator_report(
    client: A2AClient, bus: messaging.MessageBus, runner: _Runner
) -> None:
    """测试 3：Orchestrator 经 a2a_dispatch 远程分派 + 复命。"""
    orch = orchestrator.Orchestrator(max_concurrency=2)
    task = models.Task(
        task_id="task-a2a-llm",
        title="请用一句话描述今天的天气。",
        description="该任务经 A2A 发给远程 agent，由 llama.cpp 真实执行",
        owner_agent_id="llama-cpp-agent",
    )
    orch.register(task)
    await orch.arun(
        lambda t: a2a_dispatch(client, t, sender="cmd", bus=bus, timeout=REQUEST_TIMEOUT)
    )

    ok = (
        task.status == models.TaskStatus.SUCCESS
        and task.report_submitted
        and bool(task.report_payload.get("text", "").strip())
    )
    runner.record(
        "3.Orchestrator远程分派+复命",
        ok,
        f"status={task.status.value} report={task.report_payload.get('text', '')[:40]!r}",
    )

    reports = [
        m
        for m in bus.history(task_id=task.task_id)
        if m.msg_type == messaging.MessageType.REPORT
    ]
    runner.record(
        "3b.REPORT复命消息入总线",
        len(reports) == 1 and reports[0].status == "success",
        f"reports={len(reports)}",
    )


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    if not A2A_SDK_AVAILABLE:
        logger.error("A2A SDK 不可用，跳过测试。")
        return 1
    if not SDK_AVAILABLE:
        logger.error("google.antigravity SDK 不可用，无法构建真实 agent，跳过测试。")
        return 1

    try:
        model = _discover_model()
        logger.info("llama.cpp 模型: %s", model)
    except Exception as exc:  # noqa: BLE001
        logger.error("无法连接 llama.cpp (%s)：%s", LLAMA_BASE, exc)
        logger.error("请先启动 llama-server --port 9000 -m <model.gguf>")
        return 1

    config = await _build_config(model)
    logger.info("SDK 配置: %s (backend=openai, base_url=%s)", type(config).__name__, LLAMA_BASE)

    # 持久化 SDK 会话：A2A context_id 映射的 LLM 对话须落盘，跨请求保留记忆。
    conversation_save_dir = tempfile.mkdtemp(prefix="a2a-test-conv-")
    logger.info("LLM 会话持久化目录: %s", conversation_save_dir)

    server = A2AServer(
        make_sdk_executor(
            config,
            agent_id="llama-cpp-agent",
            conversation_save_dir=conversation_save_dir,
        ),
        name="llama-cpp-a2a-agent",
        description="A2A agent backed by llama.cpp on port 9000",
    )
    url = server.start()
    runner = _Runner()
    bus = messaging.MessageBus()
    try:
        async with await A2AClient.connect(url) as client:
            logger.info("已连接 A2A agent %r @ %s", client.name, url)
            await _test_round_trip(client, runner)
            await _test_context_continuation(client, runner)
            await _test_orchestrator_report(client, bus, runner)
    finally:
        server.stop()

    ok = runner.summary()
    return 0 if ok else 1


if __name__ == "__main__":
    asyncio.run(main())#sys.exit(asyncio.run(main()))
