"""用 langgraph 组合两个远程 A2A agent。

两个 agent 各自是一个独立的 A2A 1.0 服务：
  - Friday（AgentScope，默认 http://127.0.0.1:9999）：通用助手。
  - Antigravity-Assistant（本仓库 a2a_antigravity_server，默认
    http://127.0.0.1:9998）：技术 / 编程助手（后端 llama.cpp 9000）。

图结构（路由分发）：
    START -> route -> { friday | antigravity } -> END

route 节点按关键词把 query 分派给合适的 agent；每个 agent 节点经 A2AClient
跨网络调用对应服务，并各自保留 context_id，从而支持多轮会话记忆。

运行前先启动两个服务：
    # 终端 1：Antigravity（本仓库）
    python -m demo.a2a_antigravity_server --port 9998
    # 终端 2：Friday（agentscope）
    python /home/administator/workspace/agentscope-demo/agentscope/examples/a2a/server.py --port 9999

然后运行本图：
    python -m demo.a2a_graph                       # 跑一组示例 query
    python -m demo.a2a_graph --query "帮我用 Python 写一个快排"
    python -m demo.a2a_graph --agent friday --query "你好"   # 强制指定 agent
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import TypedDict

# Allow running both as `python -m demo.a2a_graph` and as a plain script
# (`python demo/a2a_graph.py`). A plain script has no parent package, so we
# establish the demo package context and make the repo root importable first.
# We also drop the script dir (demo/) from sys.path: it shadows the installed
# top-level `a2a` SDK package with demo/a2a.py. Importing demo also runs
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

from langgraph.graph import END, START, StateGraph  # noqa: E402

from .a2a import A2AClient, A2AReply  # noqa: E402

logger = logging.getLogger(__name__)

FRIDAY_URL = "http://127.0.0.1:9999"
ANTIGRAVITY_URL = "http://127.0.0.1:9998"
# 真实 LLM 后端较慢，A2A 调用给足超时。
REQUEST_TIMEOUT = 120.0

# 技术 / 编程类关键词 -> 路由到 Antigravity-Assistant；否则给 Friday。
TECH_KEYWORDS = (
    "代码", "编程", "python", "java", "函数", "bug", "调试", "debug",
    "重构", "算法", "实现", "写一个", "写个", "排序", "单元测试", "框架",
    "部署", "服务", "协议", "a2a", "llm", "模型", "api", "sql", "数据库",
    "code", "implement", "compile", "git",
)


class GraphState(TypedDict, total=False):
    """LangGraph 共享状态。每个节点返回一个局部更新，由图合并。"""

    query: str          # 用户输入
    route: str          # 路由结果："friday" | "antigravity"
    response: str       # 被选中 agent 的回复
    friday_context: str  # Friday 会话 context_id（多轮记忆）
    antigravity_context: str  # Antigravity 会话 context_id


def _route(state: GraphState) -> dict:
    """按关键词把 query 分派给 friday 或 antigravity。"""
    query = (state.get("query") or "").lower()
    route = "antigravity" if any(k in query for k in TECH_KEYWORDS) else "friday"
    return {"route": route}


def build_graph(friday: A2AClient, antigravity: A2AClient):
    """构建并编译路由分发图。两个 client 经闭包注入各 agent 节点。"""

    async def friday_node(state: GraphState) -> dict:
        reply: A2AReply = await friday.send(
            state["query"],
            context_id=state.get("friday_context") or None,
            timeout=REQUEST_TIMEOUT,
        )
        return {"response": reply.text, "friday_context": reply.context_id}

    async def antigravity_node(state: GraphState) -> dict:
        reply: A2AReply = await antigravity.send(
            state["query"],
            context_id=state.get("antigravity_context") or None,
            timeout=REQUEST_TIMEOUT,
        )
        return {"response": reply.text, "antigravity_context": reply.context_id}

    graph = StateGraph(GraphState)
    graph.add_node("route", _route)
    graph.add_node("friday", friday_node)
    graph.add_node("antigravity", antigravity_node)

    graph.add_edge(START, "route")
    graph.add_conditional_edges(
        "route",
        lambda s: s["route"],
        {"friday": "friday", "antigravity": "antigravity"},
    )
    graph.add_edge("friday", END)
    graph.add_edge("antigravity", END)
    return graph.compile()


async def _connect(url: str, name: str) -> A2AClient | None:
    """连接一个 A2A 服务；失败返回 None 并记录警告。"""
    try:
        client = await A2AClient.connect(url)
        logger.info("已连接 %s: %r @ %s", name, client.name, url)
        return client
    except Exception as exc:  # noqa: BLE001
        logger.error("无法连接 %s (%s)：%s", name, url, exc)
        return None


async def run_query(
    app, query: str, *, friday: A2AClient | None, antigravity: A2AClient | None,
    force: str | None = None,
) -> GraphState:
    """跑一次图，打印路由与回复。force 可强制指定 agent。"""
    # 若强制指定且对应 client 不可用，则回退到可用的那个。
    if force == "friday" and friday is None and antigravity is not None:
        logger.warning("friday 不可用，忽略 --agent friday，回退到 antigravity。")
        force = "antigravity"
    if force == "antigravity" and antigravity is None and friday is not None:
        logger.warning("antigravity 不可用，忽略 --agent antigravity，回退到 friday。")
        force = "friday"

    if force:
        # 绕过路由：直接构造一条只走目标 agent 的状态。
        route = force
        state: GraphState = {"query": query, "route": route}
        if route == "friday":
            reply = await friday.send(query, timeout=REQUEST_TIMEOUT)
            state.update({"response": reply.text, "friday_context": reply.context_id})
        else:
            reply = await antigravity.send(query, timeout=REQUEST_TIMEOUT)
            state.update({"response": reply.text, "antigravity_context": reply.context_id})
    else:
        state = await app.ainvoke({"query": query})

    logger.info("─" * 60)
    logger.info("query : %s", query)
    logger.info("route : %s", state.get("route"))
    logger.info("回复  : %s", (state.get("response") or "").strip()[:120])
    return state


async def main() -> int:
    parser = argparse.ArgumentParser(description="LangGraph composition of two A2A agents")
    parser.add_argument("--friday-url", default=FRIDAY_URL)
    parser.add_argument("--antigravity-url", default=ANTIGRAVITY_URL)
    parser.add_argument("--query", default=None, help="只跑一条 query")
    parser.add_argument("--agent", choices=["friday", "antigravity"], default=None,
                        help="强制指定 agent，绕过路由")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    friday = await _connect(args.friday_url, "Friday")
    antigravity = await _connect(args.antigravity_url, "Antigravity-Assistant")

    if friday is None and antigravity is None:
        logger.error("两个 A2A 服务都不可达，无法运行图。")
        return 1

    if friday is not None and antigravity is not None:
        app = build_graph(friday, antigravity)
    else:
        # 只有一个服务在线：构造一个退化图，全部路由到它。
        only = friday or antigravity
        assert only is not None
        only_name = "friday" if friday is not None else "antigravity"

        async def _only_node(state: GraphState) -> dict:
            reply = await only.send(
                state["query"],
                context_id=state.get(f"{only_name}_context") or None,
                timeout=REQUEST_TIMEOUT,
            )
            return {"response": reply.text, f"{only_name}_context": reply.context_id}

        g = StateGraph(GraphState)
        g.add_node(only_name, _only_node)
        g.add_edge(START, only_name)
        g.add_edge(only_name, END)
        app = g.compile()
        logger.warning("仅 %s 在线，图退化为单 agent。", only_name)

    try:
        if args.query is not None:
            await run_query(app, args.query, friday=friday, antigravity=antigravity,
                            force=args.agent)
        else:
            examples = [
                "你好，介绍一下你自己。",
                "帮我用 Python 写一个快速排序算法。",
                "今天天气怎么样？",
                "解释一下 A2A 协议是怎么工作的。",
            ]
            for q in examples:
                await run_query(app, q, friday=friday, antigravity=antigravity)
    finally:
        for c in (friday, antigravity):
            if c is not None:
                await c.aclose()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
