#!/usr/bin/env python3
"""E2E test: simulate the frontend "生成 idea 蛋" button + "一键运行" via WS.

Flow (mirrors demo/canvas/index.html):
  1. WS connect to ws://localhost:8000/ws
  2. send {type:'generate', goal:...}        (the ⚡ button)
  3. collect the 'eggs' event, grab egg ids
  4. send {type:'run', targets:[ids]}        (the ▶ 一键运行选中蛋 button)
  5. collect tokens/status/done until every egg reports done
  6. fetch /api/trace/{egg} and /api/egg/{egg}/files to verify deliverables
"""
import asyncio
import json
import sys

import aiohttp

WS_URL = "ws://localhost:8000/ws"
HTTP = "http://localhost:8000"
GOAL = sys.argv[1] if len(sys.argv) > 1 else "实现一个两数相加的Python脚本 add.py，并运行验证 2+3 输出 5"
TOTAL_TIMEOUT = 420.0  # planning + 3 eggs on a remote API

state = {"eggs": {}, "done": set(), "tokens": {}, "errors": []}


async def main():
    async with aiohttp.ClientSession() as sess:
        async with sess.ws_connect(WS_URL, timeout=30) as ws:
            print(f"[ws] connected, goal: {GOAL}\n", flush=True)

            async def send(obj):
                await ws.send_str(json.dumps(obj))
                print(f"[->] {obj}", flush=True)

            async def reader():
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    ev = json.loads(msg.data)
                    k = ev.get("type")
                    if k == "eggs":
                        for e in ev.get("eggs", []):
                            state["eggs"][e["id"]] = e
                        print(f"[<-] eggs: {[e['title'] for e in ev.get('eggs', [])]}", flush=True)
                    elif k == "token":
                        state["tokens"].setdefault(ev["eggId"], []).append(ev.get("text", ""))
                    elif k == "status" and ev.get("eggId") in state["eggs"]:
                        if ev.get("status") == "idle":
                            pass
                    elif k == "done" and ev.get("eggId") in state["eggs"]:
                        state["done"].add(ev["eggId"])
                        print(f"[<-] done: {ev['eggId']}", flush=True)
                    elif k == "system":
                        print(f"[<-] system({ev.get('eggId')}): {ev.get('text','')[:120]}", flush=True)
                    elif k == "error":
                        state["errors"].append(ev)
                        print(f"[<-] ERROR: {ev}", flush=True)
                    else:
                        print(f"[<-] {k}", flush=True)

            rt = asyncio.create_task(reader())
            try:
                await send({"type": "generate", "goal": GOAL})

                # wait until eggs exist
                deadline = asyncio.get_event_loop().time() + 120
                while not state["eggs"] and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(1)
                if not state["eggs"]:
                    print("FAIL: no eggs created within 120s")
                    return 1
                egg_ids = list(state["eggs"].keys())
                print(f"\n=== eggs created: {len(egg_ids)} ===", flush=True)
                for e in state["eggs"].values():
                    print(f"  - {e['title']} (deps: {e.get('deps')})", flush=True)

                await send({"type": "run", "targets": egg_ids})

                deadline = asyncio.get_event_loop().time() + TOTAL_TIMEOUT
                while state["done"] < set(egg_ids) and asyncio.get_event_loop().time() < deadline:
                    await asyncio.sleep(2)

                print(f"\n=== run finished: done={sorted(state['done'])} errors={state['errors']} ===", flush=True)
            finally:
                rt.cancel()

    # ---- verify deliverables ----
    print("\n=== verification ===", flush=True)
    ok = True
    async with aiohttp.ClientSession() as sess:
        for eid, egg in state["eggs"].items():
            tok = "".join(state["tokens"].get(eid, []))
            print(f"\n--- {egg['title']} ({eid}) ---", flush=True)
            print(f"status: {'done' if eid in state['done'] else 'MISSING'}", flush=True)
            print(f"streamed text ({len(tok)} chars): {tok[:300]}", flush=True)
            try:
                async with sess.get(f"{HTTP}/api/egg/{eid}/files") as r:
                    files = (await r.json()).get("files", [])
                print(f"files in worktree: {files}", flush=True)
                if files:
                    async with sess.get(f"{HTTP}/api/egg/{eid}/file",
                                        params={"path": files[0]}) as r2:
                        if r2.status == 200:
                            content = await r2.text()
                            print(f"  {files[0]} ({len(content)} chars): {content[:200]}", flush=True)
            except Exception as exc:
                print(f"  files probe failed: {exc}", flush=True)
            if eid not in state["done"]:
                ok = False
    print("\nRESULT:", "PASS" if ok else "FAIL", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
