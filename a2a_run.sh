#!/bin/sh
# A2A (Agent2Agent) demo run scripts.
#
# 0) main() 极简编排：REVIEWER 调用 IMPLEMENTER
#    python -m demo.main
#
# 1) A2A 服务端 + 客户端 demo
#    python -m demo.a2a_graph
#
# 2) 独立 A2A server（监听 0.0.0.0:9090，POST /webhook）
#    python -m demo.a2a_antigravity_server
