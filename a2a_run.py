# 终端 1：Antigravity 服务（本仓库）
python -m demo.a2a_antigravity_server --port 9998
# 终端 2：Friday 服务（agentscope，已在 9999 运行）
python .../agentscope/examples/a2a/server.py --port 9999

# 运行图（跑一组示例 / 单条 / 强制指定）
python -m demo.a2a_graph
python -m demo.a2a_graph --query "帮我用 Python 写一个快速排序"
python -m demo.a2a_graph --agent friday --query "你好"
