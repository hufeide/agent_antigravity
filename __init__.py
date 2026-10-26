"""Demo framework implementing the multi-agent design in link.md.

Real SDK primitives are used where available (agent_runtime.py); everything the
SDK lacks (task graph, orchestrator dispatch, artifact acceptance, reporting
protocol, completion convergence) is a placeholder in the sibling modules.
See link.md section 六 for the full feature-by-feature mapping.
"""

from __future__ import annotations

import os
import sys


def _has_generated_proto(root: str) -> bool:
    """True if *root* holds the generated localharness_pb2 module."""
    return os.path.exists(
        os.path.join(root, "google", "antigravity", "proto", "localharness_pb2.py")
    )


def _prefer_installed_sdk() -> None:
    """Let the pip-installed google.antigravity win over this repo's source tree.

    The repo ships google/antigravity/*.py but not the generated *_pb2 modules
    (they are produced at install time), so whenever the repo root comes first
    on sys.path -- `python demo/canvas/run.py`, `python -m demo.main`, or any
    run whose CWD is the repo root -- `import google.antigravity` resolves to
    the source tree and fails with
    "cannot import name 'localharness_pb2' from 'google.antigravity.proto'".
    Moving the repo root behind the other entries fixes the shadowing; demo.*
    keeps importing because this package is already resolved.
    """
    if "google.antigravity" in sys.modules:
        return
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _has_generated_proto(repo_root):
        return  # source tree has the generated modules; nothing to prefer

    shadowing: list[str] = []
    rest: list[str] = []
    for entry in sys.path:
        resolved = os.path.abspath(entry or os.getcwd())
        (shadowing if resolved == repo_root else rest).append(entry)
    if not shadowing:
        return
    if not any(_has_generated_proto(os.path.abspath(e or os.getcwd())) for e in rest):
        return  # no installed SDK to fall back to; leave sys.path alone
    sys.path[:] = rest + shadowing


_prefer_installed_sdk()
