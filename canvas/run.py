"""Entrypoint: run the IdeaCanvas backend.

    python -m demo.canvas.run        # http://localhost:8000  (from repo root)
    python run.py                    # also works from this directory
    GEMINI_API_KEY=... python run.py

Set a model backend (see canvas/config.py: IDEA_BACKEND=openai|litert|cloud) to
use a real model. Without a reachable model the backend runs in offline mode so
the prototype still serves the UI and reports eggs as not connected.
"""
import os
import sys

# Make the repo root importable so `demo.canvas` resolves as a package whether
# this file is run directly (`python run.py`) or as a module
# (`python -m demo.canvas.run`).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import uvicorn

from demo.canvas.server import app


def main():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
