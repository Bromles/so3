#!/usr/bin/env python3
"""3-node lin-kv smoke test."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main([
        "--node-count", "3",
        "--time-limit", "10",
        "--rate", "10",
        "--concurrency", "2n",
        "--log-stderr",
    ]))
