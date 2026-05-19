#!/usr/bin/env python3
"""3-node lin-kv test with partition nemesis."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main([
        "--node-count", "3",
        "--time-limit", "30",
        "--rate", "20",
        "--concurrency", "2n",
        "--nemesis", "partition",
        "--nemesis-interval", "5",
        "--log-stderr",
    ]))
