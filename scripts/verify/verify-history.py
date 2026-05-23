#!/usr/bin/env python3
"""CLI wrapper for SO3 history verification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from verify_history import verify_history_file  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Sequence


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify SO3 client-history.jsonl invariants."
    )
    parser.add_argument("history", type=Path)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    result = verify_history_file(args.history)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return 0 if result.get("verdict") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
