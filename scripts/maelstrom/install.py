#!/usr/bin/env python3
"""Download and install Maelstrom under .tools/maelstrom/.

Cross-platform replacement for install-maelstrom.sh and install-maelstrom.ps1.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tarfile
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
INSTALL_ROOT = REPO_ROOT / ".tools" / "maelstrom"
DEFAULT_VERSION = "0.2.4"


def _progress(count: int, block_size: int, total_size: int) -> None:
    if total_size <= 0:
        return
    pct = min(100, count * block_size * 100 // total_size)
    print(f"\r  {pct}%", end="", flush=True)


def install(version: str) -> int:
    url = (
        f"https://github.com/jepsen-io/maelstrom/releases/download/"
        f"v{version}/maelstrom.tar.bz2"
    )
    archive_path = INSTALL_ROOT / "maelstrom.tar.bz2"
    extract_dir = INSTALL_ROOT / "maelstrom"
    jar_path = extract_dir / "lib" / "maelstrom.jar"

    INSTALL_ROOT.mkdir(parents=True, exist_ok=True)

    if extract_dir.exists():
        print(f"Removing existing {extract_dir}")
        shutil.rmtree(extract_dir)

    print(f"Downloading {url}")
    try:
        urllib.request.urlretrieve(url, archive_path, reporthook=_progress)
        print()
    except Exception as exc:
        print(f"error: download failed: {exc}", file=sys.stderr)
        return 1

    print(f"Extracting to {INSTALL_ROOT}")
    try:
        with tarfile.open(archive_path, "r:bz2") as tf:
            tf.extractall(INSTALL_ROOT, filter="data")
    except Exception as exc:
        print(f"error: extraction failed: {exc}", file=sys.stderr)
        return 1
    finally:
        archive_path.unlink(missing_ok=True)

    print(f"Installed Maelstrom under {extract_dir}")
    print(f"Jar path: {jar_path}")
    print("Set MAELSTROM_JAR to that path or pass --maelstrom-jar to run.py.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download and install Maelstrom under .tools/maelstrom/.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "version",
        nargs="?",
        default=DEFAULT_VERSION,
        help=f"Maelstrom release version (default: {DEFAULT_VERSION})",
    )
    return install(parser.parse_args().version)


if __name__ == "__main__":
    raise SystemExit(main())
