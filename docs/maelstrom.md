# Maelstrom

`so3-maelstrom` is a separate binary crate used only for Jepsen Maelstrom runs.
The normal `so3` node binary remains production-facing and does not include Maelstrom-specific
runtime code.

## Prerequisites

- Java available on `PATH`
- Jepsen Maelstrom release available either as:
  - `maelstrom` executable on `PATH`, or
  - `MAELSTROM_JAR`, or
  - explicit `-MaelstromJar` / `-MaelstromBin` script argument

Java is already present in the current environment.

## Install

To download an official Maelstrom release into the repo-local tools directory:

```powershell
./scripts/maelstrom/install-maelstrom.ps1
```

```bash
./scripts/maelstrom/install-maelstrom.sh
```

This downloads the release tarball from `jepsen-io/maelstrom` GitHub releases and extracts it to
`.tools/maelstrom/maelstrom`.

## Smoke Run

```powershell
./scripts/maelstrom/smoke-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

```bash
./scripts/maelstrom/smoke-lin-kv.sh
```

This uses a single node and low request volume to validate the adapter wiring.

## Lin-KV Run

```powershell
./scripts/maelstrom/run-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

```bash
./scripts/maelstrom/run-lin-kv.sh
```

## Platform Notes

- Windows: use `*.ps1` under PowerShell 7.
- macOS/Linux: use `*.sh` under bash/zsh.
- WSL: use the same `*.sh` scripts.

Both script families:

- use `maelstrom` from `PATH` if available;
- otherwise use `MAELSTROM_JAR`;
- otherwise fall back to `.tools/maelstrom/maelstrom/lib/maelstrom.jar`.

Default settings:

- workload: `lin-kv`
- nodes: `1`
- time limit: `20`
- rate: `100`
- concurrency: `2n`

## Notes

- The adapter stores state under `SO3_MAELSTROM_DATA_DIR` or `./var/so3-maelstrom` by default.
- Each Maelstrom node gets its own subdirectory keyed by `node_id`.
- The current adapter is single-process local storage per node. It is useful for early protocol and
  semantics validation before real replication is wired into the Maelstrom path.
- On Windows, Maelstrom needs permission to create symbolic links for `store/current`. If that
  fails, run the script from an elevated shell or enable Windows Developer Mode first.
- On WSL, prefer building a Linux `so3-maelstrom` binary inside WSL. Running the Windows `.exe`
  from `/mnt/...` can hit SQLite locking issues.
