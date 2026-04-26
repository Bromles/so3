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

## Three-Node Smoke Run

```powershell
./scripts/maelstrom/smoke-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

```bash
./scripts/maelstrom/smoke-3-node-lin-kv.sh
```

This uses three Maelstrom nodes with a low request rate. Client requests sent to followers are
forwarded to the deterministic leader, and the leader drives the current Accord transport phases
through Maelstrom messages before replying to the client.

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

The three-node smoke scripts override these defaults with `NODE_COUNT=3`, `TIME_LIMIT=10`,
`RATE=10`, and `CONCURRENCY=2n`.

## Recent Verification

Validated on 2026-04-26 after the dependency handling fixes:

- `bash scripts/maelstrom/smoke-lin-kv.sh`
  - workload: `lin-kv`
  - nodes: `1`
  - time limit: `10`
  - rate: `20`
  - concurrency: `2n`
  - result: `:valid? true`
- `bash scripts/maelstrom/smoke-3-node-lin-kv.sh`
  - workload: `lin-kv`
  - nodes: `3`
  - time limit: `10`
  - rate: `10`
  - concurrency: `2n`
  - result: `:valid? true`
- `NODE_COUNT=3 TIME_LIMIT=30 RATE=50 CONCURRENCY=4n LOG_STDERR=1 bash scripts/maelstrom/run-lin-kv.sh`
  - workload: `lin-kv`
  - nodes: `3`
  - time limit: `30`
  - rate: `50`
  - concurrency: `4n`
  - result: `:valid? true`
  - stats: 1313 operations, 637 ok, 535 fail, 141 info; ok fraction 0.48514852

Maelstrom writes detailed histories under `store/lin-kv/`, which is intentionally gitignored.
Do not commit run-specific result paths; keep only reproducible commands, parameters, and verdicts
in this document.

## Notes

- The adapter stores state under `SO3_MAELSTROM_DATA_DIR` or `./var/so3-maelstrom` by default.
- The helper scripts create a fresh temporary `SO3_MAELSTROM_DATA_DIR` for each run unless you
  provide one explicitly.
- Each Maelstrom node gets its own subdirectory keyed by `node_id`.
- The current adapter persists each node independently and uses Maelstrom messages for forwarding
  and consensus transport. It is useful for protocol and semantics validation before the normal
  TCP/gRPC `so3` node path is used for Jepsen runs.
- On Windows, Maelstrom needs permission to create symbolic links for `store/current`. If that
  fails, run the script from an elevated shell or enable Windows Developer Mode first.
- On WSL, prefer building a Linux `so3-maelstrom` binary inside WSL. Running the Windows `.exe`
  from `/mnt/...` can hit SQLite locking issues.
