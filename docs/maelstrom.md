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

## Fault Run

The stock Maelstrom binary currently exposes `partition` as the supported fault for this workload.
Use this three-node wrapper to run `lin-kv` while Maelstrom injects network partitions:

```powershell
./scripts/maelstrom/fault-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

```bash
./scripts/maelstrom/fault-3-node-lin-kv.sh
```

This defaults to `NODE_COUNT=3`, `TIME_LIMIT=30`, `RATE=20`, `CONCURRENCY=2n`,
`NEMESIS=partition`, and `NEMESIS_INTERVAL=5`.

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
- nemesis: unset
- nemesis interval: Maelstrom default unless `NEMESIS_INTERVAL` / `-NemesisInterval` is set

The three-node smoke scripts override these defaults with `NODE_COUNT=3`, `TIME_LIMIT=10`,
`RATE=10`, and `CONCURRENCY=2n`.

The general `run-lin-kv` scripts also pass through optional fault and checker parameters:
`NEMESIS`, `NEMESIS_INTERVAL`, `LATENCY`, `LATENCY_DIST`, `AVAILABILITY`,
`CONSISTENCY_MODELS`, `LOG_NET_SEND`, and `LOG_NET_RECV` for bash; `-Nemesis`,
`-NemesisInterval`, `-Latency`, `-LatencyDist`, `-Availability`, `-ConsistencyModels`,
`-LogNetSend`, and `-LogNetRecv` for PowerShell.

## Recent Verification

For full results including Knossos verdict details, k6 S3 benchmark numbers, and interpretation
see **[docs/results.md](results.md)**.

Validated on 2026-04-27 with release binary (`target/release/so3-maelstrom`):

| Scenario | Nodes | Rate | Concurrency | Nemesis | Result |
|----------|-------|------|-------------|---------|--------|
| smoke-lin-kv | 1 | 20 | 2n | — | `:valid? true` |
| smoke-3-node-lin-kv | 3 | 10 | 2n | — | `:valid? true` |
| fault-3-node-lin-kv (rate 20) | 3 | 20 | 2n | partition/5s | `:valid? true` |
| fault-3-node-lin-kv (rate 50) | 3 | 50 | 4n | partition/5s | `:valid? true` |

The RATE=50 CONCURRENCY=4n scenario previously caused a stack overflow in the old recursive
dispatch runtime. The refactored `Arc<SharedRuntime>` + oneshot channel design eliminates this.

Maelstrom writes detailed histories under `store/lin-kv/`, which is intentionally gitignored.
Do not commit run-specific result paths; keep only reproducible commands, parameters, and verdicts
in this document.

## Notes

- The adapter stores state under `SO3_MAELSTROM_DATA_DIR` or `./var/so3-maelstrom` by default.
- The helper scripts create a fresh temporary `SO3_MAELSTROM_DATA_DIR` for each run unless you
  provide one explicitly.
- Each Maelstrom node gets isolated durable storage keyed by `node_id`: metadata/journal under
  `metadata/<node_id>` and blobs under `blobs/<node_id>`.
- The current adapter persists each node independently and uses Maelstrom messages for forwarding
  plus consensus/blob transport. It is useful for protocol and semantics validation before the
  normal TCP/gRPC `so3` node path is used for Jepsen runs.
- On Windows, Maelstrom needs permission to create symbolic links for `store/current`. If that
  fails, run the script from an elevated shell or enable Windows Developer Mode first.
- On WSL, prefer building a Linux `so3-maelstrom` binary inside WSL. Running the Windows `.exe`
  from `/mnt/...` can hit SQLite locking issues.
