# Maelstrom

`so3-maelstrom` is a separate binary crate for Jepsen Maelstrom `lin-kv` runs. It reuses
`so3-core` object and consensus code, but replaces tonic peer transport with Maelstrom
stdin/stdout JSON messages.

The production `so3` binary does not include Maelstrom runtime code.

## Prerequisites

- Java on `PATH`.
- Maelstrom executable on `PATH`, `MAELSTROM_JAR`, or an explicit `-MaelstromJar` / `-MaelstromBin` script argument.

## Install

```bash
./scripts/maelstrom/install-maelstrom.sh
```

```powershell
./scripts/maelstrom/install-maelstrom.ps1
```

The installer downloads an official `jepsen-io/maelstrom` release into
`.tools/maelstrom/maelstrom`.

## Runs

Single-node smoke:

```bash
./scripts/maelstrom/smoke-lin-kv.sh
```

```powershell
./scripts/maelstrom/smoke-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

Three-node smoke:

```bash
./scripts/maelstrom/smoke-3-node-lin-kv.sh
```

```powershell
./scripts/maelstrom/smoke-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

General `lin-kv`:

```bash
./scripts/maelstrom/run-lin-kv.sh
```

```powershell
./scripts/maelstrom/run-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

Three-node partition run:

```bash
./scripts/maelstrom/fault-3-node-lin-kv.sh
```

```powershell
./scripts/maelstrom/fault-3-node-lin-kv.ps1 -MaelstromJar .\.tools\maelstrom\maelstrom\lib\maelstrom.jar
```

The fault wrapper defaults to:

| Setting            | Value       |
|--------------------|-------------|
| `NODE_COUNT`       | `3`         |
| `TIME_LIMIT`       | `30`        |
| `RATE`             | `20`        |
| `CONCURRENCY`      | `2n`        |
| `NEMESIS`          | `partition` |
| `NEMESIS_INTERVAL` | `5`         |

The general run scripts pass through optional Maelstrom settings such as `NEMESIS`,
`NEMESIS_INTERVAL`, `LATENCY`, `LATENCY_DIST`, `AVAILABILITY`, `CONSISTENCY_MODELS`,
`LOG_NET_SEND`, and `LOG_NET_RECV`.

## Runtime Model

Maelstrom starts each node as a separate process and sends the initial node list in the `init`
message. The adapter builds one isolated `so3-core` stack per Maelstrom node:

- SQLite metadata and consensus journal under `metadata/<node_id>`.
- Blob files under `blobs/<node_id>`.
- `AccordConsensusCoordinatorService` for commands coordinated by that node.
- `InboundConsensusUseCaseImpl` for incoming consensus RPC messages.
- Maelstrom peer clients that encode core protobuf requests into JSON payloads.

Client routing is intentionally different from production:

- `node_ids.first()` is treated as the deterministic leader.
- Client requests delivered to followers are forwarded to that leader.
- The leader coordinates the core operation and returns the response through the forwarding node.

Production `so3` does not have this leader-forwarding layer; any node can coordinate requests that
arrive through its S3-compatible API.

## Current Caveats

The adapter is useful for exercising core command semantics through Maelstrom histories, but it is
not yet production-parity:

- It hides concurrent production coordinators because all Maelstrom client commands are forwarded to one leader.
- `cas` with `create_if_not_exists=true` does a coordinated read followed by a write, so two concurrent creates can both
  return `cas_ok`.
- Blob push/fetch uses one JSON payload and does not validate size or SHA-256 the way production tonic `BlobService`
  does.
- Pending consensus, blob, and forward requests wait on oneshot responses without per-operation deadlines.

Use Maelstrom results as protocol smoke coverage, not as a complete proof of production-node
behavior.

## Latest Verification

Local runs from 2026-05-05 with `target/release/so3-maelstrom` passed Knossos (`:valid? true`) for:

| Scenario | Nodes | Rate | Concurrency | Nemesis | Ops | Ok | Fail | Info | Result |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `smoke-lin-kv` | 1 | 20 | `2n` | none | 197 | 144 | 53 | 0 | `:valid? true` |
| `smoke-3-node-lin-kv` | 3 | 10 | `2n` | none | 95 | 67 | 28 | 0 | `:valid? true` |
| `fault-3-node-lin-kv` | 3 | 20 | `2n` | `partition/5s` | 238 | 53 | 108 | 77 | `:valid? true` |

See [results.md](results.md) for the full counters and current interpretation caveats.

## Platform Notes

- Windows: use `*.ps1` under PowerShell 7.
- macOS/Linux: use `*.sh` under bash/zsh.
- WSL: prefer building a Linux `so3-maelstrom` binary inside WSL.
- Maelstrom writes detailed histories under `store/lin-kv/`, which is gitignored.
- Helper scripts create a fresh temporary `SO3_MAELSTROM_DATA_DIR` unless one is provided.
