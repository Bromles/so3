# SO3

SO3 is a Rust prototype of a replicated, S3-like object store. Each node exposes an
S3-compatible API and a private tonic RPC API. Object operations are coordinated through
the current Accord-style consensus implementation and persisted to SQLite plus the local
filesystem.

The code is split into three workspace crates:

| Crate           | Purpose                                                                               |
|-----------------|---------------------------------------------------------------------------------------|
| `so3-core`      | Domain types, repositories, S3 API, RPC API, object use cases, and consensus services |
| `so3`           | Production-facing node binary                                                         |
| `so3-maelstrom` | Jepsen Maelstrom stdin/stdout adapter for `lin-kv` tests                              |

## Current Shape

- S3-compatible API: Axum route `/{bucket}/{*key}` with `GET`, `HEAD`, `PUT`, and `DELETE`.
- Private RPC API: tonic services for consensus (`PreAccept`, `Accept`, `Commit`, `Apply`, `Recover`) and blob transfer.
- Storage: SQLite metadata and consensus journal under `metadata_dir`; immutable blob files under `blob_dir`.
- Replication model: every node stores a full copy of data; writes do not shard by key.
- Node identity: `node_id` is optional. If omitted, the node generates and persists an identity under the metadata
  directory.

See [docs/architecture.md](docs/architecture.md) for diagrams, request flows, and known limitations.

## Configuration

`so3` builds configuration from defaults, an optional TOML file, and environment overrides:

1. Defaults are used when no value is configured.
2. If `SO3_CONFIG` is set, that TOML file is loaded.
3. Otherwise `./so3.toml` is loaded when present.
4. Environment variables override TOML values.

Example:

```toml
node_id = "123e4567-e89b-12d3-a456-426614174000"
object_api_addr = "127.0.0.1:3000"
rpc_api_addr = "127.0.0.1:4000"
object_request_timeout_secs = 10
data_dir = "./var/so3"

[cluster]
peers = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa@127.0.0.1:4001",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb@127.0.0.1:4002",
]
```

Supported environment overrides:

| Variable                          | Meaning                                    |
|-----------------------------------|--------------------------------------------|
| `SO3_CONFIG`                      | Path to TOML config                        |
| `SO3_NODE_ID`                     | Local UUID; optional                       |
| `SO3_OBJECT_ADDR`                 | S3-compatible API bind address             |
| `SO3_RPC_ADDR`                    | Private tonic RPC bind address             |
| `SO3_OBJECT_REQUEST_TIMEOUT_SECS` | S3-compatible request timeout              |
| `SO3_DATA_DIR`                    | Base data directory                        |
| `SO3_METADATA_DIR`                | Metadata/journal directory                 |
| `SO3_BLOB_DIR`                    | Blob directory                             |
| `SO3_CLUSTER_PEERS`               | Comma-separated `uuid@host:port` peer list |

## Run

```bash
cargo run -p so3
```

Example object calls:

```bash
curl -X PUT --data-binary 'hello' http://127.0.0.1:3000/demo/key
curl -i http://127.0.0.1:3000/demo/key
curl -I http://127.0.0.1:3000/demo/key
curl -X DELETE http://127.0.0.1:3000/demo/key
```

## Verification

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -W clippy::pedantic
```

Maelstrom helper scripts and caveats live in [docs/maelstrom.md](docs/maelstrom.md).
Performance benchmarks must use release binaries. The k6 helper refuses to benchmark a detected
debug `so3` process by default and prints CPU/RSS aggregates:

```bash
cargo build --release -p so3
SO3_OBJECT_ADDR=127.0.0.1:3301 SO3_RPC_ADDR=127.0.0.1:4301 target/release/so3
SO3_ADDR=http://127.0.0.1:3301 bash scripts/k6/run-benchmark.sh --runs 30
```

Historical benchmark notes live in [docs/results.md](docs/results.md).

## License

All code in this repository is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option.
