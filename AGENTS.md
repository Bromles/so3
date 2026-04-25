# AGENTS.md

## Purpose

This repository is a minimal prototype of a replicated object store built in Rust on top of the Accord consensus protocol.

The near-term product goal is narrow on purpose:

- Support only `Read`, `Write`, and `CAS`.
- Verify behavior with Jepsen Maelstrom's lin-kv workload.
- Prefer correctness, determinism, and recoverability over throughput or feature breadth.
- Avoid dependencies on external services.
- Persist metadata in SQLite and object blobs on the local filesystem.

This repo is still an early scaffold. Agents should optimize for clarifying and completing the intended architecture, not for preserving accidental placeholders.
Full S3 API support is planned, but only in the distant future.

## Current Repository State

The codebase currently compiles, but most of it is skeletal:

- `crates/core` is the main library crate.
- `crates/so3` is the binary crate and currently only prints `Hello, world!`.
- `object_server` exists and starts an Axum server, but handlers are empty.
- `rpc_server` exists as a module placeholder and is not implemented yet.
- `node` exists as a partial builder/config shell.
- `proto/consensus.proto` already describes Accord transport RPCs and should be treated as the seed of the intra-cluster protocol.
- `docs/classDiagram.puml` is an outdated rough sketch and must not be treated as a source of truth.

When code and old diagram disagree, prefer:

1. The explicit project goal in this file.
2. Working code and tests.
3. Recent design notes written for the current prototype direction.
4. The diagram only as a distant historical reference.

## Architectural Direction

Keep the project split into a reusable library and a thin binary:

- `crates/core`
  - domain types
  - storage backends
  - consensus core
  - public object API server
  - internal RPC server
  - node bootstrap/composition
- `crates/so3`
  - config loading
  - process bootstrap
  - wiring a `Node`
  - signal handling

Target runtime shape:

- `ObjectServer`
  - public API used by clients
  - only exposes object and bucket operations
  - should not contain consensus logic directly
- `RPCServer`
  - private intra-cluster API
  - carries Accord protocol traffic and node-to-node coordination
- consensus core
  - pure application/replication logic as much as possible
  - deterministic state transitions
  - transport-agnostic interfaces where practical
- storage layer
  - SQLite for metadata
  - filesystem for blobs
  - no external DB, cache, queue, or object store
- `Node`
  - owns lifecycle, binds servers, composes storage + consensus + services

## Product Scope

The prototype is not an S3 clone. Keep the API surface minimal until Maelstrom verification is in place.

Preferred logical model:

- A single keyspace of objects keyed by string keys.
- Each object has:
  - key
  - current version or revision
  - blob reference
  - content length
  - checksum or etag
  - timestamps if needed
- Required operations:
  - `Read(key) -> value | not_found`
  - `Write(key, value) -> version`
  - `CAS(key, expected_version, value) -> ok | mismatch | not_found`

Do not add auth, ACLs, multipart upload, listing, leases, TTLs, or garbage collection unless they are necessary for correctness or required by tests.

## Core Invariants

These invariants are more important than feature speed:

- A committed write must survive process restart.
- Metadata and blob persistence must not point to missing data after a successful commit.
- CAS must be linearizable with respect to the chosen object version/revision model.
- Reads must not observe partially applied writes.
- Consensus decisions must be replayable from persisted state.
- State machine apply logic must be deterministic.
- Recovery must prefer safety over fast startup.
- Code must not be unsafe and should work on macOS, Windows and Linux

If an implementation choice makes these harder to reason about, reject it.

## Persistence Rules

Metadata lives in SQLite. Blob bytes live on disk.

Expected direction:

- SQLite stores the authoritative mapping from object key to current committed version and blob identifier.
- Blob files are immutable once committed.
- New blob data should be written to a temp file, flushed, and atomically renamed into place.
- Metadata should only reference a blob after the blob is durably placed.
- Deletion or garbage collection can be deferred; leaking unreachable blobs temporarily is preferable to corrupting committed data.
- Schema migrations must be explicit and versioned.

Do not introduce:

- PostgreSQL, MySQL, Redis, Kafka, NATS, etcd, Consul, or any other external service.
- A separate metadata service.
- In-memory-only state as the sole source of truth for committed data.

## Consensus and Command Model

Model object operations as deterministic commands applied by the replicated state machine.

Preferred command set:

- `Read`
- `Write`
- `Cas`

Suggested internal shape:

- command enum with fully typed payloads
- stable serialization for replicated commands
- explicit command/result types
- explicit object version/revision type, not ad-hoc strings everywhere

Keep consensus isolated from HTTP and storage details:

- HTTP handlers translate requests into commands.
- RPC handlers translate network messages into consensus calls.
- The state machine applies commands against storage-facing traits.

Accord-specific details may evolve, but transport boundaries should remain clean.

## Error Handling

Runtime code should not panic on normal failures.

Rules:

- Avoid `unwrap`/`expect` outside tests and obviously fatal startup code.
- Use typed errors at crate boundaries.
- Distinguish protocol errors, storage errors, and caller-visible object errors.
- Return structured mismatch errors for CAS.
- Log enough context to debug replication and recovery paths.

## Testing Expectations

Every meaningful change should preserve or improve testability.

Minimum expectations over time:

- unit tests for domain logic and state machine transitions
- integration tests for SQLite + filesystem persistence behavior
- restart/recovery tests
- concurrency tests around CAS semantics
- Maelstrom verification for `Read`/`Write`/`CAS`

Before finishing a change, run what is relevant:

- `cargo check`
- `cargo test`
- targeted integration tests if added

If Maelstrom is wired into the repo later, include the exact command used and the workload covered.

## Implementation Guidance

Prefer simple boundaries and small traits.

Good direction:

- strongly typed IDs and versions
- append-only or immutable blobs
- narrow interfaces between consensus, storage, and transport
- one obvious happy path
- explicit recovery path

Avoid:

- premature generic abstractions
- plugin-style storage engines
- mixing public API types with internal consensus types
- hidden background tasks that mutate durable state outside the replicated path
- broad "service" layers that only rename calls without adding invariants

## Code Organization Guidance

When extending the repo, move toward modules roughly like these:

- `types` or `domain`
  - keys, versions, object metadata, commands, errors
- `storage`
  - SQLite metadata store
  - filesystem blob store
- `consensus`
  - Accord engine, log/recovery state, command application
- `object_server`
  - client-facing handlers and request/response mapping
- `rpc_server`
  - tonic service implementation for Accord transport
- `node`
  - config, builder, lifecycle, shutdown

Do not split crates aggressively yet. A small number of coherent modules inside `so3-core` is better than many premature crates.

## Guidance for Editing This Repo

Because the repo is raw, agents are expected to correct structure when needed, but changes should stay aligned with the prototype goal.

When making edits:

- preserve the library/binary split
- keep public API and intra-cluster API separate
- prefer renaming misleading placeholders over layering new abstractions on top of them
- update docs when changing intended architecture
- keep generated protobuf output reproducible

If a new feature is proposed, ask:

1. Does it help `Read`, `Write`, `CAS`, durability, recovery, or Maelstrom verification?
2. Does it simplify the path to a correct Accord-backed prototype?
3. Can it be implemented without introducing external dependencies?

If the answer is no, defer it.

## Known Gaps To Close

High-priority missing pieces at the time of writing:

- real node configuration and bootstrap flow
- actual object API shape
- actual RPC server implementation
- persistent metadata schema
- filesystem blob layout
- command and result types for `Read`/`Write`/`CAS`
- durable version/revision model
- Accord engine integration with storage
- recovery and restart semantics
- tests beyond compilation
- Maelstrom harness or adapter

## References

Useful local references:

- `crates/core/proto/consensus.proto` for current Accord transport sketch

Treat `docs/classDiagram.puml` as stale historical material only. It may help explain old naming or abandoned structure, but it must not drive architecture or implementation choices.

External projects such as `synevi` or `s3pico` can be consulted for Accord ideas, but do not copy architecture blindly. This repository should stay smaller, more explicit, and directly optimized for a correct object-store prototype.
