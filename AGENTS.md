# AGENTS.md

## Purpose

This repository is a minimal prototype of a replicated object store built in Rust on top of the Accord consensus protocol.

The near-term product goal is narrow on purpose:

- Support only `Read`, `Write`, and `CAS`.
- Verify behavior with Jepsen Maelstrom's lin-kv workload.
- Prefer correctness, determinism, and recoverability over throughput or feature breadth.
- Avoid dependencies on external services.
- Persist metadata in SQLite and object blobs on the local filesystem.

This repo is an early but functional prototype scaffold. Agents should optimize for clarifying and completing the intended architecture, not for preserving accidental placeholders.
Full S3 API support is planned, but only in the distant future.

## Current Repository State

The codebase currently compiles and has working prototype paths for storage, node bootstrap, object APIs, RPC transport, and Maelstrom smoke testing:

- `crates/core` is the main library crate.
- `crates/so3` is the binary crate for config loading, node bootstrap, and signal handling.
- `crates/so3-maelstrom` is the Maelstrom adapter used for lin-kv verification.
- `object_server` exposes the minimal object API and maps requests to object commands.
- `rpc_server` exposes the intra-cluster Accord transport over tonic.
- `node` wires storage, recovery, local/peer consensus transports, object API, and RPC API.
- `proto/consensus.proto` describes the current Accord transport RPCs and should be treated as the seed of the intra-cluster protocol.
- `storage` persists object metadata in SQLite, blob bytes on the filesystem, applied command results, and the consensus journal.
- `consensus` has command IDs, HLC timestamps, durable journal state, replay of committed commands, a coordinator, and an applying transport.
- `docs/classDiagram.puml` is the current detailed structural sketch.
- `docs/moduleDiagram.drawio` is the current compact editable module overview for presentations.

When code and diagrams disagree, prefer:

1. The explicit project goal in this file.
2. Working code and tests.
3. Recent design notes written for the current prototype direction.
4. Updated diagrams only after they are brought back in line with the code.

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

Current Accord implementation status:

- `PreAccept`, `Accept`, `Commit`, `Apply`, and `Recover` are wired through the local applying transport and tonic peer transport.
- Command journal entries persist protocol state, serialized command/result bytes, `timestamp_zero`, selected timestamp, dependency set, and highest accepted ballot.
- Pre-accept computes dependencies for unapplied local conflicts in the same object key.
- Accept rejects stale ballots using durable ballot metadata.
- Recover reports durable local state, timestamp, dependencies, `wait_for` for unapplied dependencies, and stale-ballot `nack`.
- Commit currently applies immediately after recording committed state; dependency ordering is represented in metadata but is not yet fully enforced before apply.
- The coordinator drives a basic all-replica path; it does not yet implement full Accord fast/slow quorum behavior, recovery orchestration, or retry with higher ballots.

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

- `cargo clippy --all-targets --all-features -- -W clippy::pedantic`
- `cargo test`
- targeted integration tests if added
- relevant Maelstrom smoke or full lin-kv runs when consensus, node runtime, or Maelstrom behavior changes

When running Maelstrom, include the exact command used and the workload covered. Existing useful commands:

- `bash scripts/maelstrom/smoke-lin-kv.sh`
- `bash scripts/maelstrom/smoke-3-node-lin-kv.sh`
- `bash scripts/maelstrom/run-lin-kv.sh`

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

High-priority remaining work before final prototype testing:

1. Complete Accord recovery orchestration. ✓
   - Peer `Recover` calls wired into the coordinator/peer transport.
   - Recovered state, timestamps, dependencies, `wait_for`, and `nack` responses merged.
   - Retry with a higher durable ballot when recovery or accept receives a stale-ballot rejection.
   - Committed/Applied state observed on a peer: coordinator re-broadcasts commit and skips Accept.

2. Enforce dependency-aware apply behavior. ✓
   - Committed commands are not applied before their durable dependencies are applied.
   - `apply_committed_commands` loops until no progress is made; blocked commands are reported.
   - Tests cover conflicting writes where dependency order matters.

3. Harden Accord quorum semantics. ✓
   - `AccordCoordinator` uses majority quorum (`total / 2 + 1`) for PreAccept and Accept.
   - RPC errors from a minority of peers are tolerated; a nack still triggers recovery immediately.
   - Fast path: when all replicas respond with the same `timestamp_zero` (unanimous), the Accept
     phase is skipped and the coordinator commits directly with `timestamp_zero`.
   - Commit is broadcast best-effort to peers; peers that miss it learn via recovery.
   - Tests cover fast path, minority-failure tolerance, majority-failure rejection, and
     best-effort commit (peer commit failure does not fail the operation).

4. Improve failure and retry behavior. ✓
   - `So3Error::PeerUnavailable` added for transient peer failures; distinguished from protocol
     errors (`InvalidRequest`) and local storage errors (`Storage`/`Io`).
   - `map_tonic_status` in the peer transport classifies `Unavailable`/`DeadlineExceeded` as
     `PeerUnavailable`; all other codes remain `InvalidRequest`.
   - `map_error` in the applying transport maps `PeerUnavailable` back to `Status::unavailable`
     so the distinction survives round-trips.
   - `LocalConsensusObjectCommandExecutor` retries the full Accord flow on `PeerUnavailable`
     up to three times with exponential back-off. Each retry uses a fresh command ID so a
     partially-propagated ID cannot be confused with the retry.
   - Non-transient failures (ballot rejection, storage error, serialization) are returned
     immediately; the caller never receives a success before a durable commit.

5. Expand restart and recovery coverage. ✓
   - `next_sequence_is_monotonic_after_restart_with_mixed_state_commands`: verifies that
     `next_sequence_for_origin` is strictly greater than the highest durable sequence across
     pre-accepted, accepted, and committed commands after a journal reopen.
   - `replay_applies_cross_origin_committed_commands_in_dependency_order_after_restart`:
     two origins with a cross-dependency; simulates restart and verifies both commands are
     applied in dependency order and the objects are durably readable.
   - `pre_accepted_and_accepted_commands_are_skipped_during_replay`: committed command whose
     dependencies are only pre-accepted/accepted remains blocked; those commands stay in their
     original states; durable ballot and timestamp metadata is preserved.

6. Run final Maelstrom verification matrix.
   - Single-node smoke for adapter regressions. ✓
   - Three-node smoke at low rate. ✓
   - Longer three-node lin-kv run at higher rate/concurrency. ✓
   - A run with process restarts or partitions once the harness supports it.
   - Preserve exact commands, rates, concurrency, node count, and whether histories are valid.
   - Do not commit run-specific Maelstrom result paths; `store/lin-kv/` is gitignored.

## Remaining Prototype Readiness Work

The core `Read`/`Write`/`CAS` path is now in reasonable prototype shape: durable local storage,
Accord transport phases, recovery replay, dependency-aware apply, quorum behavior, retry behavior,
and Maelstrom lin-kv smoke/stress runs are in place.

Remaining work before calling the prototype ready:

1. Add Maelstrom fault runs.
   - Extend or configure the harness for process restarts and partitions.
   - Verify that committed-but-not-applied commands recover safely after node restart.
   - Record only reproducible commands, parameters, and validity verdicts in docs.

2. Improve availability under load without weakening safety.
   - Longer three-node lin-kv currently remains valid but can produce client `:net-timeout`
     `info` operations at higher rate/concurrency.
   - Any improvement must preserve the rule that clients do not receive success before the command
     is durably committed and locally applied.

3. Tighten Accord completeness.
   - The coordinator has majority quorum, fast path, slow path accept, recovery retry, and
     best-effort commit broadcast.
   - It still is not a full Accord implementation with all production-grade recovery orchestration,
     background anti-entropy, or optimized dependency pruning.

4. Keep documentation current.
   - `docs/classDiagram.puml` is the current detailed structural diagram.
   - `docs/moduleDiagram.drawio` is the slide-sized editable architectural module diagram.
   - `docs/maelstrom.md` records verification commands and verdicts without gitignored result paths.

## References

Useful local references:

- `crates/core/proto/consensus.proto` for current Accord transport sketch
- `docs/moduleDiagram.drawio` for a compact editable architecture overview
- `docs/classDiagram.puml` for the detailed current structural sketch

Keep diagrams aligned with working code. When code and diagrams disagree, working code and tests win;
update the diagrams as part of the same change when architecture or ownership boundaries move.

External projects such as `synevi` or `s3pico` can be consulted for Accord ideas, but do not copy architecture blindly. This repository should stay smaller, more explicit, and directly optimized for a correct object-store prototype.
