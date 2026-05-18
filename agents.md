# Known Issues

`cargo test --workspace` passes. `cargo clippy --workspace --all-targets -- -W clippy::pedantic` also completes,
but the workspace still emits many warnings. The remaining issues below are correctness, durability, Accord
conformance, Maelstrom parity, and operational risks that current tests do not cover.

## Critical

1. Coordinator local apply bypasses committed-command reorder gating.
   Remote inbound `Apply` waits for earlier committed timestamps via `reorder_buffer`
   (`crates/core/src/use_case/inbound_consensus/apply.rs`), but coordinator `apply_local` only waits explicit
   dependencies (`crates/core/src/service/consensus_coordinator/service.rs`). A node can apply its own coordinated
   command while an earlier committed command received through inbound `Commit` is still unapplied.

## High Risk

2. Accept after missed PreAccept discards local conflict dependencies.
   `accept_internal` synthesizes a PreAccepted row when the replica missed PreAccept, but ignores the dependencies
   returned by `check_conflicts_and_record_pre_accepted` and responds with only `req.dependencies`
   (`crates/core/src/use_case/inbound_consensus/accept.rs`). Slow-path or recovery Accept sent to a replica outside the
   PreAccept quorum can lose conflicts known only by that replica.

3. Blob fetch/repair accepts unverified bytes.
   Fetch paths stream bytes from the first peer that responds and commit by `blob_id` only, without checking the
   expected size or SHA-256 from object metadata (`crates/core/src/use_case/object/use_case.rs`,
   `crates/core/src/use_case/inbound_consensus/use_case.rs`,
   `crates/core/src/service/consensus_coordinator/service.rs`). A corrupt or buggy peer can repair local storage with
   invalid bytes.

4. Maelstrom hides production multi-coordinator behavior.
   The Maelstrom runtime forwards all client requests to `node_ids.first()` (
   `crates/so3-maelstrom/src/runtime/types.rs`,
   `crates/so3-maelstrom/src/runtime/handler/client.rs`). Production allows any node to coordinate via the shared core
   object use case. This means Maelstrom is not exercising concurrent coordinators, where Accord dependency and recovery
   bugs are most likely.

5. Maelstrom CAS create-if-not-exists is not atomic.
   Missing-key CAS performs a coordinated read, then an unconditional write when `create_if_not_exists` is true
   (`crates/so3-maelstrom/src/service.rs`). Two concurrent create-CAS operations can both return `cas_ok`, with the
   later write overwriting the earlier value.

## Medium Risk

6. PreAccept replay can return dependencies that differ from durable journal state.
   `check_conflicts_and_record_pre_accepted` computes conflicts, then uses `INSERT OR IGNORE`, and returns the newly
   computed dependency set (`crates/core/src/repository/consensus_journal/sqlite.rs`). If the row already existed, the
   response may not match the dependency set stored in SQLite.

7. RPC calls lack operation deadlines.
   Production tonic clients use lazy channels and await each consensus/blob call without per-call deadlines
   (`crates/core/src/client/consensus_transport_client.rs`, `crates/core/src/client/blob_client.rs`). Maelstrom
   consensus/blob/forward requests await oneshot responses without timeout (`crates/so3-maelstrom/src/runtime/peer.rs`,
   `crates/so3-maelstrom/src/runtime/handler/client.rs`). Partitions can leak pending maps and stall handlers.

8. Generated durable node identity is not written atomically or fsynced.
   The fallback identity is generated when no configured or stored `node_id` exists, but persistence is a plain
   `fs::write` (`crates/core/src/use_case/node_identity/use_case.rs`,
   `crates/core/src/repository/node_identity/fs.rs`). A crash after startup can still lose or partially write the
   identity file.

9. Maelstrom blob push/fetch does not match production blob transport validation.
   `MaelstromBlobPeerClient::push` ignores the `size` and `sha256` arguments, sends a single JSON payload, and the
   receiver commits it without checksum validation (`crates/so3-maelstrom/src/runtime/peer.rs`,
   `crates/so3-maelstrom/src/runtime/handler/blob.rs`). This diverges from the production push path and can hide blob
   validation bugs.

## TODO

- Blob replication to peers is currently sequential (one peer at a time). Consider parallel tee-streaming: read the
  committed local file once, fan out to N peers simultaneously. Requires a multi-consumer broadcast stream (e.g.
  `tokio::sync::broadcast` or a custom tee combinator), which adds meaningful complexity.

## Performance / Scalability TODO

These tasks are still useful, but they should not be interpreted as blockers for a product-level performance comparison.
For the current PoC, performance work is mainly needed when it helps explain or stabilize fault/conflict/recovery
dynamics: queue growth, dependency-chain growth, recovery backlog, and normalized latency/throughput changes.

- Add SQLite indexes for consensus journal hot paths.
  `check_conflicts_and_record_pre_accepted` scans `consensus_journal` by `key` and `state < Applied`
  (`crates/core/src/repository/consensus_journal/sqlite.rs`) but the table only has the primary key
  `(origin_node_id, sequence)`. Add and benchmark an index such as `(key, state)` for conflict detection and an index on
  `state` for `list_by_state` / startup reorder-buffer loading. Re-check write overhead after adding the indexes.

- Add consensus journal compaction or snapshotting.
  Applied commands remain in `consensus_journal` forever. Even with indexes, the table grows with every `PUT`, `GET`,
  `HEAD`, and `DELETE`, increasing SQLite file size, cache churn, startup scans, and conflict-index maintenance. Define
  a safe retention boundary for applied commands after dependencies/recovery no longer need the full row, then archive,
  compact, or checkpoint old entries.

- Stop routing every `GET` and `HEAD` through the write-heavy consensus coordinator.
  `ObjectUseCaseImpl::read` and `head` coordinate `ObjectCommand::Read` before loading metadata
  (`crates/core/src/use_case/object/read.rs`, `crates/core/src/use_case/object/head.rs`). This appends journal rows and
  performs consensus work for read-only S3 operations, so read-heavy benchmarks age the journal like write workloads.
  Decide the intended consistency contract for S3 reads and implement an optimized read path, for example local read
  after a committed/applied watermark, quorum read, lease/read-index, or a documented weaker mode.

- Add blob garbage collection for overwritten and deleted objects.
  Each write/CAS creates a fresh immutable `BlobId`, while object metadata stores only the current blob
  (`crates/core/src/repository/metadata/sqlite.rs`, `crates/core/src/repository/blob/fs.rs`). Repeated writes to the
  same key leave old committed blob files behind, and `DELETE` removes metadata but not historical blobs. Add reference
  tracking, tombstone-aware cleanup, or a mark-and-sweep pass that is safe with recovery and in-flight consensus.

- Avoid flat committed-blob directories for large stores.
  `FileSystemBlobRepository` stores every committed blob directly under `blob_dir/committed`
  (`crates/core/src/repository/blob/fs.rs`). Large benchmark or production runs can create thousands to millions of
  files in one directory. Shard committed blobs by hash prefix or another stable fanout scheme before relying on large
  object counts.

- Parallelize consensus RPC phases to quorum.
  Coordinator `PreAccept`, `Accept`, and `Commit` loops currently await peers sequentially
  (`crates/core/src/service/consensus_coordinator/service.rs`). In multi-node production this makes phase latency
  roughly the sum of peer latencies instead of the latency to the fastest quorum. Send requests concurrently, stop when
  quorum is reached, apply per-RPC deadlines, and drain/cancel the remaining work carefully.

- Reduce small-object fsync amplification or make durability level configurable.
  A single small `PUT` fsyncs blob data, fsyncs the committed directory, updates consensus journal state multiple times,
  and updates object metadata with SQLite `synchronous=FULL`
  (`crates/core/src/repository/blob/fs.rs`, `crates/core/src/repository/consensus_journal/sqlite.rs`,
  `crates/core/src/repository/metadata/sqlite.rs`). This is durable but expensive and dominates small-object latency.
  Consider group commit, batching, WAL checkpoint tuning, or an explicit benchmark/dev durability profile while keeping
  the default production profile conservative.

- Update k6 methodology to separate fresh-run performance from aging behavior.
  A 30-run benchmark against one long-lived process and one `SO3_DATA_DIR` measures storage/journal aging as much as
  steady-state release performance. `scripts/k6/run-backend-benchmark.py --backend so3` now starts `target/release/so3`
  on a fresh temp data dir for each run, samples CPU/RSS for that run, stops the process, and reports fresh-run
  aggregates. Long-lived aging aggregates are still a separate possible follow-up.

- Add fault/conflict/recovery-oriented workload profiles.
  The old k6 profile is useful as a stable baseline, but the new research framing needs dedicated scenarios: fail one
  node during load, restore it and track recovery, route clients through multiple entry nodes, run hot-key and mixed
  hot/independent-key workloads, and report ratios relative to each scenario's own baseline rather than competitor
  throughput.

## Test Gaps

- No multi-node production-node integration test covers real tonic communication, coordinator concurrency, and mixed
  client entrypoints.
- No tests cover recovery with local-only dependencies, accepted ballots from multiple replicas, or committed/applied
  state present only on the recovering coordinator.
- No tests cover missed-PreAccept Accept delivery where the accepting replica discovers additional local conflicts.
- No crash/restart tests cover atomic durable `NodeId` persistence or coordinator-vs-inbound apply ordering.
- No tests verify blob fetch/repair against expected SHA-256 and size, or Maelstrom blob parity with production blob
  transport.
- No Maelstrom tests exercise non-leader local coordination or atomic create-if-not-exists CAS races.
