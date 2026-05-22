# Known Issues

`cargo test --workspace` passes. `cargo clippy --workspace --all-targets -- -W clippy::pedantic` also completes,
but the workspace still emits many warnings. The remaining issues below are correctness, durability, Accord
conformance, Maelstrom parity, and operational risks that current tests do not cover.

## High Risk

1. Accept after missed PreAccept discards local conflict dependencies.
   `accept_internal` synthesizes a PreAccepted row when the replica missed PreAccept, but ignores the dependencies
   returned by `check_conflicts_and_record_pre_accepted` and responds with only `req.dependencies`
   (`crates/core/src/use_case/inbound_consensus/accept.rs`). Slow-path or recovery Accept sent to a replica outside the
   PreAccept quorum can lose conflicts known only by that replica.

2. Blob fetch/repair accepts unverified bytes.
   Fetch paths stream bytes from the first peer that responds and commit by `blob_id` only, without checking the
   expected size or SHA-256 from object metadata (`crates/core/src/use_case/object/use_case.rs`,
   `crates/core/src/use_case/inbound_consensus/use_case.rs`). A corrupt or buggy peer can repair local storage with
   invalid bytes.

3. Maelstrom CAS create-if-not-exists is not atomic.
   Missing-key CAS performs a coordinated read, then an unconditional write when `create_if_not_exists` is true
   (`crates/so3-maelstrom/src/service.rs`). Two concurrent create-CAS operations can both return `cas_ok`, with the
   later write overwriting the earlier value.

## Medium Risk

4. PreAccept replay can return dependencies that differ from durable journal state.
   `check_conflicts_and_record_pre_accepted` computes conflicts, then uses `INSERT OR IGNORE`, and returns the newly
   computed dependency set (`crates/core/src/repository/consensus_journal/sqlite.rs`). If the row already existed, the
   response may not match the dependency set stored in SQLite.

5. Blob RPC calls lack operation deadlines.
   Consensus RPC clients now have per-call deadlines (3 seconds), but blob RPC calls still await without deadlines
   (`crates/core/src/client/blob_client.rs`). Partitions can leak pending blob operations.

6. Generated durable node identity is not written atomically or fsynced.
   The fallback identity is generated when no configured or stored `node_id` exists, but persistence is a plain
   `fs::write` (`crates/core/src/use_case/node_identity/use_case.rs`,
   `crates/core/src/repository/node_identity/fs.rs`). A crash after startup can still lose or partially write the
   identity file.

7. Maelstrom blob push/fetch does not match production blob transport validation.
   `MaelstromBlobPeerClient::push` ignores the `size` and `sha256` arguments, sends a single JSON payload, and the
   receiver commits it without checksum validation (`crates/so3-maelstrom/src/runtime/peer.rs`,
   `crates/so3-maelstrom/src/runtime/handler/blob.rs`). This diverges from the production push path and can hide blob
   validation bugs.

8. Maelstrom consensus/blob/metadata-query oneshot responses lack timeouts.
   `send_rpc`, `send_blob_request`, and `send_metadata_query` in `crates/so3-maelstrom/src/runtime/peer.rs` await
   oneshot receivers without timeouts. Partitions can leak pending maps and stall handlers.

## Performance / Scalability TODO

These tasks are still useful, but they should not be interpreted as blockers for a product-level performance comparison.
For the current PoC, performance work is mainly needed when it helps explain or stabilize fault/conflict/recovery
dynamics: queue growth, dependency-chain growth, recovery backlog, and normalized latency/throughput changes.

- Add consensus journal compaction or snapshotting.
  Applied commands remain in `consensus_journal` forever. Even with indexes, the table grows with every `PUT`, `GET`,
  `HEAD`, and `DELETE`, increasing SQLite file size, cache churn, startup scans, and conflict-index maintenance. Define
  a safe retention boundary for applied commands after dependencies/recovery no longer need the full row, then archive,
  compact, or checkpoint old entries.

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
- No Maelstrom tests exercise atomic create-if-not-exists CAS races.
