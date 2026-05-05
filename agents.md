# Audit Findings

Current status: `cargo test --workspace` passes. `cargo clippy --workspace --all-targets -- -W clippy::pedantic` also
completes, but the workspace still emits many warnings. The remaining issues below are correctness, durability, Accord
conformance, Maelstrom parity, and operational risks that current tests do not cover.

## Critical

1. Recovery counts the coordinator as part of the quorum but does not include local replica state in the recovered
   decision.
   `recover_and_complete` records a local recovery ballot, then collects only peer `RecoverSuccess` values and checks
   quorum as `successes.len() + 1` (`crates/core/src/service/consensus_coordinator/service.rs`). Local dependencies,
   local committed/applied state, and local `wait_for` are not merged into the recovered value. This can commit a
   recovery decision that omits conflicts known only by the coordinator replica.

2. Recovery cannot choose the accepted value by ballot.
   `RecoverSuccess` exposes `superseding: bool`, dependencies, and timestamp, but not the accepted ballot
   (`crates/core/src/domain/consensus/transport.rs`). `recover_and_complete` chooses a superseding value by max
   timestamp instead of the highest accepted ballot (`crates/core/src/service/consensus_coordinator/service.rs`). Accord
   recovery needs enough accepted-ballot information to preserve a previously accepted value.

3. Coordinator local apply bypasses committed-command reorder gating.
   Remote inbound `Apply` waits for earlier committed timestamps via `reorder_buffer`
   (`crates/core/src/use_case/inbound_consensus/apply.rs`), but coordinator `apply_local` only waits explicit
   dependencies (`crates/core/src/service/consensus_coordinator/service.rs`). A node can apply its own coordinated
   command while an earlier committed command received through inbound `Commit` is still unapplied.

## High Risk

4. Accept after missed PreAccept discards local conflict dependencies.
   `accept_internal` synthesizes a PreAccepted row when the replica missed PreAccept, but ignores the dependencies
   returned by `check_conflicts_and_record_pre_accepted` and responds with only `req.dependencies`
   (`crates/core/src/use_case/inbound_consensus/accept.rs`). Slow-path or recovery Accept sent to a replica outside the
   PreAccept quorum can lose conflicts known only by that replica.

5. Blob fetch/repair accepts unverified bytes.
   Fetch paths stream bytes from the first peer that responds and commit by `blob_id` only, without checking the expected
   size or SHA-256 from object metadata (`crates/core/src/use_case/object/use_case.rs`,
   `crates/core/src/use_case/inbound_consensus/use_case.rs`,
   `crates/core/src/service/consensus_coordinator/service.rs`). A corrupt or buggy peer can repair local storage with
   invalid bytes.

6. Maelstrom hides production multi-coordinator behavior.
   The Maelstrom runtime forwards all client requests to `node_ids.first()` (`crates/so3-maelstrom/src/runtime/types.rs`,
   `crates/so3-maelstrom/src/runtime/handler/client.rs`). Production allows any node to coordinate via the shared core
   object use case. This means Maelstrom is not exercising concurrent coordinators, where Accord dependency and recovery
   bugs are most likely.

7. Maelstrom CAS create-if-not-exists is not atomic.
   Missing-key CAS performs a coordinated read, then an unconditional write when `create_if_not_exists` is true
   (`crates/so3-maelstrom/src/service.rs`). Two concurrent create-CAS operations can both return `cas_ok`, with the
   later write overwriting the earlier value.

## Medium Risk

8. PreAccept replay can return dependencies that differ from durable journal state.
   `check_conflicts_and_record_pre_accepted` computes conflicts, then uses `INSERT OR IGNORE`, and returns the newly
   computed dependency set (`crates/core/src/repository/consensus_journal/sqlite.rs`). If the row already existed, the
   response may not match the dependency set stored in SQLite.

9. RPC calls lack operation deadlines.
   Production tonic clients use lazy channels and await each consensus/blob call without per-call deadlines
   (`crates/core/src/client/consensus_transport_client.rs`, `crates/core/src/client/blob_client.rs`). Maelstrom
   consensus/blob/forward requests await oneshot responses without timeout (`crates/so3-maelstrom/src/runtime/peer.rs`,
   `crates/so3-maelstrom/src/runtime/handler/client.rs`). Partitions can leak pending maps and stall handlers.

10. Generated durable node identity is not written atomically or fsynced.
    The fallback identity is generated when no configured or stored `node_id` exists, but persistence is a plain
    `fs::write` (`crates/core/src/use_case/node_identity/use_case.rs`,
    `crates/core/src/repository/node_identity/fs.rs`). A crash after startup can still lose or partially write the
    identity file.

11. Maelstrom blob push/fetch does not match production blob transport validation.
    `MaelstromBlobPeerClient::push` ignores the `size` and `sha256` arguments, sends a single JSON payload, and the
    receiver commits it without checksum validation (`crates/so3-maelstrom/src/runtime/peer.rs`,
    `crates/so3-maelstrom/src/runtime/handler/blob.rs`). This diverges from the production push path and can hide blob
    validation bugs.

## TODO

- Blob replication to peers is currently sequential (one peer at a time). Consider parallel tee-streaming: read the
  committed local file once, fan out to N peers simultaneously. Requires a multi-consumer broadcast stream (e.g.
  `tokio::sync::broadcast` or a custom tee combinator), which adds meaningful complexity.

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
