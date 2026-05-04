# Audit Findings

Current status: `cargo test --workspace` passes. `cargo clippy --workspace --all-targets -- -W clippy::pedantic` also
completes, but the workspace still emits many warnings. The remaining issues below are correctness, durability, and
operational risks that current tests do not cover.

## Critical

1. (FIXED) Node identity is not durable by default.
   `so3` generates a fresh `Uuid::new_v4()` when `SO3_NODE_ID`/`node_id` is not configured (`crates/so3/src/config.rs`).
   A restarted node can come back with a different `NodeId`, so `max_sequence` no longer resumes the old command stream
   and durable consensus state is effectively orphaned.

2. Production node startup eagerly connects to every peer.
   `Node::new` constructs `ConsensusTransportClient` and `BlobClient` with `connect().await` for every configured peer (
   `crates/core/src/node/runtime.rs`). This can deadlock cluster bootstrap: a node cannot start while its peers are
   still down or starting.

3. Inbound `Commit`/`Apply` cannot handle a missing journal row.
   `commit_internal` calls `record_committed`, and SQLite only performs `UPDATE` with a `rows_affected == 1` check (
   `crates/core/src/use_case/inbound_consensus/commit.rs`, `crates/core/src/repository/consensus_journal/sqlite.rs`). If
   a replica missed PreAccept/Accept but receives Commit, it rejects the committed decision instead of synthesizing and
   storing the row.

## High Risk

4. Accepted dependency sets are not durably stored.
   `accept_internal` returns `req.dependencies`, but `record_accepted` only persists state, ballot, and timestamp (
   `crates/core/src/use_case/inbound_consensus/accept.rs`, `crates/core/src/repository/consensus_journal/sqlite.rs`).
   After a crash, recovery can observe an Accepted row with stale PreAccept dependencies.

5. Command execution and `Applied` journaling are not atomic.
   Both inbound apply and coordinator local apply mutate object metadata before calling `record_applied` (
   `crates/core/src/use_case/inbound_consensus/apply.rs`, `crates/core/src/service/consensus_coordinator/service.rs`). A
   crash in that window can make replay apply Write/CAS/Delete again and return a different result.

6. The committed reorder buffer is only in memory.
   `commit_internal` durably records the command as Committed, then inserts its timestamp into an in-memory
   `reorder_buffer` (`crates/core/src/use_case/inbound_consensus/commit.rs`). After restart, committed-but-not-applied
   commands are not restored into the buffer, so later Apply requests can bypass earlier committed commands.

7. Production peer identity is derived from socket address.
   `ClusterConfig` stores only `SocketAddr`, while `Node::new` maps peers to `NodeId(addr.to_string())` and self to the
   configured UUID (`crates/core/src/node/config.rs`, `crates/core/src/node/runtime.rs`). Accord needs stable peer
   identities; address-derived IDs cannot validate duplicate identities, self-in-peers, or node-id/address changes.

## Medium Risk

8. Recovery responses expose `wait_for`, but the coordinator ignores it.
   `recover_internal` computes unapplied dependencies and returns `wait_for`, but `recover_and_complete` only collects
   successes and dependencies (`crates/core/src/use_case/inbound_consensus/recover.rs`,
   `crates/core/src/service/consensus_coordinator/service.rs`). If `wait_for` is intended to drive Accord recovery
   ordering, recovery remains incomplete.

9. Blob RPC buffers full objects in memory and trusts header size too early.
   `BlobService::store_blob` collects all chunks into `Vec<Bytes>`, then `BlobUseCaseImpl::store` allocates
   `BytesMut::with_capacity(size as usize)` (`crates/core/src/api/rpc/tonic/blob_service.rs`,
   `crates/core/src/use_case/blob/use_case.rs`). There is no streaming write path or configured max object size, so a
   malformed peer can force large allocations.

10. Object reads do not repair or fetch missing local blobs.
    `read_internal` coordinates a Read, then loads the referenced blob only from the local repository (
    `crates/core/src/use_case/object/read.rs`). If metadata is present but the local blob is missing after crash or
    partial replication, GET fails instead of fetching from a peer and repairing local state.

## Test Gaps

- No multi-node production-node integration test covers startup ordering, peer identity, or real tonic communication.
- No tests cover missing-PreAccept Commit/Apply delivery.
- No crash/restart tests cover durable `NodeId`, committed reorder-buffer recovery, or apply idempotency.
- No tests cover blob fetch/repair after metadata exists but the local blob file is missing.
