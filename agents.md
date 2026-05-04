# Audit Findings

Current status: `cargo test --workspace` passes, and `cargo clippy --workspace --all-targets -- -W clippy::pedantic` completes with warnings. The remaining issues are mostly Accord correctness and durability risks that current tests do not cover.

## Critical

1. (FIXED) Concurrent commands on the same key can miss each other during dependency discovery.
   `coordinate` runs `check_conflicts` and then separately calls `record_pre_accepted` (`crates/core/src/service/consensus_coordinator/service.rs`). Maelstrom handlers spawn requests concurrently, so two commands can both see no conflict before either is written to the journal.

2. (FIXED) Commit quorum is not enforced.
   The coordinator retries Commit up to `MAX_COMMIT_ATTEMPTS`, but after the loop it proceeds to local Apply even if quorum was never reached.

3. (FIXED) Journal state transitions can acknowledge missing rows.
   `accept_internal` can call `record_accepted` after `load(None)`, and SQLite `UPDATE` calls in `record_accepted`, `record_committed`, and `record_applied` do not check `rows_affected`.

4. (FIXED) Commit does not durably store the full final decision.
   `CommitRequest` includes final `timestamp` and `dependencies`, but `record_committed` only receives `command_id` and updates state. Slow-path final deps/timestamp can be lost before recovery.

## High Risk

5. (FIXED) `record_pre_accepted` uses `INSERT OR REPLACE`.
   A late or duplicate PreAccept can replace an Accepted/Committed/Applied row and drop ballot, timestamp, dependencies, or result data.

6. (FIXED) Apply failures after a committed decision are returned as regular errors.
   Local and inbound Apply return `PeerUnavailable` when dependencies are not yet applied. After a decision is committed, this can make clients retry and create duplicate commands instead of driving recovery/apply completion.

7. Blob replication requires every peer before consensus.
   Write/CAS push blobs to all peers before `coordinate`; any failed peer aborts the operation even when an Accord quorum would still be available.

## Medium Risk

8. Recovery is still mostly nominal.
   Missing entries are reported as synthetic `PreAccepted`, `superseding` is always false, recovery ballots are not recorded, and coordinator NACK handling returns an error instead of retrying through recovery.

9. Production node runtime is still unfinished.
   `Node::new`, `Node::bind`, and `BoundNode::run` contain `unimplemented!`, so the production node path is not usable even though maelstrom and config tests pass.

