use crate::client::interface::ConsensusPeerClient;
use crate::domain::clock::physical_millis_now;
use crate::domain::clock::{HybridLogicalClock, LogicalTimestamp};
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{AppliedSet, CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{
    AcceptRequest, ApplyRequest, CommitRequest, PreAcceptRequest, RecoverRequest, RecoverResponse,
    RecoverSuccess,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::BufferedEntry;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::service::consensus_coordinator::apply_engine::AccordApplyEngine;
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, sleep};
use tracing::info;

enum WriteBufferEntry {
    Write {
        timestamp: LogicalTimestamp,
        metadata: ObjectMetadata,
    },
    Deleted {
        timestamp: LogicalTimestamp,
    },
}

pub struct AccordConsensusCoordinatorService<CJR, CPC, OMR>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
{
    node_id: NodeId,
    epoch: AtomicU64,
    hlc: Mutex<HybridLogicalClock>,
    sequence: AtomicU64,
    network_skew_ms: u64,
    consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
    engine: Arc<AccordApplyEngine<CJR, OMR>>,
    consensus_journal_repository: Arc<CJR>,
    apply_notify: Arc<Notify>,
    in_flight_operations: AtomicU64,
    write_buffer: Arc<DashMap<ObjectKey, WriteBufferEntry>>,
}

struct InFlightOperationGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> InFlightOperationGuard<'a> {
    fn new(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for InFlightOperationGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CompletionMetrics {
    result: CommandResult,
    commit_ms: u64,
    apply_ms: u64,
    commit_attempts: u32,
    commit_ok: usize,
}

fn command_object_key(command: &ObjectCommand) -> &ObjectKey {
    match command {
        ObjectCommand::Read { key }
        | ObjectCommand::Write { key, .. }
        | ObjectCommand::Cas { key, .. }
        | ObjectCommand::Delete { key } => key,
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl<CJR, CPC, OMR> AccordConsensusCoordinatorService<CJR, CPC, OMR>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
{
    pub async fn new(
        node_id: NodeId,
        epoch: u64,
        network_skew_ms: u64,
        consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
        consensus_journal_repository: Arc<CJR>,
        object_metadata_repository: Arc<OMR>,
        apply_notify: Arc<Notify>,
    ) -> So3Result<Self> {
        let initial_sequence = consensus_journal_repository
            .max_sequence(&node_id)
            .await?
            .saturating_add(1);
        let hlc = HybridLogicalClock::new(node_id.clone());

        let engine = AccordApplyEngine::new(
            Arc::clone(&consensus_journal_repository),
            Arc::clone(&object_metadata_repository),
            Arc::clone(&apply_notify),
        );
        let engine = Arc::new(engine);

        let committed = consensus_journal_repository
            .list_by_state(JournalState::Committed)
            .await?;
        engine.populate_from_journal(committed);

        Ok(Self {
            node_id,
            epoch: AtomicU64::new(epoch),
            hlc: Mutex::new(hlc),
            sequence: AtomicU64::new(initial_sequence),
            network_skew_ms,
            consensus_peer_client_map,
            engine,
            consensus_journal_repository,
            apply_notify,
            in_flight_operations: AtomicU64::new(0),
            write_buffer: Arc::new(DashMap::new()),
        })
    }

    fn next_command_id(&self) -> CommandId {
        CommandId {
            origin_node_id: self.node_id.clone(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Release);
    }

    // n = self + peers; quorum = strict majority
    fn quorum_size(&self) -> usize {
        self.consensus_peer_client_map.len().div_ceil(2) + 1
    }

    async fn last_applied(&self) -> So3Result<AppliedSet> {
        let entries = self
            .consensus_journal_repository
            .list_by_state(JournalState::Applied)
            .await?;
        Ok(AppliedSet(
            entries.into_iter().map(|e| e.command_id).collect(),
        ))
    }

    /// Recover any journal entries left in PreAccepted/Accepted state from a prior crash.
    /// Called once at startup. Each stalled entry is recovered through the full Accord
    /// recovery path, which learns any decision already reached on other replicas or
    /// commits one if the original coordinator never finished.
    pub async fn recover_stalled_entries(&self) {
        let peers: Vec<Arc<CPC>> = self.consensus_peer_client_map.values().cloned().collect();
        let quorum = self.quorum_size();

        let stalled = match self
            .consensus_journal_repository
            .list_by_state(JournalState::PreAccepted)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("failed to list PreAccepted entries: {e}");
                return;
            }
        };
        let stalled_accepted = match self
            .consensus_journal_repository
            .list_by_state(JournalState::Accepted)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("failed to list Accepted entries: {e}");
                return;
            }
        };

        let all_stalled: Vec<_> = stalled.into_iter().chain(stalled_accepted).collect();
        if all_stalled.is_empty() {
            return;
        }

        tracing::info!(
            stalled_count = all_stalled.len(),
            "recovering stalled journal entries from prior crash"
        );

        // Phase 1: commit all stalled entries without applying.
        // We must not wait for inter-entry dependencies during commit because
        // the entries may form a dependency chain — waiting would deadlock
        // against our own sequential loop.
        let mut committed = Vec::new();
        for entry in &all_stalled {
            let ballot = Ballot::initial(self.node_id.clone());
            let last_applied = match self.last_applied().await {
                Ok(la) => la,
                Err(e) => {
                    tracing::warn!("failed to get last_applied: {e}");
                    continue;
                }
            };
            match self
                .recover_and_commit(
                    &entry.command_id,
                    &entry.command,
                    &entry.timestamp_zero,
                    &ballot,
                    &peers,
                    quorum,
                    &last_applied,
                )
                .await
            {
                Some(commit_req) => {
                    tracing::info!(
                        origin_node = entry.command_id.origin_node_id.as_ref(),
                        sequence = entry.command_id.sequence,
                        "stalled entry committed"
                    );
                    committed.push(commit_req);
                }
                None => {
                    tracing::warn!(
                        origin_node = entry.command_id.origin_node_id.as_ref(),
                        sequence = entry.command_id.sequence,
                        "stalled entry recovery failed"
                    );
                }
            }
        }

        // Phase 2: apply all committed entries in timestamp order.
        // In Accord, if entry A depends on entry B (same key), then B was
        // committed with a lower-or-equal timestamp.  Sorting ascending
        // guarantees dependencies are applied before the entries that need them.
        committed.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        for commit_req in &committed {
            let apply_req = ApplyRequest {
                command_id: commit_req.command_id.clone(),
                command: commit_req.command.clone(),
                timestamp_zero: commit_req.timestamp_zero.clone(),
                timestamp: commit_req.timestamp.clone(),
                dependencies: commit_req.dependencies.clone(),
            };

            // Verify all local dependencies are Applied before applying.
            // We process in timestamp order so a dependency must have been attempted
            // already; if it is still Committed the apply failed and we must skip
            // dependents too.
            if !self.deps_ready(&apply_req).await {
                tracing::warn!(
                    origin_node = apply_req.command_id.origin_node_id.as_ref(),
                    sequence = apply_req.command_id.sequence,
                    "skipping: dependency not applied (prior entry failed?)"
                );
                continue;
            }

            match self.engine.apply(&apply_req).await {
                Ok(_) => {
                    tracing::info!(
                        origin_node = apply_req.command_id.origin_node_id.as_ref(),
                        sequence = apply_req.command_id.sequence,
                        "stalled entry applied"
                    );
                    // Fire-and-forget apply RPCs to peers.
                    #[allow(clippy::unnecessary_to_owned)]
                    for peer in peers.iter().cloned() {
                        let req = apply_req.clone();
                        tokio::spawn(async move {
                            let _ = peer.apply(req).await;
                        });
                    }
                }
                Err(e) => tracing::warn!(
                    origin_node = apply_req.command_id.origin_node_id.as_ref(),
                    sequence = apply_req.command_id.sequence,
                    "stalled entry apply failed: {e}"
                ),
            }
        }

        // Phase 3: retry apply for entries Committed in a previous run but not Applied
        // (e.g., blob was temporarily unavailable at apply time).
        let mut committed_not_applied = match self
            .consensus_journal_repository
            .list_by_state(JournalState::Committed)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("failed to list Committed entries: {e}");
                return;
            }
        };

        if !committed_not_applied.is_empty() {
            committed_not_applied.sort_by(|a, b| match (&a.timestamp, &b.timestamp) {
                (Some(ta), Some(tb)) => ta.cmp(tb),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });

            tracing::info!(
                count = committed_not_applied.len(),
                "applying committed-but-not-applied journal entries"
            );
            for entry in &committed_not_applied {
                let Some(ts) = &entry.timestamp else {
                    tracing::warn!(
                        origin_node = entry.command_id.origin_node_id.as_ref(),
                        sequence = entry.command_id.sequence,
                        "committed entry missing timestamp, skipping"
                    );
                    continue;
                };
                let timestamp = ts.clone();

                let apply_req = ApplyRequest {
                    command_id: entry.command_id.clone(),
                    command: entry.command.clone(),
                    timestamp_zero: entry.timestamp_zero.clone(),
                    timestamp: timestamp.clone(),
                    dependencies: entry.dependencies.clone(),
                };

                // Readiness check: all local deps must be Applied.
                if !self.deps_ready(&apply_req).await {
                    tracing::warn!(
                        origin_node = apply_req.command_id.origin_node_id.as_ref(),
                        sequence = apply_req.command_id.sequence,
                        "skipping committed entry: dependency not applied"
                    );
                    continue;
                }

                match self.engine.apply(&apply_req).await {
                    Ok(_) => {
                        tracing::info!(
                            origin_node = apply_req.command_id.origin_node_id.as_ref(),
                            sequence = apply_req.command_id.sequence,
                            "committed entry applied"
                        );
                        #[allow(clippy::unnecessary_to_owned)]
                        for peer in peers.iter().cloned() {
                            let req = apply_req.clone();
                            tokio::spawn(async move {
                                let _ = peer.apply(req).await;
                            });
                        }
                    }
                    Err(e) => tracing::warn!(
                        origin_node = apply_req.command_id.origin_node_id.as_ref(),
                        sequence = apply_req.command_id.sequence,
                        "committed entry apply failed: {e}"
                    ),
                }
            }
        }
    }

    /// Check whether all explicit dependencies of `req` are ready for apply:
    /// Applied, not in local journal, or spurious (higher timestamp).
    async fn deps_ready(&self, req: &ApplyRequest) -> bool {
        for dep_id in &req.dependencies.0 {
            match self.consensus_journal_repository.load(dep_id).await {
                Ok(None) | Err(_) => {}
                Ok(Some(e)) if e.state == JournalState::Applied => {}
                Ok(Some(e)) if e.timestamp.as_ref() > Some(&req.timestamp) => {}
                _ => return false,
            }
        }
        true
    }

    async fn apply_with_recovery(&self, req: &ApplyRequest) -> So3Result<CommandResult> {
        let max_recovery_attempts = 3usize;
        let mut recovery_attempts = 0usize;

        'deps: loop {
            for dep_id in &req.dependencies.0 {
                match self.consensus_journal_repository.load(dep_id).await? {
                    None => {}
                    Some(e) if e.state == JournalState::Applied => {}
                    Some(e) if e.timestamp.as_ref() > Some(&req.timestamp) => {}
                    Some(e) if e.state == JournalState::Committed => {
                        recovery_attempts += 1;
                        if recovery_attempts > max_recovery_attempts {
                            return Err(So3Error::PeerUnavailable(format!(
                                "apply_with_recovery: aborted after {max_recovery_attempts} recovery attempts for committed dependency"
                            )));
                        }
                        self.recover_and_apply_stalled_chain(dep_id).await?;
                        continue 'deps;
                    }
                    Some(_) => {
                        recovery_attempts += 1;
                        if recovery_attempts > max_recovery_attempts {
                            return Err(So3Error::PeerUnavailable(format!(
                                "apply_with_recovery: aborted after {max_recovery_attempts} recovery attempts for stalled dependency"
                            )));
                        }
                        self.recover_and_apply_stalled_chain(dep_id).await?;
                        continue 'deps;
                    }
                }
            }
            break;
        }

        self.engine.apply(req).await
    }

    /// On-demand recovery of a stalled dependency and its transitive stalled dependencies.
    /// Uses BFS to discover all stalled entries in the dependency chain, commits them via
    /// the Accord recovery protocol, then collects any Committed-but-not-Applied entries
    /// in the same chain and applies everything in timestamp order.
    async fn recover_and_apply_stalled_chain(&self, stalled_id: &CommandId) -> So3Result<()> {
        let peers: Vec<Arc<CPC>> = self.consensus_peer_client_map.values().cloned().collect();
        let quorum = self.quorum_size();

        // BFS to discover all stalled AND committed-but-not-applied entries in the
        // dependency chain.  We must include Committed entries because a stalled entry
        // may depend on a Committed one whose apply hasn't run yet — if we don't apply
        // it here, the readiness check in Phase 2 will skip it and we'll loop forever.
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(stalled_id.clone());
        visited.insert(stalled_id.clone());

        let mut stalled_entries = Vec::new();
        let mut committed_entries = Vec::new();

        while let Some(id) = queue.pop_front() {
            let Some(entry) = self.consensus_journal_repository.load(&id).await? else {
                continue;
            };

            match entry.state {
                JournalState::Applied => {}
                JournalState::Committed => {
                    // Committed but not yet Applied — include in the apply phase.
                    // Also explore its deps for more unapplied entries.
                    for dep_id in &entry.dependencies.0 {
                        if visited.insert(dep_id.clone()) {
                            queue.push_back(dep_id.clone());
                        }
                    }
                    committed_entries.push(entry);
                }
                JournalState::PreAccepted | JournalState::Accepted => {
                    // Stalled — explore its dependencies for more entries.
                    for dep_id in &entry.dependencies.0 {
                        if visited.insert(dep_id.clone()) {
                            queue.push_back(dep_id.clone());
                        }
                    }
                    stalled_entries.push(entry);
                }
            }
        }

        if stalled_entries.is_empty() && committed_entries.is_empty() {
            return Ok(());
        }

        tracing::info!(
            stalled_count = stalled_entries.len(),
            committed_count = committed_entries.len(),
            "on-demand recovery: recovering stalled dependency chain"
        );

        // Phase 1: commit all stalled (PreAccepted/Accepted) entries without applying.
        let mut recovered_commit_reqs = Vec::new();
        for entry in &stalled_entries {
            let ballot = Ballot::initial(self.node_id.clone());
            let last_applied = match self.last_applied().await {
                Ok(la) => la,
                Err(e) => {
                    tracing::warn!("failed to get last_applied: {e}");
                    continue;
                }
            };
            match self
                .recover_and_commit(
                    &entry.command_id,
                    &entry.command,
                    &entry.timestamp_zero,
                    &ballot,
                    &peers,
                    quorum,
                    &last_applied,
                )
                .await
            {
                Some(commit_req) => {
                    tracing::info!(
                        origin_node = entry.command_id.origin_node_id.as_ref(),
                        sequence = entry.command_id.sequence,
                        "on-demand recovery: stalled entry committed"
                    );
                    recovered_commit_reqs.push(commit_req);
                }
                None => {
                    tracing::warn!(
                        origin_node = entry.command_id.origin_node_id.as_ref(),
                        sequence = entry.command_id.sequence,
                        "on-demand recovery: failed to commit stalled entry"
                    );
                }
            }
        }

        // Merge: entries recovered just now + entries that were already Committed.
        // Convert Committed journal entries to CommitRequests for uniform processing.
        let mut all_to_apply: Vec<CommitRequest> = recovered_commit_reqs;
        for entry in &committed_entries {
            let timestamp = match &entry.timestamp {
                Some(ts) => ts.clone(),
                None => continue, // should not happen for Committed, skip
            };
            all_to_apply.push(CommitRequest {
                command_id: entry.command_id.clone(),
                command: entry.command.clone(),
                timestamp_zero: entry.timestamp_zero.clone(),
                timestamp,
                dependencies: entry.dependencies.clone(),
            });
        }

        // Phase 2: sort by timestamp and apply.
        all_to_apply.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        for commit_req in &all_to_apply {
            let apply_req = ApplyRequest {
                command_id: commit_req.command_id.clone(),
                command: commit_req.command.clone(),
                timestamp_zero: commit_req.timestamp_zero.clone(),
                timestamp: commit_req.timestamp.clone(),
                dependencies: commit_req.dependencies.clone(),
            };

            // Readiness check: all local deps must be Applied (or not local, or spurious).
            if !self.deps_ready(&apply_req).await {
                tracing::warn!(
                    origin_node = apply_req.command_id.origin_node_id.as_ref(),
                    sequence = apply_req.command_id.sequence,
                    "on-demand recovery: skipping entry, dependency not applied"
                );
                continue;
            }

            match self.engine.apply(&apply_req).await {
                Ok(_) => {
                    tracing::info!(
                        origin_node = apply_req.command_id.origin_node_id.as_ref(),
                        sequence = apply_req.command_id.sequence,
                        "on-demand recovery: entry applied"
                    );
                    #[allow(clippy::unnecessary_to_owned)]
                    for peer in peers.iter().cloned() {
                        let req = apply_req.clone();
                        tokio::spawn(async move {
                            let _ = peer.apply(req).await;
                        });
                    }
                }
                Err(e) => tracing::warn!(
                    origin_node = apply_req.command_id.origin_node_id.as_ref(),
                    sequence = apply_req.command_id.sequence,
                    "on-demand recovery: apply failed: {e}"
                ),
            }
        }

        Ok(())
    }

    async fn complete_from_commit(
        &self,
        commit_req: CommitRequest,
        peers: &[Arc<CPC>],
        quorum: usize,
    ) -> So3Result<CompletionMetrics> {
        const MAX_COMMIT_ATTEMPTS: u32 = 10;
        let commit_started = Instant::now();

        let key = command_object_key(&commit_req.command).clone();
        self.engine.register_committed(
            key.clone(),
            commit_req.timestamp.clone(),
            commit_req.command_id.clone(),
        );

        self.consensus_journal_repository
            .record_committed(
                &commit_req.command_id,
                &commit_req.timestamp,
                &commit_req.dependencies,
            )
            .await?;

        let mut delay_ms = 10u64;
        let mut commit_reached_quorum = false;
        let mut commit_attempts = 0u32;
        let mut final_commit_ok = 1usize;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            commit_attempts = attempt;
            let mut commit_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = commit_req.clone();
                commit_set.spawn(async move { peer.commit(req).await });
            }
            let mut commit_ok = 1usize; // self already committed
            while let Some(res) = commit_set.join_next().await {
                if res.is_ok_and(|r| r.is_ok()) {
                    commit_ok += 1;
                    if commit_ok >= quorum {
                        break; // quorum reached
                    }
                }
            }
            commit_set.abort_all();
            final_commit_ok = commit_ok;
            if commit_ok >= quorum {
                commit_reached_quorum = true;
                break;
            }
            sleep(Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(1_000);
        }
        let commit_ms = elapsed_ms(commit_started);
        if !commit_reached_quorum {
            return Err(So3Error::PeerUnavailable(format!(
                "commit quorum not reached after {MAX_COMMIT_ATTEMPTS} attempts"
            )));
        }

        let apply_req = ApplyRequest {
            command_id: commit_req.command_id.clone(),
            command: commit_req.command.clone(),
            timestamp_zero: commit_req.timestamp_zero.clone(),
            timestamp: commit_req.timestamp.clone(),
            dependencies: commit_req.dependencies.clone(),
        };

        let is_cas = matches!(commit_req.command, ObjectCommand::Cas { .. });
        if is_cas {
            let apply_started = Instant::now();
            let apply_result = self.apply_with_recovery(&apply_req).await;
            let apply_ms = elapsed_ms(apply_started);
            #[allow(clippy::unnecessary_to_owned)]
            for peer in peers.iter().cloned() {
                let req = apply_req.clone();
                tokio::spawn(async move {
                    let _ = peer.apply(req).await;
                });
            }
            let result = apply_result?;
            return Ok(CompletionMetrics {
                result,
                commit_ms,
                apply_ms,
                commit_attempts,
                commit_ok: final_commit_ok,
            });
        }

        let apply_key = key.clone();
        let is_delete = matches!(commit_req.command, ObjectCommand::Delete { .. });
        if is_delete {
            self.write_buffer.insert(
                key,
                WriteBufferEntry::Deleted {
                    timestamp: commit_req.timestamp.clone(),
                },
            );
        } else {
            let version = self.engine.peek_next_version(&apply_key).await?;
            let metadata = match &commit_req.command {
                ObjectCommand::Write {
                    key: cmd_key,
                    blob_id,
                    sha256,
                    size,
                } => ObjectMetadata {
                    key: cmd_key.clone(),
                    version,
                    blob_id: blob_id.clone(),
                    sha256: *sha256,
                    size: *size,
                    last_modified_ms: physical_millis_now(),
                    deleted: false,
                },
                _ => unreachable!(),
            };
            self.write_buffer.insert(
                key,
                WriteBufferEntry::Write {
                    timestamp: commit_req.timestamp.clone(),
                    metadata: metadata.clone(),
                },
            );
        }

        let engine = Arc::clone(&self.engine);
        let write_buffer = Arc::clone(&self.write_buffer);
        let spawn_key = apply_key.clone();
        let spawn_ts = commit_req.timestamp.clone();
        let peers_owned: Vec<Arc<CPC>> = peers.to_vec();
        tokio::spawn(async move {
            let result = engine.apply(&apply_req).await;
            if let Some(entry) = write_buffer.get(&spawn_key) {
                let is_mine = match entry.value() {
                    WriteBufferEntry::Write { timestamp, .. }
                    | WriteBufferEntry::Deleted { timestamp } => *timestamp == spawn_ts,
                };
                drop(entry);
                if is_mine {
                    write_buffer.remove(&spawn_key);
                }
            }
            if result.is_ok() {
                for peer in peers_owned {
                    let req = ApplyRequest {
                        command_id: apply_req.command_id.clone(),
                        command: apply_req.command.clone(),
                        timestamp_zero: apply_req.timestamp_zero.clone(),
                        timestamp: apply_req.timestamp.clone(),
                        dependencies: apply_req.dependencies.clone(),
                    };
                    tokio::spawn(async move {
                        let _ = peer.apply(req).await;
                    });
                }
            }
        });

        let result = if is_delete {
            CommandResult::Delete
        } else {
            match self.write_buffer.get(&apply_key) {
                Some(entry) => match entry.value() {
                    WriteBufferEntry::Write { metadata, .. } => {
                        CommandResult::Write(crate::domain::command::WriteResult {
                            metadata: metadata.clone(),
                        })
                    }
                    WriteBufferEntry::Deleted { .. } => CommandResult::Delete,
                },
                None => CommandResult::Delete,
            }
        };

        Ok(CompletionMetrics {
            result,
            commit_ms,
            apply_ms: 0,
            commit_attempts,
            commit_ok: final_commit_ok,
        })
    }

    async fn unapplied_deps_local(&self, deps: &DependencySet) -> So3Result<Vec<CommandId>> {
        let mut unapplied = Vec::new();
        for dep_id in &deps.0 {
            match self.consensus_journal_repository.load(dep_id).await? {
                Some(e) if e.state == JournalState::Applied => {}
                _ => unapplied.push(dep_id.clone()),
            }
        }
        Ok(unapplied)
    }

    async fn local_recover_success(
        &self,
        command_id: &CommandId,
        command: &ObjectCommand,
        timestamp_zero: &LogicalTimestamp,
    ) -> So3Result<RecoverSuccess> {
        let entry = self.consensus_journal_repository.load(command_id).await?;
        match entry {
            None => {
                let deps = self
                    .consensus_journal_repository
                    .check_conflicts_and_record_pre_accepted(command_id, command, timestamp_zero)
                    .await?;
                let wait_for = self.unapplied_deps_local(&deps).await?;
                Ok(RecoverSuccess {
                    local_state: JournalState::PreAccepted,
                    wait_for,
                    superseding: false,
                    dependencies: deps,
                    timestamp_zero: timestamp_zero.clone(),
                    timestamp: timestamp_zero.clone(),
                    accepted_ballot: None,
                })
            }
            Some(e) => {
                let wait_for = self.unapplied_deps_local(&e.dependencies).await?;
                let superseding = matches!(
                    e.state,
                    JournalState::Accepted | JournalState::Committed | JournalState::Applied
                );
                let accepted_ballot = if e.state == JournalState::Accepted {
                    e.ballot.clone()
                } else {
                    None
                };
                Ok(RecoverSuccess {
                    local_state: e.state,
                    wait_for,
                    superseding,
                    dependencies: e.dependencies,
                    timestamp_zero: e.timestamp_zero,
                    timestamp: e.timestamp.unwrap_or_else(|| timestamp_zero.clone()),
                    accepted_ballot,
                })
            }
        }
    }

    fn command_operation(command: &ObjectCommand) -> &'static str {
        match command {
            ObjectCommand::Read { .. } => "read",
            ObjectCommand::Write { .. } => "write",
            ObjectCommand::Cas { .. } => "cas",
            ObjectCommand::Delete { .. } => "delete",
        }
    }

    async fn wait_for_applied(&self, deps: &[CommandId]) -> So3Result<()> {
        'deps: loop {
            let notified = self.apply_notify.notified();
            for dep_id in deps {
                match self.consensus_journal_repository.load(dep_id).await? {
                    None => {}
                    Some(e) if e.state == JournalState::Applied => {}
                    Some(e) if e.state == JournalState::Committed => {
                        // Committed but not yet Applied — will be resolved by inbound apply
                        // or on-demand recovery.
                        notified.await;
                        continue 'deps;
                    }
                    Some(_) => {
                        // PreAccepted or Accepted — the coordinator never finished.
                        // Recover the stalled dependency chain on demand.
                        self.recover_and_apply_stalled_chain(dep_id).await?;
                        continue 'deps;
                    }
                }
            }
            return Ok(());
        }
    }

    /// Recovery phase 1: determine the outcome for a stalled entry and commit it
    /// locally and on peers, but do NOT apply.  Returns the `CommitRequest` needed
    /// for the subsequent apply phase, or None if recovery failed.
    async fn recover_and_commit(
        &self,
        command_id: &CommandId,
        command: &ObjectCommand,
        timestamp_zero: &LogicalTimestamp,
        ballot: &Ballot,
        peers: &[Arc<CPC>],
        quorum: usize,
        last_applied: &AppliedSet,
    ) -> Option<CommitRequest> {
        let recovery_ballot = ballot.next(self.node_id.clone());

        if self
            .consensus_journal_repository
            .record_ballot(command_id, &recovery_ballot)
            .await
            .is_err()
        {
            return None;
        }

        let Ok(local_success) = self
            .local_recover_success(command_id, command, timestamp_zero)
            .await
        else {
            return None;
        };

        let mut successes = vec![local_success];
        {
            let mut recover_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = RecoverRequest {
                    command_id: command_id.clone(),
                    ballot: recovery_ballot.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                };
                recover_set.spawn(async move { peer.recover(req).await });
            }
            while let Some(res) = recover_set.join_next().await {
                if let Ok(Ok(RecoverResponse::Success(s))) = res {
                    successes.push(s);
                }
            }
        }

        if successes.len() < quorum {
            return None;
        }

        // If any peer has already committed, use that decision directly.
        if let Some(done) = successes.iter().find(|s| {
            matches!(
                s.local_state,
                JournalState::Committed | JournalState::Applied
            )
        }) {
            let commit_req = CommitRequest {
                command_id: command_id.clone(),
                command: command.clone(),
                timestamp_zero: timestamp_zero.clone(),
                timestamp: done.timestamp.clone(),
                dependencies: done.dependencies.clone(),
            };
            return self
                .commit_locally_and_on_peers(&commit_req, peers, quorum)
                .await;
        }

        // Determine final timestamp and deps from recovery responses.
        let (final_timestamp, final_deps) = {
            let mut deps: Vec<CommandId> = vec![];
            let mut ts = timestamp_zero.clone();
            let mut best_ballot: Option<&Ballot> = None;
            for s in &successes {
                if s.superseding {
                    match (&s.accepted_ballot, &best_ballot) {
                        (Some(b), None) => {
                            best_ballot = Some(b);
                            ts = s.timestamp.clone();
                        }
                        (Some(b), Some(best)) if b > best => {
                            best_ballot = Some(b);
                            ts = s.timestamp.clone();
                        }
                        _ => {}
                    }
                }
                deps.extend(s.dependencies.0.iter().cloned());
            }
            (ts, deps)
        };

        // Accept phase with recovery ballot.
        if self
            .consensus_journal_repository
            .record_accepted(
                command_id,
                &recovery_ballot,
                &final_timestamp,
                &DependencySet(final_deps.clone()),
            )
            .await
            .is_err()
        {
            return None;
        }

        let mut accept_ok = 0usize;
        let mut refined_deps = final_deps;
        {
            let mut accept_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = AcceptRequest {
                    command_id: command_id.clone(),
                    ballot: recovery_ballot.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                    timestamp: final_timestamp.clone(),
                    dependencies: DependencySet(refined_deps.clone()),
                    last_applied: last_applied.clone(),
                };
                accept_set.spawn(async move { peer.accept(req).await });
            }
            while let Some(res) = accept_set.join_next().await {
                match res {
                    Ok(Ok(r)) if !r.nack => {
                        accept_ok += 1;
                        refined_deps.extend(r.dependencies.0);
                    }
                    _ => {}
                }
            }
        }

        if accept_ok + 1 < quorum {
            return None;
        }

        let commit_req = CommitRequest {
            command_id: command_id.clone(),
            command: command.clone(),
            timestamp_zero: timestamp_zero.clone(),
            timestamp: final_timestamp,
            dependencies: DependencySet(refined_deps),
        };
        self.commit_locally_and_on_peers(&commit_req, peers, quorum)
            .await
    }

    /// Commit a request locally (`record_committed`) and send Commit RPCs to peers.
    /// Returns `Some(commit_req)` on success, `None` on failure.
    async fn commit_locally_and_on_peers(
        &self,
        commit_req: &CommitRequest,
        peers: &[Arc<CPC>],
        quorum: usize,
    ) -> Option<CommitRequest> {
        const MAX_COMMIT_ATTEMPTS: u32 = 10;
        if self
            .consensus_journal_repository
            .record_committed(
                &commit_req.command_id,
                &commit_req.timestamp,
                &commit_req.dependencies,
            )
            .await
            .is_err()
        {
            return None;
        }

        let mut delay_ms = 10u64;
        let mut commit_ok = 1usize; // self
        for _ in 1..=MAX_COMMIT_ATTEMPTS {
            let mut commit_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = commit_req.clone();
                commit_set.spawn(async move { peer.commit(req).await });
            }
            while let Some(res) = commit_set.join_next().await {
                if res.is_ok_and(|r| r.is_ok()) {
                    commit_ok += 1;
                    if commit_ok >= quorum {
                        break;
                    }
                }
            }
            commit_set.abort_all();
            if commit_ok >= quorum {
                return Some(commit_req.clone());
            }
            sleep(Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(1_000);
        }
        None
    }

    async fn recover_and_complete(
        &self,
        command_id: &CommandId,
        command: &ObjectCommand,
        timestamp_zero: &LogicalTimestamp,
        ballot: &Ballot,
        peers: &[Arc<CPC>],
        quorum: usize,
        last_applied: &AppliedSet,
    ) -> So3Result<CommandResult> {
        let operation_started = Instant::now();
        let recovery_started = Instant::now();
        let recovery_ballot = ballot.next(self.node_id.clone());

        self.consensus_journal_repository
            .record_ballot(command_id, &recovery_ballot)
            .await?;

        // Include the coordinator's own local state — it is a replica and its journal
        // state must be merged, not merely counted toward quorum as an anonymous +1.
        let local_success = self
            .local_recover_success(command_id, command, timestamp_zero)
            .await?;

        let mut successes = vec![local_success];
        {
            let mut recover_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = RecoverRequest {
                    command_id: command_id.clone(),
                    ballot: recovery_ballot.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                };
                recover_set.spawn(async move { peer.recover(req).await });
            }
            while let Some(res) = recover_set.join_next().await {
                if let Ok(Ok(RecoverResponse::Success(s))) = res {
                    successes.push(s);
                }
            }
        }

        if successes.len() < quorum {
            return Err(So3Error::PeerUnavailable(
                "recovery quorum not reached".into(),
            ));
        }

        // Wait for all deps that peers report as unapplied before we proceed.
        // Without this, we might commit the recovered command before its deps are applied.
        let wait_for: Vec<CommandId> = successes
            .iter()
            .flat_map(|s| s.wait_for.iter().cloned())
            .collect();
        let recovery_wait_for_count = wait_for.len();
        let recovery_response_count = successes.len();
        let recovery_superseding_count = successes.iter().filter(|s| s.superseding).count();
        self.wait_for_applied(&wait_for).await?;
        let recover_ms = elapsed_ms(recovery_started);

        // If any peer has already committed, use that decision directly.
        if let Some(done) = successes.iter().find(|s| {
            matches!(
                s.local_state,
                JournalState::Committed | JournalState::Applied
            )
        }) {
            let dependency_count = done.dependencies.0.len();
            let completion = self
                .complete_from_commit(
                    CommitRequest {
                        command_id: command_id.clone(),
                        command: command.clone(),
                        timestamp_zero: timestamp_zero.clone(),
                        timestamp: done.timestamp.clone(),
                        dependencies: done.dependencies.clone(),
                    },
                    peers,
                    quorum,
                )
                .await?;
            info!(
                coordination_event = "consensus_operation",
                coordinator_node = self.node_id.as_ref(),
                origin_node = command_id.origin_node_id.as_ref(),
                operation_id_sequence = command_id.sequence,
                operation = Self::command_operation(command),
                consensus_path = "recovery",
                quorum,
                participating_replicas = peers.len() + 1,
                recovery_response_count,
                recovery_wait_for_count,
                recovery_superseding_count,
                dependency_count,
                dependency_depth = dependency_count,
                recover_ms,
                accept_ms = 0u64,
                commit_ms = completion.commit_ms,
                apply_ms = completion.apply_ms,
                total_ms = elapsed_ms(operation_started),
                quorum_wait_ms = recover_ms + completion.commit_ms,
                retry_count = completion.commit_attempts.saturating_sub(1),
                commit_attempts = completion.commit_attempts,
                commit_ok = completion.commit_ok,
                in_flight_operations = self.in_flight_operations.load(Ordering::Acquire),
                "consensus coordination"
            );
            return Ok(completion.result);
        }

        // Determine final timestamp and deps from recovery responses.
        // If any peer has Accepted state, pick the one with the highest accepted ballot —
        // that is the most recently accepted decision and must be preserved per Accord.
        // Selecting by timestamp instead can pick an older Accept from a lower ballot.
        let (final_timestamp, final_deps) = {
            let mut deps: Vec<CommandId> = vec![];
            let mut ts = timestamp_zero.clone();
            let mut best_ballot: Option<&crate::domain::consensus::ballot::Ballot> = None;
            for s in &successes {
                if s.superseding {
                    match (&s.accepted_ballot, &best_ballot) {
                        (Some(b), None) => {
                            best_ballot = Some(b);
                            ts = s.timestamp.clone();
                        }
                        (Some(b), Some(best)) if b > *best => {
                            best_ballot = Some(b);
                            ts = s.timestamp.clone();
                        }
                        // Committed/Applied have no accepted_ballot; they are handled by
                        // the early-return above so this branch is only reached for
                        // PreAccepted, which sets superseding=false.
                        _ => {}
                    }
                }
                deps.extend(s.dependencies.0.iter().cloned());
            }
            (ts, deps)
        };

        // Accept phase with recovery ballot.
        let accept_started = Instant::now();
        self.consensus_journal_repository
            .record_accepted(
                command_id,
                &recovery_ballot,
                &final_timestamp,
                &DependencySet(final_deps.clone()),
            )
            .await?;

        let mut accept_ok = 0usize;
        let mut refined_deps = final_deps.clone();
        {
            let mut accept_set = JoinSet::new();
            for peer in peers {
                let peer = Arc::clone(peer);
                let req = AcceptRequest {
                    command_id: command_id.clone(),
                    ballot: recovery_ballot.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                    timestamp: final_timestamp.clone(),
                    dependencies: DependencySet(final_deps.clone()),
                    last_applied: last_applied.clone(),
                };
                accept_set.spawn(async move { peer.accept(req).await });
            }
            while let Some(res) = accept_set.join_next().await {
                match res {
                    Ok(Ok(r)) if !r.nack => {
                        accept_ok += 1;
                        refined_deps.extend(r.dependencies.0);
                    }
                    _ => {}
                }
            }
        }

        if accept_ok + 1 < quorum {
            return Err(So3Error::PeerUnavailable(format!(
                "recovery accept quorum not reached: {}/{}",
                accept_ok + 1,
                quorum,
            )));
        }
        let accept_ms = elapsed_ms(accept_started);

        let dependency_count = refined_deps.len();
        let completion = self
            .complete_from_commit(
                CommitRequest {
                    command_id: command_id.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                    timestamp: final_timestamp,
                    dependencies: DependencySet(refined_deps),
                },
                peers,
                quorum,
            )
            .await?;
        info!(
            coordination_event = "consensus_operation",
            coordinator_node = self.node_id.as_ref(),
            origin_node = command_id.origin_node_id.as_ref(),
            operation_id_sequence = command_id.sequence,
            operation = Self::command_operation(command),
            consensus_path = "recovery",
            quorum,
            participating_replicas = peers.len() + 1,
            recovery_response_count,
            recovery_wait_for_count,
            recovery_superseding_count,
            dependency_count,
            dependency_depth = dependency_count,
            recover_ms,
            accept_ms,
            commit_ms = completion.commit_ms,
            apply_ms = completion.apply_ms,
            total_ms = elapsed_ms(operation_started),
            quorum_wait_ms = recover_ms + accept_ms + completion.commit_ms,
            retry_count = completion.commit_attempts.saturating_sub(1),
            commit_attempts = completion.commit_attempts,
            commit_ok = completion.commit_ok,
            in_flight_operations = self.in_flight_operations.load(Ordering::Acquire),
            "consensus coordination"
        );
        Ok(completion.result)
    }
}

#[async_trait]
impl<CJR, CPC, OMR> ConsensusCoordinatorService for AccordConsensusCoordinatorService<CJR, CPC, OMR>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
{
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<CommandResult> {
        let operation_started = Instant::now();
        let _in_flight_guard = InFlightOperationGuard::new(&self.in_flight_operations);
        let command_id = self.next_command_id();
        let ballot = Ballot::initial(self.node_id.clone());
        let timestamp_zero = self
            .hlc
            .lock()
            .await
            .tick(self.epoch.load(Ordering::Acquire), self.network_skew_ms);
        let last_applied = self.last_applied().await?;

        let pre_accept_started = Instant::now();
        // Coordinator is also a replica — atomically check local conflicts and record locally.
        let DependencySet(local_deps) = self
            .consensus_journal_repository
            .check_conflicts_and_record_pre_accepted(&command_id, &command, &timestamp_zero)
            .await?;

        // --- PreAccept (parallel, quorum-driven with drain for fast path) ---
        let peers: Vec<Arc<CPC>> = self.consensus_peer_client_map.values().cloned().collect();
        let quorum = self.quorum_size();
        let peers_needed = quorum.saturating_sub(1); // self already counts as 1

        let mut pre_accept_set = JoinSet::new();
        for peer in &peers {
            let peer = Arc::clone(peer);
            let req = PreAcceptRequest {
                command_id: command_id.clone(),
                command: command.clone(),
                timestamp_zero: timestamp_zero.clone(),
                last_applied: last_applied.clone(),
            };
            pre_accept_set.spawn(async move { peer.pre_accept(req).await });
        }

        let mut pre_ok: Vec<_> = vec![];
        let mut pre_failures = 0usize;
        let mut got_nack = false;

        // PreAccept: collect responses until quorum or error.
        // Fast path electorate = quorum (self + peers_needed). By the Accord intersection
        // property (2 × quorum > n) two concurrent fast-path commands on the same key
        // cannot both succeed — the intersecting replica serialises them and creates a
        // dependency that breaks the “deps empty” condition for the second one.
        while let Some(res) = pre_accept_set.join_next().await {
            match res {
                Ok(Ok(r)) if !r.nack => {
                    pre_ok.push(r);
                    if pre_ok.len() >= peers_needed {
                        break; // quorum reached
                    }
                }
                Ok(Ok(_)) => {
                    got_nack = true;
                    break;
                }
                Ok(Err(_)) | Err(_) => {
                    pre_failures += 1;
                }
            }
        }
        pre_accept_set.abort_all();

        if got_nack {
            return self
                .recover_and_complete(
                    &command_id,
                    &command,
                    &timestamp_zero,
                    &ballot,
                    &peers,
                    quorum,
                    &last_applied,
                )
                .await;
        }

        if pre_ok.len() < peers_needed {
            // The command never left PreAccepted on any peer that mattered — safe to
            // remove the local entry so it does not poison future writes to the same key.
            // Peers that did record it will have it as a harmless stalled entry that
            // self-heals on their next restart (recover_stalled_entries).
            let _ = self.consensus_journal_repository.delete(&command_id).await;
            return Err(So3Error::PeerUnavailable(format!(
                "pre-accept quorum not reached: {}/{} (failures: {})",
                pre_ok.len() + 1,
                quorum,
                pre_failures,
            )));
        }
        let pre_accept_ms = elapsed_ms(pre_accept_started);

        // Final timestamp = max of self + all responding peers.
        let final_timestamp =
            pre_ok
                .iter()
                .map(|r| &r.timestamp)
                .fold(timestamp_zero.clone(), |max, t| {
                    if t > &max { t.clone() } else { max }
                });

        // Union dependency sets from self and all responding peers.
        let mut all_deps: Vec<CommandId> = local_deps;
        for r in &pre_ok {
            all_deps.extend(r.dependencies.0.iter().cloned());
        }

        // Fast path: the quorum (self + peers_needed) agrees on the proposed timestamp
        // and reports no conflicting dependencies. This is the Accord fast-path electorate:
        // only quorum agreement is needed, not unanimous consent from all replicas.
        // Safety: the intersection property (2 × quorum > n) guarantees that if two commands
        // on the same key both claim fast path, their electorates share at least one replica
        // which serialised both PreAccepts and created a dependency for the second one.
        let quorum_agrees = pre_ok.len() >= peers_needed;
        let ts_match = final_timestamp == timestamp_zero;
        let deps_empty = all_deps.is_empty();
        if !ts_match && deps_empty && quorum_agrees {
            tracing::warn!(
                t0_physical = timestamp_zero.physical_millis,
                t0_logical = timestamp_zero.logical,
                final_physical = final_timestamp.physical_millis,
                final_logical = final_timestamp.logical,
                t0_node = %timestamp_zero.node_id.as_ref(),
                final_node = %final_timestamp.node_id.as_ref(),
                "fast path blocked by timestamp mismatch"
            );
        }
        let fast_path = ts_match && deps_empty && quorum_agrees;

        let mut accept_ok_for_log = 0usize;
        let mut accept_ms = 0u64;
        let (commit_timestamp, commit_deps) = if fast_path {
            (timestamp_zero.clone(), all_deps)
        } else {
            // --- Slow path: Accept (parallel, quorum-driven) ---
            let accept_started = Instant::now();
            self.consensus_journal_repository
                .record_accepted(
                    &command_id,
                    &ballot,
                    &final_timestamp,
                    &DependencySet(all_deps.clone()),
                )
                .await?;

            let mut accept_set = JoinSet::new();
            for peer in &peers {
                let peer = Arc::clone(peer);
                let req = AcceptRequest {
                    command_id: command_id.clone(),
                    ballot: ballot.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                    timestamp: final_timestamp.clone(),
                    dependencies: DependencySet(all_deps.clone()),
                    last_applied: last_applied.clone(),
                };
                accept_set.spawn(async move { peer.accept(req).await });
            }

            let mut accept_ok = 0usize;
            let mut refined_deps = all_deps.clone();

            while let Some(res) = accept_set.join_next().await {
                match res {
                    Ok(Ok(r)) if !r.nack => {
                        accept_ok += 1;
                        refined_deps.extend(r.dependencies.0);
                        if accept_ok + 1 >= quorum {
                            break; // quorum reached
                        }
                    }
                    Ok(Ok(_)) => {
                        // NACK → recovery
                        accept_set.abort_all();
                        return self
                            .recover_and_complete(
                                &command_id,
                                &command,
                                &timestamp_zero,
                                &ballot,
                                &peers,
                                quorum,
                                &last_applied,
                            )
                            .await;
                    }
                    Ok(Err(_)) | Err(_) => {} // RPC error — skip
                }
            }
            accept_set.abort_all();

            if accept_ok + 1 < quorum {
                return Err(So3Error::PeerUnavailable(format!(
                    "accept quorum not reached: {}/{}",
                    accept_ok + 1,
                    quorum,
                )));
            }
            accept_ok_for_log = accept_ok + 1;
            accept_ms = elapsed_ms(accept_started);

            (final_timestamp, refined_deps)
        };

        let commit_req = CommitRequest {
            command_id: command_id.clone(),
            command,
            timestamp_zero,
            timestamp: commit_timestamp,
            dependencies: DependencySet(commit_deps),
        };
        let operation = Self::command_operation(&commit_req.command);
        let dependency_count = commit_req.dependencies.0.len();
        let dependency_depth = dependency_count;
        let completion = self
            .complete_from_commit(commit_req, &peers, quorum)
            .await?;

        info!(
            coordination_event = "consensus_operation",
            coordinator_node = self.node_id.as_ref(),
            origin_node = command_id.origin_node_id.as_ref(),
            operation_id_sequence = command_id.sequence,
            operation,
            consensus_path = if fast_path { "fast" } else { "slow" },
            quorum,
            participating_replicas = self.consensus_peer_client_map.len() + 1,
            pre_accept_ok = pre_ok.len() + 1,
            pre_accept_failures = pre_failures,
            accept_ok = accept_ok_for_log,
            dependency_count,
            dependency_depth,
            pre_accept_ms,
            accept_ms,
            commit_ms = completion.commit_ms,
            apply_ms = completion.apply_ms,
            recover_ms = 0u64,
            total_ms = elapsed_ms(operation_started),
            quorum_wait_ms = pre_accept_ms + accept_ms + completion.commit_ms,
            retry_count = completion.commit_attempts.saturating_sub(1),
            commit_attempts = completion.commit_attempts,
            commit_ok = completion.commit_ok,
            in_flight_operations = self.in_flight_operations.load(Ordering::Acquire),
            "consensus coordination"
        );

        Ok(completion.result)
    }

    async fn apply(&self, req: ApplyRequest) -> So3Result<CommandResult> {
        self.engine.apply(&req).await
    }

    fn register_committed(
        &self,
        key: ObjectKey,
        timestamp: LogicalTimestamp,
        command_id: CommandId,
    ) {
        self.engine.register_committed(key, timestamp, command_id);
    }

    fn get_buffered_entry(&self, key: &ObjectKey) -> Option<BufferedEntry> {
        let entry = self.write_buffer.get(key)?;
        match entry.value() {
            WriteBufferEntry::Write { metadata, .. } => {
                Some(BufferedEntry::Write(metadata.clone()))
            }
            WriteBufferEntry::Deleted { .. } => Some(BufferedEntry::Deleted),
        }
    }
}
