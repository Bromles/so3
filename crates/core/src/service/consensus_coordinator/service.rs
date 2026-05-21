use crate::client::interface::{BlobPeerClient, ConsensusPeerClient};
use crate::domain::clock::{physical_millis_now, HybridLogicalClock, LogicalTimestamp};
use crate::domain::command::{CasResult, CommandResult, ObjectCommand, ReadResult, WriteResult};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{AppliedSet, CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{
    AcceptRequest, ApplyRequest, CommitRequest, PreAcceptRequest, RecoverRequest, RecoverResponse,
    RecoverSuccess,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout_at, Duration, Instant};
use tracing::info;

pub struct AccordConsensusCoordinatorService<CJR, CPC, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    node_id: NodeId,
    epoch: AtomicU64,
    hlc: Mutex<HybridLogicalClock>,
    sequence: AtomicU64,
    network_skew_ms: u64,
    consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
    consensus_journal_repository: Arc<CJR>,
    object_metadata_repository: Arc<OMR>,
    blob_repository: Arc<BR>,
    blob_peer_clients: HashMap<NodeId, Arc<BPC>>,
    apply_notify: Arc<Notify>,
    in_flight_operations: AtomicU64,
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

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl<CJR, CPC, OMR, BR, BPC> AccordConsensusCoordinatorService<CJR, CPC, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub async fn new(
        node_id: NodeId,
        epoch: u64,
        network_skew_ms: u64,
        consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
        consensus_journal_repository: Arc<CJR>,
        object_metadata_repository: Arc<OMR>,
        blob_repository: Arc<BR>,
        blob_peer_clients: HashMap<NodeId, Arc<BPC>>,
        apply_notify: Arc<Notify>,
    ) -> So3Result<Self> {
        let initial_sequence = consensus_journal_repository
            .max_sequence(&node_id)
            .await?
            .saturating_add(1);
        let hlc = HybridLogicalClock::new(node_id.clone());

        Ok(Self {
            node_id,
            epoch: AtomicU64::new(epoch),
            hlc: Mutex::new(hlc),
            sequence: AtomicU64::new(initial_sequence),
            network_skew_ms,
            consensus_peer_client_map,
            consensus_journal_repository,
            object_metadata_repository,
            blob_repository,
            blob_peer_clients,
            apply_notify,
            in_flight_operations: AtomicU64::new(0),
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

    async fn fetch_blob_from_any_peer(
        &self,
        blob_id: &crate::domain::blob::id::BlobId,
    ) -> So3Result<()> {
        use tokio_stream::StreamExt;
        for client in self.blob_peer_clients.values() {
            if let Ok(mut stream) = client.fetch(blob_id).await {
                let mut failed = false;
                while let Some(chunk) = stream.next().await {
                    if let Ok(c) = chunk {
                        if self.blob_repository.append_chunk(blob_id, c).await.is_err() {
                            failed = true;
                            break;
                        }
                    } else {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    let _ = self.blob_repository.abort(blob_id).await;
                    continue;
                }
                return self.blob_repository.commit(blob_id).await;
            }
        }
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available on any peer"
        )))
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
                .recover_and_complete(
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
                Ok(_) => tracing::info!(
                    origin_node = entry.command_id.origin_node_id.as_ref(),
                    sequence = entry.command_id.sequence,
                    "stalled entry recovered"
                ),
                Err(e) => tracing::warn!(
                    origin_node = entry.command_id.origin_node_id.as_ref(),
                    sequence = entry.command_id.sequence,
                    "stalled entry recovery failed: {e}"
                ),
            }
        }
    }

    async fn apply_local(&self, req: &ApplyRequest) -> So3Result<CommandResult> {
        // Idempotency
        if let Some(entry) = self
            .consensus_journal_repository
            .load(&req.command_id)
            .await?
            && entry.state == JournalState::Applied
        {
            let result = entry
                .result
                .ok_or_else(|| So3Error::Storage("applied entry missing result".to_string()))?;
            return Ok(result);
        }

        // Wait for all explicit dependencies to be applied locally.
        // Register the Notify future before checking state to avoid the TOCTOU where a dep
        // gets applied between the check and the await.
        let dep_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let notified = self.apply_notify.notified();
            let mut pending = None;
            for dep_id in &req.dependencies.0 {
                match self.consensus_journal_repository.load(dep_id).await? {
                    Some(e) if e.state == JournalState::Applied => {}
                    Some(e) if e.timestamp.as_ref() > Some(&req.timestamp) => {
                        // Spurious dependency from concurrent PreAccept: the dep has a
                        // later committed timestamp, so it will apply after us anyway.
                        continue;
                    }
                    _ => {
                        pending = Some(dep_id.sequence);
                        break;
                    }
                }
            }
            match pending {
                None => break,
                Some(seq) => {
                    timeout_at(dep_deadline, notified).await.map_err(|_| {
                        So3Error::PeerUnavailable(format!(
                            "dependency seq={seq} not applied within deadline"
                        ))
                    })?;
                }
            }
        }

        let result = match &req.command {
            ObjectCommand::Read { key } => match self.object_metadata_repository.load(key).await? {
                Some(m) => CommandResult::Read(ReadResult::Found(m)),
                None => CommandResult::Read(ReadResult::NotFound),
            },
            ObjectCommand::Write {
                key,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repository.exists(blob_id).await? {
                    self.fetch_blob_from_any_peer(blob_id).await?;
                }
                let version = self
                    .object_metadata_repository
                    .load(key)
                    .await?
                    .map_or_else(ObjectVersion::initial, |m| m.version.next());
                CommandResult::Write(WriteResult {
                    metadata: ObjectMetadata {
                        key: key.clone(),
                        version,
                        blob_id: blob_id.clone(),
                        sha256: sha256.clone(),
                        size: *size,
                        last_modified_ms: physical_millis_now(),
                    },
                })
            }
            ObjectCommand::Delete { .. } => CommandResult::Delete,
            ObjectCommand::Cas {
                key,
                expected_version,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repository.exists(blob_id).await? {
                    self.fetch_blob_from_any_peer(blob_id).await?;
                }
                match self.object_metadata_repository.load(key).await? {
                    Some(meta) if meta.version == *expected_version => {
                        CommandResult::Cas(CasResult::Updated(ObjectMetadata {
                            key: key.clone(),
                            version: meta.version.next(),
                            blob_id: blob_id.clone(),
                            sha256: sha256.clone(),
                            size: *size,
                            last_modified_ms: physical_millis_now(),
                        }))
                    }
                    Some(meta) => CommandResult::Cas(CasResult::Conflict {
                        current_version: meta.version,
                    }),
                    None => CommandResult::Cas(CasResult::Conflict {
                        current_version: ObjectVersion::initial(),
                    }),
                }
            }
        };

        // Journal-first: persist the result before mutating object metadata.
        self.consensus_journal_repository
            .record_applied(&req.command_id, &result)
            .await?;

        // Apply object metadata side effects.
        match (&req.command, &result) {
            (ObjectCommand::Write { .. }, CommandResult::Write(WriteResult { metadata })) => {
                self.object_metadata_repository.store(metadata).await?;
            }
            (ObjectCommand::Delete { key }, CommandResult::Delete) => {
                self.object_metadata_repository.delete(key).await?;
            }
            (ObjectCommand::Cas { .. }, CommandResult::Cas(CasResult::Updated(metadata))) => {
                self.object_metadata_repository.store(metadata).await?;
            }
            _ => {}
        }

        self.apply_notify.notify_waiters();

        Ok(result)
    }

    async fn complete_from_commit(
        &self,
        commit_req: CommitRequest,
        peers: &[Arc<CPC>],
        quorum: usize,
    ) -> So3Result<CompletionMetrics> {
        let commit_started = Instant::now();
        self.consensus_journal_repository
            .record_committed(
                &commit_req.command_id,
                &commit_req.timestamp,
                &commit_req.dependencies,
            )
            .await?;

        const MAX_COMMIT_ATTEMPTS: u32 = 10;
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
        let apply_started = Instant::now();
        let result = self.apply_local(&apply_req).await?;
        let apply_ms = elapsed_ms(apply_started);
        for peer in peers.iter().cloned() {
            let req = apply_req.clone();
            tokio::spawn(async move {
                let _ = peer.apply(req).await;
            });
        }
        Ok(CompletionMetrics {
            result,
            commit_ms,
            apply_ms,
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
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let notified = self.apply_notify.notified();
            let mut pending = None;
            for dep_id in deps {
                match self.consensus_journal_repository.load(dep_id).await? {
                    None => continue, // not local — resolved independently on owning peer
                    Some(e) if e.state == JournalState::Applied => {}
                    _ => {
                        pending = Some(dep_id.sequence);
                        break;
                    }
                }
            }
            match pending {
                None => return Ok(()),
                Some(seq) => {
                    timeout_at(deadline, notified).await.map_err(|_| {
                        So3Error::PeerUnavailable(format!(
                            "recovery wait_for dep seq={seq} not applied within deadline"
                        ))
                    })?;
                }
            }
        }
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
impl<CJR, CPC, OMR, BR, BPC> ConsensusCoordinatorService
    for AccordConsensusCoordinatorService<CJR, CPC, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
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
}
