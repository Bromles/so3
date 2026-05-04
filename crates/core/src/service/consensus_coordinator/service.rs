use crate::client::interface::{BlobPeerClient, ConsensusPeerClient};
use crate::domain::clock::{HybridLogicalClock, physical_millis_now};
use crate::domain::command::{CasResult, CommandResult, ObjectCommand, ReadResult, WriteResult};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{AppliedSet, CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{
    AcceptRequest, ApplyRequest, CommitRequest, PreAcceptRequest,
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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

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
        (1 + self.consensus_peer_client_map.len()) / 2 + 1
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
    ) -> So3Result<crate::domain::blob::payload::BlobPayload> {
        for client in self.blob_peer_clients.values() {
            if let Ok(payload) = client.fetch(blob_id).await {
                return Ok(payload);
            }
        }
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available on any peer"
        )))
    }

    async fn apply_local(&self, req: &ApplyRequest) -> So3Result<CommandResult> {
        // Idempotency
        if let Some(entry) = self
            .consensus_journal_repository
            .load(&req.command_id)
            .await?
        {
            if entry.state == JournalState::Applied {
                let result = entry
                    .result
                    .ok_or_else(|| So3Error::Storage("applied entry missing result".to_string()))?;
                return Ok(result);
            }
        }

        // Wait for explicit dependencies to be applied locally.
        // TODO: add reorder-buffer + Notify here to avoid busy-poll.
        for dep_id in &req.dependencies.0 {
            match self.consensus_journal_repository.load(dep_id).await? {
                Some(e) if e.state == JournalState::Applied => {}
                _ => {
                    return Err(So3Error::PeerUnavailable(format!(
                        "dependency seq={} not yet applied locally",
                        dep_id.sequence
                    )));
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
                    let payload = self.fetch_blob_from_any_peer(blob_id).await?;
                    self.blob_repository.store(blob_id, &payload).await?;
                }
                let version = self
                    .object_metadata_repository
                    .load(key)
                    .await?
                    .map(|m| m.version.next())
                    .unwrap_or_else(ObjectVersion::initial);
                let metadata = ObjectMetadata {
                    key: key.clone(),
                    version,
                    blob_id: blob_id.clone(),
                    sha256: sha256.clone(),
                    size: *size,
                    last_modified_ms: physical_millis_now(),
                };
                self.object_metadata_repository.store(&metadata).await?;
                CommandResult::Write(WriteResult { metadata })
            }
            ObjectCommand::Delete { key } => {
                self.object_metadata_repository.delete(key).await?;
                CommandResult::Delete
            }
            ObjectCommand::Cas {
                key,
                expected_version,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repository.exists(blob_id).await? {
                    let payload = self.fetch_blob_from_any_peer(blob_id).await?;
                    self.blob_repository.store(blob_id, &payload).await?;
                }
                match self.object_metadata_repository.load(key).await? {
                    Some(meta) if meta.version == *expected_version => {
                        let new_meta = ObjectMetadata {
                            key: key.clone(),
                            version: meta.version.next(),
                            blob_id: blob_id.clone(),
                            sha256: sha256.clone(),
                            size: *size,
                            last_modified_ms: physical_millis_now(),
                        };
                        self.object_metadata_repository.store(&new_meta).await?;
                        CommandResult::Cas(CasResult::Updated(new_meta))
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

        self.consensus_journal_repository
            .record_applied(&req.command_id, &result)
            .await?;

        Ok(result)
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
        let command_id = self.next_command_id();
        let ballot = Ballot::initial(self.node_id.clone());
        let timestamp_zero = self
            .hlc
            .lock()
            .await
            .tick(self.epoch.load(Ordering::Acquire), self.network_skew_ms);
        let last_applied = self.last_applied().await?;

        // Coordinator is also a replica — atomically check local conflicts and record locally.
        let DependencySet(local_deps) = self
            .consensus_journal_repository
            .check_conflicts_and_record_pre_accepted(&command_id, &command, &timestamp_zero)
            .await?;

        // --- PreAccept (TODO: parallelize) ---
        let peers: Vec<Arc<CPC>> = self.consensus_peer_client_map.values().cloned().collect();
        let quorum = self.quorum_size();
        let peers_needed = quorum.saturating_sub(1); // self already counts as 1

        let mut pre_ok: Vec<_> = vec![];
        let mut pre_failures = 0usize;

        for peer in &peers {
            match peer
                .pre_accept(PreAcceptRequest {
                    command_id: command_id.clone(),
                    command: command.clone(),
                    timestamp_zero: timestamp_zero.clone(),
                    last_applied: last_applied.clone(),
                })
                .await
            {
                Ok(r) if !r.nack => pre_ok.push(r),
                Ok(_) => {
                    // Nack means a recovery coordinator has already superseded this ballot.
                    // TODO: retry with incremented ballot via recovery path.
                    return Err(So3Error::PeerUnavailable(
                        "pre-accept nacked: ballot superseded by recovery".to_owned(),
                    ));
                }
                Err(_) => pre_failures += 1,
            }
        }

        if pre_ok.len() < peers_needed {
            return Err(So3Error::PeerUnavailable(format!(
                "pre-accept quorum not reached: {}/{} (failures: {})",
                pre_ok.len() + 1,
                quorum,
                pre_failures,
            )));
        }

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

        // Fast path: all peers agreed on t0 with no deps.
        let fast_path =
            final_timestamp == timestamp_zero && all_deps.is_empty() && pre_failures == 0;

        let (commit_timestamp, commit_deps) = if fast_path {
            (timestamp_zero.clone(), all_deps)
        } else {
            // --- Slow path: Accept (TODO: parallelize) ---
            self.consensus_journal_repository
                .record_accepted(&command_id, &ballot, &final_timestamp)
                .await?;

            let mut accept_ok = 0usize;
            let mut refined_deps = all_deps.clone();

            for peer in &peers {
                match peer
                    .accept(AcceptRequest {
                        command_id: command_id.clone(),
                        ballot: ballot.clone(),
                        command: command.clone(),
                        timestamp_zero: timestamp_zero.clone(),
                        timestamp: final_timestamp.clone(),
                        dependencies: DependencySet(all_deps.clone()),
                        last_applied: last_applied.clone(),
                    })
                    .await
                {
                    Ok(r) if !r.nack => {
                        accept_ok += 1;
                        refined_deps.extend(r.dependencies.0);
                    }
                    Ok(_) => {
                        // TODO: retry with higher ballot via recovery path.
                        return Err(So3Error::PeerUnavailable(
                            "accept nacked: ballot superseded".to_owned(),
                        ));
                    }
                    Err(_) => {}
                }
            }

            if accept_ok + 1 < quorum {
                return Err(So3Error::PeerUnavailable(format!(
                    "accept quorum not reached: {}/{}",
                    accept_ok + 1,
                    quorum,
                )));
            }

            (final_timestamp, refined_deps)
        };

        let commit_req = CommitRequest {
            command_id: command_id.clone(),
            command,
            timestamp_zero,
            timestamp: commit_timestamp,
            dependencies: DependencySet(commit_deps),
        };

        // Record committed locally with the final timestamp and deps.
        self.consensus_journal_repository
            .record_committed(&command_id, &commit_req.timestamp, &commit_req.dependencies)
            .await?;

        // Commit must reach a quorum before applying — CASSANDRA-18365.
        const MAX_COMMIT_ATTEMPTS: u32 = 10;
        let mut delay_ms = 10u64;
        let mut commit_reached_quorum = false;
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let mut commit_ok = 1usize;
            for peer in &peers {
                if peer.commit(commit_req.clone()).await.is_ok() {
                    commit_ok += 1;
                }
            }
            if commit_ok >= quorum {
                commit_reached_quorum = true;
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(1_000);
        }

        if !commit_reached_quorum {
            return Err(So3Error::PeerUnavailable(format!(
                "commit quorum not reached after {MAX_COMMIT_ATTEMPTS} attempts"
            )));
        }

        // --- Apply ---
        // Apply locally to produce the CommandResult returned to the client.
        // Peers receive Apply fire-and-forget — they apply independently once their
        // reorder buffer and dependency checks pass.
        let apply_req = ApplyRequest {
            command_id: command_id.clone(),
            command: commit_req.command.clone(),
            timestamp_zero: commit_req.timestamp_zero.clone(),
            timestamp: commit_req.timestamp.clone(),
            dependencies: commit_req.dependencies.clone(),
        };

        let result = self.apply_local(&apply_req).await?;

        for peer in peers {
            let req = apply_req.clone();
            tokio::spawn(async move {
                let _ = peer.apply(req).await;
            });
        }

        Ok(result)
    }
}
