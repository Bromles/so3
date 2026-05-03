use crate::client::interface::ConsensusPeerClient;
use crate::domain::clock::HybridLogicalClock;
use crate::domain::command::ObjectCommand;
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{AppliedSet, CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{AcceptRequest, CommitRequest, PreAcceptRequest};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct AccordConsensusCoordinatorService<
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
> {
    node_id: NodeId,
    epoch: AtomicU64,
    hlc: Mutex<HybridLogicalClock>,
    sequence: AtomicU64,
    network_skew_ms: u64,
    consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
    consensus_journal_repository: Arc<CJR>,
}

impl<CJR, CPC> AccordConsensusCoordinatorService<CJR, CPC>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
{
    pub async fn new(
        node_id: NodeId,
        epoch: u64,
        network_skew_ms: u64,
        consensus_peer_client_map: HashMap<NodeId, Arc<CPC>>,
        consensus_journal_repository: Arc<CJR>,
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
}

#[async_trait]
impl<CJR, CPC> ConsensusCoordinatorService for AccordConsensusCoordinatorService<CJR, CPC>
where
    CJR: ConsensusJournalRepository,
    CPC: ConsensusPeerClient,
{
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<CommandId> {
        let command_id = self.next_command_id();
        let ballot = Ballot::initial(self.node_id.clone());
        let timestamp_zero = self
            .hlc
            .lock()
            .await
            .tick(self.epoch.load(Ordering::Acquire), self.network_skew_ms);
        let last_applied = self.last_applied().await?;

        // Coordinator is also a replica — check local conflicts and record locally.
        let local_deps = self
            .consensus_journal_repository
            .check_conflicts(&command_id)
            .await?;
        self.consensus_journal_repository
            .record_pre_accepted(
                &command_id,
                &command,
                &timestamp_zero,
                &DependencySet(local_deps.clone()),
            )
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

        // Record committed locally.
        self.consensus_journal_repository
            .record_committed(&command_id)
            .await?;

        // Commit must reach a quorum before we return — this is the fix for CASSANDRA-18365.
        // Accept quorum makes the decision final, but if the coordinator crashes before Commit
        // reaches f+1 nodes, recovery quorum may not see any Committed state and will
        // incorrectly re-run Accept with different deps, violating linearizability.
        //
        // We do NOT fail on quorum miss — Accept already persisted the decision, so returning
        // an error would cause the client to retry and create a duplicate command. Instead we
        // retry Commit until quorum is reached (TODO: parallelize, add bounded back-off).
        let commit_req = CommitRequest {
            command_id: command_id.clone(),
            command,
            timestamp_zero,
            timestamp: commit_timestamp,
            dependencies: DependencySet(commit_deps),
        };

        // Bounded retry with exponential back-off capped at 1 s.
        // We never return an error here — Accept already made the decision final;
        // failing would cause the client to retry with a new CommandId (duplicate).
        // Remaining nodes will eventually be caught up by the recovery coordinator.
        const MAX_COMMIT_ATTEMPTS: u32 = 10;
        let mut delay_ms = 10u64;
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let mut commit_ok = 1usize;
            for peer in &peers {
                if peer.commit(commit_req.clone()).await.is_ok() {
                    commit_ok += 1;
                }
            }

            if commit_ok >= quorum {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;

            delay_ms = (delay_ms * 2).min(1_000);
        }

        Ok(command_id)
    }
}
