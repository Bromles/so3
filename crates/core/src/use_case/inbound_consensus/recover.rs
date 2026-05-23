use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{
    RecoverNack, RecoverRequest, RecoverResponse, RecoverSuccess,
};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl<CJR, CCS, BR, BPC> InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn recover_internal(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let entry = self.journal.load(&req.command_id).await?;

        if let Some(ref e) = entry
            && let Some(ref stored) = e.ballot
            && *stored > req.ballot
        {
            return Ok(RecoverResponse::Nack(RecoverNack {
                superseding_ballot: stored.clone(),
            }));
        }

        let timestamp = self.observe(&req.timestamp_zero).await;

        match entry {
            None => {
                // Never heard about this command: discover actual conflicts, record it,
                // then stamp it with the recovery ballot so no lower-ballot coordinator
                // can overwrite us.
                let deps = self
                    .journal
                    .check_conflicts_and_record_pre_accepted(
                        &req.command_id,
                        &req.command,
                        &req.timestamp_zero,
                    )
                    .await?;
                self.journal
                    .record_ballot(&req.command_id, &req.ballot)
                    .await?;
                let wait_for = self.unapplied_deps(&deps).await?;
                Ok(RecoverResponse::Success(RecoverSuccess {
                    local_state: JournalState::PreAccepted,
                    wait_for,
                    superseding: false,
                    dependencies: deps,
                    timestamp_zero: req.timestamp_zero,
                    timestamp,
                    accepted_ballot: None,
                }))
            }
            Some(e) => {
                self.journal
                    .record_ballot(&req.command_id, &req.ballot)
                    .await?;
                let wait_for = self.unapplied_deps(&e.dependencies).await?;
                // superseding = true when this replica has voted to accept a specific
                // timestamp (state >= Accepted), meaning the recovery coordinator must
                // use the slow path and honor this node's data.
                let superseding = matches!(
                    e.state,
                    JournalState::Accepted | JournalState::Committed | JournalState::Applied
                );
                // Expose the accepted ballot only for Accepted state so the recovery
                // coordinator can pick by highest ballot rather than highest timestamp.
                let accepted_ballot = if e.state == JournalState::Accepted {
                    e.ballot.clone()
                } else {
                    None
                };
                Ok(RecoverResponse::Success(RecoverSuccess {
                    local_state: e.state,
                    wait_for,
                    superseding,
                    dependencies: e.dependencies,
                    timestamp_zero: e.timestamp_zero,
                    timestamp: e.timestamp.unwrap_or(timestamp),
                    accepted_ballot,
                }))
            }
        }
    }

    async fn unapplied_deps(&self, deps: &DependencySet) -> So3Result<Vec<CommandId>> {
        let mut unapplied = Vec::new();

        for dep_id in &deps.0 {
            match self.journal.load(dep_id).await? {
                Some(e) if e.state == JournalState::Applied => {}
                _ => unapplied.push(dep_id.clone()),
            }
        }

        Ok(unapplied)
    }
}
