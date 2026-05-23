use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{PreAcceptRequest, PreAcceptResponse};
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
    pub(super) async fn pre_accept_internal(
        &self,
        req: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        // If this command is already accepted/committed/applied under a different
        // ballot, the coordinator must go through recovery to learn the existing decision.
        // Without this check a late-arriving PreAccept could falsely claim fast-path
        // agreement on a command that was already decided.
        if let Some(entry) = self.journal.load(&req.command_id).await?
            && matches!(
                entry.state,
                JournalState::Accepted | JournalState::Committed | JournalState::Applied
            )
        {
            return Ok(PreAcceptResponse {
                timestamp: entry
                    .timestamp
                    .unwrap_or_else(|| req.timestamp_zero.clone()),
                dependencies: entry.dependencies,
                nack: true,
            });
        }

        let timestamp = self.accept_or_observe(&req.timestamp_zero).await;

        let dependencies = self
            .journal
            .check_conflicts_and_record_pre_accepted(
                &req.command_id,
                &req.command,
                &req.timestamp_zero,
            )
            .await?;

        Ok(PreAcceptResponse {
            timestamp,
            dependencies,
            nack: false,
        })
    }
}
