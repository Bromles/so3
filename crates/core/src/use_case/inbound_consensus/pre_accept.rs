use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::transport::{PreAcceptRequest, PreAcceptResponse};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn pre_accept_internal(
        &self,
        req: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        let timestamp = self.observe(&req.timestamp_zero).await;

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
