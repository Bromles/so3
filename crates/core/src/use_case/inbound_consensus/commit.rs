use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::transport::{CommitRequest, CommitResponse};
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
    pub(super) async fn commit_internal(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        self.observe(&req.timestamp).await;

        // Synthesize a journal row if we missed PreAccept/Accept entirely.
        if self.journal.load(&req.command_id).await?.is_none() {
            self.journal
                .check_conflicts_and_record_pre_accepted(
                    &req.command_id,
                    &req.command,
                    &req.timestamp_zero,
                )
                .await?;
        }

        self.journal
            .record_committed(&req.command_id, &req.timestamp, &req.dependencies)
            .await?;

        self.reorder_buffer
            .lock()
            .await
            .insert(req.timestamp, req.command_id);

        Ok(CommitResponse)
    }
}
