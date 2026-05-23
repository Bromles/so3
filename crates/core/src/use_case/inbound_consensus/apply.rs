use crate::client::interface::BlobPeerClient;
use crate::domain::command::ObjectCommand;
use crate::domain::consensus::transport::{ApplyRequest, ApplyResponse};
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
    pub(super) async fn apply_internal(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        self.ensure_blob_present(&req.command).await?;
        let result = self.coordinator.apply(req).await?;
        Ok(ApplyResponse { result })
    }

    async fn ensure_blob_present(&self, command: &ObjectCommand) -> So3Result<()> {
        let (ObjectCommand::Write { blob_id, .. } | ObjectCommand::Cas { blob_id, .. }) = command
        else {
            return Ok(());
        };
        if !self.blob_repository.exists(blob_id).await? {
            self.fetch_blob_from_any_peer(blob_id).await?;
        }
        Ok(())
    }
}
