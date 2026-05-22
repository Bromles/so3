use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::transport::{CommitRequest, CommitResponse};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use tracing::info;

impl<CJR, CCS, BR, BPC> InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn commit_internal(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        self.observe(&req.timestamp).await;

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

        let commit_dependency_count = req.dependencies.0.len();
        let operation = Self::command_operation(&req.command);
        let origin_node = req.command_id.origin_node_id.clone();
        let operation_id_sequence = req.command_id.sequence;
        let key = Self::command_object_key(&req.command).clone();
        self.coordinator
            .register_committed(key, req.timestamp, req.command_id);

        info!(
            coordination_event = "apply_backlog",
            backlog_event = "commit",
            node = self.node_id.as_ref(),
            origin_node = origin_node.as_ref(),
            operation_id_sequence,
            operation,
            commit_dependency_count,
            "inbound commit"
        );

        Ok(CommitResponse)
    }
}
