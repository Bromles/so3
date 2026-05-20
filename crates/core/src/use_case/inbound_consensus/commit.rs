use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::transport::{CommitRequest, CommitResponse};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use tracing::info;

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

        let commit_dependency_count = req.dependencies.0.len();
        let operation = Self::command_operation(&req.command);
        let origin_node = req.command_id.origin_node_id.clone();
        let operation_id_sequence = req.command_id.sequence;
        let commit_reorder_buffer_size = {
            let mut buffer = self.reorder_buffer.lock().await;
            let key = Self::command_object_key(&req.command).clone();
            buffer
                .entry(key)
                .or_default()
                .insert(req.timestamp, req.command_id);
            buffer.values().map(|m| m.len()).sum::<usize>()
        };

        info!(
            coordination_event = "apply_backlog",
            backlog_event = "commit",
            node = self.node_id.as_ref(),
            origin_node = origin_node.as_ref(),
            operation_id_sequence,
            operation,
            commit_dependency_count,
            commit_reorder_buffer_size,
            "inbound apply backlog"
        );

        Ok(CommitResponse)
    }
}
