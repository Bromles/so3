use tonic::Status;
use tracing::{info, warn};
use crate::consensus::recovery::{apply_committed_commands, wait_for_unapplied_dependencies};
use crate::domain::consensus::journal::JournalMetadata;
use crate::domain::consensus::transport::{
    CommitRequest, CommitResponse,
};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl InboundConsensusUseCaseImpl {
    pub async fn commit_internal(&self, request: CommitRequest) -> So3Result<CommitResponse> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let observed_timestamp = self
            .observe_or_tick(
                request
                    .timestamp
                    .as_ref()
                    .or(request.timestamp_zero.as_ref()),
            )
            .await;
        let committed_timestamp = request.timestamp.clone().unwrap_or(observed_timestamp);
        let dependencies = request.dependencies.unwrap_or_else(empty_dependencies);
        let entry = self
            .journal
            .record_committed_with_metadata(
                &command_id,
                command_bytes,
                JournalMetadata {
                    timestamp_zero: request.timestamp_zero.clone(),
                    timestamp: Some(committed_timestamp),
                    dependencies,
                    ballot: None,
                },
            )
            .await
            .map_err(|error| map_error(&error))?;
        if entry.state == JournalState::Applied {
            // Already applied; entry.result contains serialised ObjectResult bytes.
            return Ok(CommitResponse {
                result: entry.result.clone(),
            });
        }

        // Single best-effort apply pass. Dependency commits from concurrent coordinators may
        // still be in-flight; the coordinator retries locally after processing those messages.
        let _ = apply_committed_commands(&self.journal, &self.executor)
            .await
            .map_err(|error| map_error(&error))?;

        let entry = self
            .journal
            .load(&command_id)
            .await
            .map_err(|error| map_error(&error))?
            .ok_or_else(|| Status::internal("committed command missing after apply attempt"))?;

        if entry.state == JournalState::Applied {
            info!(
                node_id = %self.node_id,
                command_origin = command_id.origin_node_id(),
                "applied committed command after resolving dependency chain"
            );
            return Ok(CommitResponse {
                result: entry.result.clone(),
            });
        }

        let wait_for = wait_for_unapplied_dependencies(
            &self.journal,
            &entry.metadata.dependencies,
            entry.metadata.timestamp.as_ref(),
        )
            .await
            .map_err(|error| map_error(&error))?;
        warn!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            ?wait_for,
            "committed command deps not yet resolved; coordinator will retry"
        );
        // Return empty result — the coordinator will re-issue the commit (idempotent) after
        // processing any in-flight dep commits, which naturally flows through its message loop.
        Ok(CommitResponse { result: Vec::new() })
    }
}
