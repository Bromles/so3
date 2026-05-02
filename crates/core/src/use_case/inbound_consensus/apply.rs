use crate::domain::command::ObjectCommand;
use crate::domain::consensus::journal::{JournalMetadata, JournalState};
use crate::domain::consensus::transport::{
    ApplyRequest, ApplyResponse,
};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl InboundConsensusUseCaseImpl {
    pub async fn apply_internal(&self, request: ApplyRequest) -> So3Result<ApplyResponse> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        if let Some(entry) = self
            .journal
            .load(&command_id)
            .await
            .map_err(|error| map_error(&error))?
            .filter(|entry| entry.state == JournalState::Applied)
        {
            // Already applied; entry.result contains serialised ObjectResult bytes.
            return Ok(ApplyResponse {
                result: entry.result.clone(),
            });
        }

        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let command =
            ObjectCommand::from_bytes(command_bytes).map_err(|error| map_error(&error))?;
        let result = self
            .executor
            .execute_replicated(&command_id, command)
            .await
            .map_err(|error| map_error(&error))?;
        let result_bytes = result.to_bytes().map_err(|error| map_error(&error))?;
        let _ = self
            .journal
            .record_applied_with_metadata(
                &command_id,
                command_bytes,
                &result_bytes,
                JournalMetadata {
                    timestamp_zero: request.timestamp_zero,
                    timestamp: request.timestamp,
                    dependencies: request.dependencies.unwrap_or_else(empty_dependencies),
                    ballot: None,
                },
            )
            .await
            .map_err(|error| map_error(&error))?;

        Ok(ApplyResponse {
            result: result_bytes,
        })
    }
}
