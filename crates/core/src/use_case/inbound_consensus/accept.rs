use crate::domain::consensus::journal::JournalMetadata;
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse,
};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use tracing::debug;

impl InboundConsensusUseCaseImpl {
    pub async fn accept_internal(&self, request: AcceptRequest) -> So3Result<AcceptResponse> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        if let Some(response) = self
            .reject_stale_accept(&command_id, request.ballot.as_ref())
            .await?
        {
            return Ok(response);
        }
        let observed_timestamp = self
            .observe_or_tick(
                request
                    .timestamp
                    .as_ref()
                    .or(request.timestamp_zero.as_ref()),
            )
            .await;
        let accepted_timestamp = request.timestamp.clone().unwrap_or(observed_timestamp);
        let dependencies = request.dependencies.unwrap_or_else(empty_dependencies);
        let entry = self
            .journal
            .record_accepted_with_metadata(
                &command_id,
                command_bytes,
                JournalMetadata {
                    timestamp_zero: request.timestamp_zero.clone(),
                    timestamp: Some(accepted_timestamp),
                    dependencies: dependencies.clone(),
                    ballot: request.ballot.clone(),
                },
            )
            .await
            .map_err(|error| map_error(&error))?;

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = journal_state_to_proto(entry.state).as_str_name(),
            dependency_count = dependencies.commands.len(),
            "recorded local accept state in consensus journal"
        );

        Ok(AcceptResponse {
            dependencies: Some(dependencies),
            nack: false,
        })
    }
}
