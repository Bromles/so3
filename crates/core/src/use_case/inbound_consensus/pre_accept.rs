use tracing::debug;
use crate::domain::command::ObjectCommand;
use crate::domain::consensus::journal::JournalMetadata;
use crate::domain::consensus::transport::{PreAcceptRequest, PreAcceptResponse};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl InboundConsensusUseCaseImpl {
    pub async fn pre_accept_internal(
        &self,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let command =
            ObjectCommand::from_bytes(command_bytes).map_err(|error| map_error(&error))?;
        let timestamp = self.observe_or_tick(request.timestamp_zero.as_ref()).await;
        let dependencies = self
            .dependencies_for_unapplied_conflicts(&command_id, &command)
            .await
            .map_err(|error| map_error(&error))?;
        let entry = self
            .journal
            .record_pre_accepted_with_metadata(
                &command_id,
                command_bytes,
                JournalMetadata {
                    timestamp_zero: request.timestamp_zero.clone(),
                    timestamp: Some(timestamp.clone()),
                    dependencies: dependencies.clone(),
                    ballot: None,
                },
            )
            .await
            .map_err(|error| map_error(&error))?;

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = journal_state_to_proto(entry.state).as_str_name(),
            event_size = command_bytes.len(),
            dependency_count = dependencies.commands.len(),
            "recorded local pre_accept state in consensus journal"
        );

        Ok(PreAcceptResponse {
            timestamp: Some(timestamp),
            dependencies: Some(dependencies),
            nack: false,
        })
    }
}
