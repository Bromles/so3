use tracing::debug;
use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::transport::{
    RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl InboundConsensusUseCaseImpl {
    pub async fn recover_internal(&self, request: RecoverRequest) -> So3Result<RecoverResponse> {
        let timestamp = self.observe_or_tick(request.timestamp_zero.as_ref()).await;
        let Some(command_id) = request.command_id.as_ref() else {
            return Ok(RecoverResponse {
                local_state: State::Undefined.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(empty_dependencies()),
                timestamp: Some(timestamp),
                nack: None,
            });
        };
        let command_id =
            CommandId::try_from(command_id).map_err(|error| map_error(&error))?;
        let entry = self
            .journal
            .load(&command_id)
            .await
            .map_err(|error| map_error(&error))?;
        if let Some(nack) = recover_nack(entry.as_ref(), request.ballot.as_ref()) {
            return Ok(RecoverResponse {
                local_state: State::Undefined.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(empty_dependencies()),
                timestamp: Some(timestamp),
                nack: Some(nack),
            });
        }
        let (local_state, dependencies, response_timestamp) = entry.map_or(
            (State::Undefined, empty_dependencies(), timestamp.clone()),
            |entry| {
                (
                    journal_state_to_proto(entry.state),
                    entry.metadata.dependencies,
                    entry
                        .metadata
                        .timestamp
                        .unwrap_or_else(|| timestamp.clone()),
                )
            },
        );
        let wait_for = wait_for_unapplied_dependencies(
            &self.journal,
            &dependencies,
            Some(&response_timestamp),
        )
            .await
            .map_err(|error| map_error(&error))?;

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = local_state.as_str_name(),
            "returning recover response from durable local command journal"
        );

        Ok(RecoverResponse {
            local_state: local_state.into(),
            wait_for,
            superseding: false,
            dependencies: Some(dependencies),
            timestamp: Some(response_timestamp),
            nack: None,
        })
    }
}
