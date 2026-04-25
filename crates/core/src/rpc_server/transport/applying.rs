use async_trait::async_trait;
use tonic::Status;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::consensus::state_machine::LocalStateMachine;
use crate::domain::ObjectCommand;
use crate::domain::error::So3Error;
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    DependencySet, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};
use crate::rpc_server::transport::ConsensusTransportHandler;
use crate::storage::object::repository::ObjectRepository;

const MISSING_EVENT_PAYLOAD_ERROR: &str = "missing apply event payload";

#[derive(Clone)]
pub struct ApplyingConsensusTransport<R: ObjectRepository> {
    node_id: String,
    state_machine: LocalStateMachine<R>,
}

impl<R: ObjectRepository> ApplyingConsensusTransport<R> {
    #[must_use]
    pub fn new(node_id: Uuid, state_machine: LocalStateMachine<R>) -> Self {
        Self {
            node_id: node_id.to_string(),
            state_machine,
        }
    }
}

#[async_trait]
impl<R> ConsensusTransportHandler for ApplyingConsensusTransport<R>
where
    R: ObjectRepository + Clone + Send + Sync + 'static,
{
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            event_size = request.event.as_ref().map_or(0, |event| event.command.len()),
            "accepting placeholder pre_accept while accord ordering is not wired"
        );

        Ok(PreAcceptResponse {
            timestamp: request.timestamp_zero,
            dependencies: Some(empty_dependencies()),
            nack: false,
        })
    }

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            dependency_count = request
                .dependencies
                .as_ref()
                .map_or(0, |dependencies| dependencies.commands.len()),
            "accepting placeholder accept while accord ordering is not wired"
        );

        Ok(AcceptResponse {
            dependencies: Some(request.dependencies.unwrap_or_else(empty_dependencies)),
            nack: false,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status> {
        warn!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            "commit reached transport before durable accord coordination was implemented"
        );

        let command = extract_command_bytes(request.event.as_ref())?;
        let _ = ObjectCommand::from_bytes(command).map_err(|error| map_error(&error))?;

        Err(Status::failed_precondition(
            "accord commit persistence is not configured",
        ))
    }

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status> {
        let command = ObjectCommand::from_bytes(extract_command_bytes(request.event.as_ref())?)
            .map_err(|error| map_error(&error))?;
        let result = self
            .state_machine
            .execute(command)
            .await
            .map_err(|error| map_error(&error))?;
        let result = result.to_bytes().map_err(|error| map_error(&error))?;

        Ok(ApplyResponse { result })
    }

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            "returning placeholder recover response while accord recovery is not wired"
        );

        Ok(RecoverResponse {
            local_state: State::Undefined.into(),
            wait_for: Vec::new(),
            superseding: false,
            dependencies: Some(empty_dependencies()),
            timestamp: request.timestamp_zero,
            nack: None,
        })
    }
}

fn extract_command_bytes(
    event: Option<&crate::rpc_server::proto::EventPayload>,
) -> Result<&[u8], Status> {
    event
        .map(|event| event.command.as_slice())
        .filter(|command| !command.is_empty())
        .ok_or_else(|| Status::invalid_argument(MISSING_EVENT_PAYLOAD_ERROR))
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
    }
}

fn map_error(error: &So3Error) -> Status {
    match error {
        So3Error::InvalidKey
        | So3Error::InvalidVersion(_)
        | So3Error::InvalidRequest(_)
        | So3Error::Serialization(_) => Status::invalid_argument(error.to_string()),
        So3Error::NotFound(_) | So3Error::CasMismatch { .. } => {
            Status::failed_precondition(error.to_string())
        }
        So3Error::Storage(_) | So3Error::Io(_) | So3Error::RpcNotImplemented => {
            Status::internal(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tonic::Code;
    use uuid::Uuid;

    use super::ApplyingConsensusTransport;
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand, WriteCommand,
    };
    use crate::rpc_server::proto::{ApplyRequest, EventPayload};
    use crate::rpc_server::transport::ConsensusTransportHandler;
    use crate::storage::object::persistent::SqliteFsObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";

    async fn test_transport() -> (
        ApplyingConsensusTransport<SqliteFsObjectRepository>,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let state_machine = LocalStateMachine::new(repository);

        (
            ApplyingConsensusTransport::new(Uuid::nil(), state_machine),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn apply_executes_serialized_write_command() {
        let (transport, _temp_dir) = test_transport().await;
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let response = transport
            .apply(ApplyRequest {
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();

        let result = ObjectResult::from_bytes(&response.result).unwrap();
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };

        assert_eq!(write.object.record.version, ObjectVersion::initial());
        assert_eq!(write.object.value, FIRST_VALUE.to_vec());
    }

    #[tokio::test]
    async fn apply_rejects_missing_event_payload() {
        let (transport, _temp_dir) = test_transport().await;

        let error = transport.apply(ApplyRequest::default()).await.unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn apply_read_observes_previously_applied_write() {
        let (transport, _temp_dir) = test_transport().await;
        let write = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });
        let read = ObjectCommand::Read(ReadCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
        });

        transport
            .apply(ApplyRequest {
                event: Some(EventPayload {
                    command: write.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();
        let response = transport
            .apply(ApplyRequest {
                event: Some(EventPayload {
                    command: read.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();

        let result = ObjectResult::from_bytes(&response.result).unwrap();
        let ObjectResult::Read(read) = result else {
            panic!("expected read result");
        };

        let object = read.object.expect("expected stored object");
        assert_eq!(object.value, FIRST_VALUE.to_vec());
    }
}
