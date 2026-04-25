use async_trait::async_trait;
use tonic::Status;
use tracing::{debug, info};
use uuid::Uuid;

use crate::consensus::ConsensusCommandId;
use crate::consensus::executor::ReplicatedCommandExecutor;
use crate::consensus::journal::{JournalState, SqliteConsensusJournal};
use crate::domain::ObjectCommand;
use crate::domain::error::So3Error;
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    DependencySet, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

const MISSING_EVENT_PAYLOAD_ERROR: &str = "missing apply event payload";
const MISSING_COMMAND_ID_ERROR: &str = "missing consensus command_id";

#[derive(Clone)]
pub struct ApplyingConsensusTransport<E: ReplicatedCommandExecutor> {
    node_id: String,
    executor: E,
    journal: SqliteConsensusJournal,
}

impl<E: ReplicatedCommandExecutor> ApplyingConsensusTransport<E> {
    #[must_use]
    pub fn new(node_id: Uuid, executor: E, journal: SqliteConsensusJournal) -> Self {
        Self {
            node_id: node_id.to_string(),
            executor,
            journal,
        }
    }
}

#[async_trait]
impl<E> ConsensusTransportHandler for ApplyingConsensusTransport<E>
where
    E: ReplicatedCommandExecutor + Clone + Send + Sync + 'static,
{
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let entry = self
            .journal
            .record_pre_accepted(&command_id, command_bytes)
            .await
            .map_err(|error| map_error(&error))?;

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = journal_state_to_proto(entry.state).as_str_name(),
            event_size = command_bytes.len(),
            "recorded local pre_accept state in consensus journal"
        );

        Ok(PreAcceptResponse {
            timestamp: request.timestamp_zero,
            dependencies: Some(empty_dependencies()),
            nack: false,
        })
    }

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let entry = self
            .journal
            .record_accepted(&command_id, command_bytes)
            .await
            .map_err(|error| map_error(&error))?;

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = journal_state_to_proto(entry.state).as_str_name(),
            dependency_count = request.dependencies.as_ref().map_or(0, |dependencies| dependencies.commands.len()),
            "recorded local accept state in consensus journal"
        );

        Ok(AcceptResponse {
            dependencies: Some(request.dependencies.unwrap_or_else(empty_dependencies)),
            nack: false,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        let command_bytes = extract_command_bytes(request.event.as_ref())?;
        let entry = self
            .journal
            .record_committed(&command_id, command_bytes)
            .await
            .map_err(|error| map_error(&error))?;
        if entry.state == JournalState::Applied {
            return Ok(CommitResponse {
                result: entry.result,
            });
        }

        let command =
            ObjectCommand::from_bytes(command_bytes).map_err(|error| map_error(&error))?;
        let result = self
            .executor
            .execute_replicated(&command_id, command)
            .await
            .map_err(|error| map_error(&error))?;
        let result = result.to_bytes().map_err(|error| map_error(&error))?;
        let entry = self
            .journal
            .record_applied(&command_id, command_bytes, &result)
            .await
            .map_err(|error| map_error(&error))?;

        info!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = journal_state_to_proto(entry.state).as_str_name(),
            "executed committed command and recorded applied state in consensus journal"
        );

        Ok(CommitResponse {
            result: entry.result,
        })
    }

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status> {
        let command_id = extract_command_id(request.command_id.as_ref())?;
        if let Some(entry) = self
            .journal
            .load(&command_id)
            .await
            .map_err(|error| map_error(&error))?
        {
            return Ok(ApplyResponse {
                result: entry.result,
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
        let result = result.to_bytes().map_err(|error| map_error(&error))?;
        self.journal
            .record_applied(&command_id, command_bytes, &result)
            .await
            .map_err(|error| map_error(&error))?;

        Ok(ApplyResponse { result })
    }

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status> {
        let Some(command_id) = request.command_id.as_ref() else {
            return Ok(RecoverResponse {
                local_state: State::Undefined.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(empty_dependencies()),
                timestamp: request.timestamp_zero,
                nack: None,
            });
        };
        let command_id =
            ConsensusCommandId::try_from(command_id).map_err(|error| map_error(&error))?;
        let entry = self
            .journal
            .load(&command_id)
            .await
            .map_err(|error| map_error(&error))?;
        let local_state = entry.map_or(State::Undefined, |entry| {
            journal_state_to_proto(entry.state)
        });

        debug!(
            node_id = %self.node_id,
            command_origin = command_id.origin_node_id(),
            local_state = local_state.as_str_name(),
            "returning recover response from durable local command journal"
        );

        Ok(RecoverResponse {
            local_state: local_state.into(),
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

fn extract_command_id(
    command_id: Option<&crate::rpc_server::proto::CommandId>,
) -> Result<ConsensusCommandId, Status> {
    let command_id =
        command_id.ok_or_else(|| Status::invalid_argument(MISSING_COMMAND_ID_ERROR))?;
    ConsensusCommandId::try_from(command_id).map_err(|error| map_error(&error))
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
    }
}

fn journal_state_to_proto(state: JournalState) -> State {
    match state {
        JournalState::PreAccepted => State::PreAccepted,
        JournalState::Accepted => State::Accepted,
        JournalState::Committed => State::Committed,
        JournalState::Applied => State::Applied,
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
    use crate::consensus::executor::PersistentReplicatedCommandExecutor;
    use crate::consensus::journal::SqliteConsensusJournal;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand, WriteCommand,
    };
    use crate::rpc_server::proto::{
        AcceptRequest, ApplyRequest, CommandId, CommitRequest, EventPayload, PreAcceptRequest,
        RecoverRequest, State,
    };
    use crate::rpc_server::transport::ConsensusTransportHandler;
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const COMMAND_SEQUENCE_TWO: u64 = 2;

    async fn test_transport() -> (
        ApplyingConsensusTransport<
            PersistentReplicatedCommandExecutor<
                SqliteFsPersistentObjectRepository,
                crate::storage::metadata::sqlite::SqliteObjectMetadataRepository,
            >,
        >,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let metadata_repository =
            crate::storage::metadata::sqlite::SqliteObjectMetadataRepository::new(
                temp_dir.path().join("metadata"),
            )
            .await
            .unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path().join("consensus"))
            .await
            .unwrap();

        (
            ApplyingConsensusTransport::new(
                Uuid::nil(),
                PersistentReplicatedCommandExecutor::new(repository, metadata_repository),
                journal,
            ),
            temp_dir,
        )
    }

    fn command_id(sequence: u64) -> CommandId {
        CommandId {
            origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            sequence,
        }
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
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
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
    async fn pre_accept_and_accept_are_reflected_in_recover_state() {
        let (transport, _temp_dir) = test_transport().await;
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let _ = transport
            .pre_accept(PreAcceptRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..PreAcceptRequest::default()
            })
            .await
            .unwrap();
        let pre_accepted = transport
            .recover(RecoverRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                ..RecoverRequest::default()
            })
            .await
            .unwrap();
        let _ = transport
            .accept(AcceptRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..AcceptRequest::default()
            })
            .await
            .unwrap();
        let accepted = transport
            .recover(RecoverRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                ..RecoverRequest::default()
            })
            .await
            .unwrap();

        assert_eq!(pre_accepted.local_state, State::PreAccepted as i32);
        assert_eq!(accepted.local_state, State::Accepted as i32);
    }

    #[tokio::test]
    async fn commit_executes_command_and_reports_applied_state() {
        let (transport, _temp_dir) = test_transport().await;
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let response = transport
            .commit(CommitRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..CommitRequest::default()
            })
            .await
            .unwrap();
        let recovered = transport
            .recover(RecoverRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                ..RecoverRequest::default()
            })
            .await
            .unwrap();
        let applied = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();

        let committed_result = ObjectResult::from_bytes(&response.result).unwrap();
        let applied_result = ObjectResult::from_bytes(&applied.result).unwrap();

        assert_eq!(recovered.local_state, State::Applied as i32);
        assert_eq!(committed_result, applied_result);
    }

    #[tokio::test]
    async fn apply_rejects_missing_event_payload() {
        let (transport, _temp_dir) = test_transport().await;

        let error = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                ..ApplyRequest::default()
            })
            .await
            .unwrap_err();

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
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: write.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();
        let response = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_TWO)),
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

    #[tokio::test]
    async fn apply_is_idempotent_for_duplicate_command_id() {
        let (transport, _temp_dir) = test_transport().await;
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let first = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();
        let second = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();

        let first_result = ObjectResult::from_bytes(&first.result).unwrap();
        let second_result = ObjectResult::from_bytes(&second.result).unwrap();

        assert_eq!(first_result, second_result);
    }

    #[tokio::test]
    async fn recover_reports_applied_state_for_journaled_command() {
        let (transport, _temp_dir) = test_transport().await;
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let _ = transport
            .apply(ApplyRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                event: Some(EventPayload {
                    command: command.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            })
            .await
            .unwrap();
        let response = transport
            .recover(RecoverRequest {
                command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                ..RecoverRequest::default()
            })
            .await
            .unwrap();

        assert_eq!(response.local_state, State::Applied as i32);
    }
}
