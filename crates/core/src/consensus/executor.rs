use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::consensus::ConsensusCommandId;
use crate::consensus::clock::HybridLogicalClock;
use crate::consensus::state_machine::{LocalStateMachine, ObjectCommandExecutor};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectCommand, ObjectResult};
use crate::rpc_server::proto::{
    AcceptRequest, Ballot, CommitRequest, DependencySet, EventPayload, LastApplied,
    PreAcceptRequest,
};
use crate::rpc_server::transport::ConsensusTransportHandler;
use crate::storage::applied_command::repository::AppliedCommandStore;
use crate::storage::object::repository::ObjectRepository;

const INITIAL_COMMAND_SEQUENCE: u64 = 1;

#[async_trait]
pub trait ReplicatedCommandExecutor: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the command cannot be durably applied or replayed.
    async fn execute_replicated(
        &self,
        command_id: &ConsensusCommandId,
        command: ObjectCommand,
    ) -> So3Result<ObjectResult>;
}

#[derive(Clone)]
pub struct PersistentReplicatedCommandExecutor<R: ObjectRepository, S: AppliedCommandStore> {
    state_machine: LocalStateMachine<R>,
    applied_command_store: S,
}

impl<R: ObjectRepository, S: AppliedCommandStore> PersistentReplicatedCommandExecutor<R, S> {
    #[must_use]
    pub fn new(object_repository: R, applied_command_store: S) -> Self {
        Self {
            state_machine: LocalStateMachine::new(object_repository),
            applied_command_store,
        }
    }
}

#[async_trait]
impl<R, S> ReplicatedCommandExecutor for PersistentReplicatedCommandExecutor<R, S>
where
    R: ObjectRepository + Clone + Send + Sync,
    S: AppliedCommandStore + Clone + Send + Sync,
{
    async fn execute_replicated(
        &self,
        command_id: &ConsensusCommandId,
        command: ObjectCommand,
    ) -> So3Result<ObjectResult> {
        if let Some(result) = self.applied_command_store.load_result(command_id).await? {
            return Ok(result);
        }

        let result = self.state_machine.execute_command(command).await?;

        self.applied_command_store
            .save_result(command_id, &result)
            .await?;
        Ok(result)
    }
}

#[derive(Clone)]
pub struct LocalConsensusObjectCommandExecutor<H: ConsensusTransportHandler> {
    node_id: String,
    local_transport: H,
    next_sequence: Arc<AtomicU64>,
    clock: HybridLogicalClock,
}

impl<H: ConsensusTransportHandler> LocalConsensusObjectCommandExecutor<H> {
    #[must_use]
    pub fn new(node_id: String, local_transport: H) -> Self {
        Self::with_initial_sequence(node_id, local_transport, INITIAL_COMMAND_SEQUENCE)
    }

    #[must_use]
    pub fn with_initial_sequence(
        node_id: String,
        local_transport: H,
        initial_sequence: u64,
    ) -> Self {
        Self {
            clock: HybridLogicalClock::new(node_id.clone()),
            node_id,
            local_transport,
            next_sequence: Arc::new(AtomicU64::new(initial_sequence)),
        }
    }

    fn next_command_id(&self) -> ConsensusCommandId {
        ConsensusCommandId::new(
            self.node_id.clone(),
            self.next_sequence.fetch_add(1, Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl<H> ObjectCommandExecutor for LocalConsensusObjectCommandExecutor<H>
where
    H: ConsensusTransportHandler + Clone + Send + Sync + 'static,
{
    async fn execute_command(&self, command: ObjectCommand) -> So3Result<ObjectResult> {
        let command_id = self.next_command_id();
        let command_bytes = command.to_bytes()?;
        let timestamp_zero = self.clock.tick().await;
        let command_id = command_id_proto(&command_id);
        let event = event_payload(&command_bytes);
        let dependencies = empty_dependencies();

        let pre_accept = self
            .local_transport
            .pre_accept(PreAcceptRequest {
                command_id: Some(command_id.clone()),
                event: Some(event.clone()),
                timestamp_zero: Some(timestamp_zero.clone()),
                last_applied: Some(LastApplied {
                    commands: Vec::new(),
                }),
            })
            .await
            .map_err(|status| map_status(&status))?;
        if pre_accept.nack {
            return Err(So3Error::InvalidRequest(
                "local pre_accept rejected object command".to_owned(),
            ));
        }
        let timestamp = pre_accept
            .timestamp
            .unwrap_or_else(|| timestamp_zero.clone());

        let accept = self
            .local_transport
            .accept(AcceptRequest {
                command_id: Some(command_id.clone()),
                ballot: Some(ballot(&self.node_id)),
                event: Some(event.clone()),
                timestamp_zero: Some(timestamp_zero.clone()),
                timestamp: Some(timestamp.clone()),
                dependencies: Some(pre_accept.dependencies.unwrap_or(dependencies)),
                last_applied: Some(LastApplied {
                    commands: Vec::new(),
                }),
            })
            .await
            .map_err(|status| map_status(&status))?;
        if accept.nack {
            return Err(So3Error::InvalidRequest(
                "local accept rejected object command".to_owned(),
            ));
        }

        let commit = self
            .local_transport
            .commit(CommitRequest {
                command_id: Some(command_id),
                event: Some(event),
                timestamp_zero: Some(timestamp_zero),
                timestamp: Some(timestamp),
                dependencies: accept.dependencies,
            })
            .await
            .map_err(|status| map_status(&status))?;

        ObjectResult::from_bytes(&commit.result)
    }
}

fn command_id_proto(command_id: &ConsensusCommandId) -> crate::rpc_server::proto::CommandId {
    crate::rpc_server::proto::CommandId {
        origin_node_id: command_id.origin_node_id().to_owned(),
        sequence: command_id.sequence(),
    }
}

fn event_payload(command: &[u8]) -> EventPayload {
    EventPayload {
        command: command.to_vec(),
    }
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
    }
}

fn ballot(node_id: &str) -> Ballot {
    Ballot {
        round: 0,
        node_id: node_id.to_owned(),
    }
}

fn map_status(status: &tonic::Status) -> So3Error {
    So3Error::InvalidRequest(status.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{PersistentReplicatedCommandExecutor, ReplicatedCommandExecutor};
    use crate::consensus::ConsensusCommandId;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand, WriteCommand,
    };
    use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const COMMAND_SEQUENCE_TWO: u64 = 2;

    async fn test_executor() -> (
        PersistentReplicatedCommandExecutor<
            SqliteFsPersistentObjectRepository,
            SqliteObjectMetadataRepository,
        >,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let metadata_repository =
            SqliteObjectMetadataRepository::new(temp_dir.path().join("metadata"))
                .await
                .unwrap();
        let object_repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();

        (
            PersistentReplicatedCommandExecutor::new(object_repository, metadata_repository),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn execute_replicated_returns_stored_result_for_duplicate_command_id() {
        let (executor, _temp_dir) = test_executor().await;
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let first = executor
            .execute_replicated(&command_id, command.clone())
            .await
            .unwrap();
        let second = executor
            .execute_replicated(
                &command_id,
                ObjectCommand::Write(WriteCommand {
                    key: ObjectKey::new(ALPHA_KEY).unwrap(),
                    value: SECOND_VALUE.to_vec(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(first, second);
        let ObjectResult::Write(write) = second else {
            panic!("expected write result");
        };
        assert_eq!(write.object.record.version, ObjectVersion::initial());
        assert_eq!(write.object.value, FIRST_VALUE.to_vec());
    }

    #[tokio::test]
    async fn execute_replicated_read_observes_previous_write() {
        let (executor, _temp_dir) = test_executor().await;
        let write_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let read_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);

        let _ = executor
            .execute_replicated(
                &write_id,
                ObjectCommand::Write(WriteCommand {
                    key: ObjectKey::new(ALPHA_KEY).unwrap(),
                    value: FIRST_VALUE.to_vec(),
                }),
            )
            .await
            .unwrap();
        let result = executor
            .execute_replicated(
                &read_id,
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(ALPHA_KEY).unwrap(),
                }),
            )
            .await
            .unwrap();

        let ObjectResult::Read(read) = result else {
            panic!("expected read result");
        };
        let object = read.object.expect("expected stored object");
        assert_eq!(object.value, FIRST_VALUE.to_vec());
    }
}
