use async_trait::async_trait;

use crate::consensus::ConsensusCommandId;
use crate::consensus::state_machine::{LocalStateMachine, ObjectCommandExecutor};
use crate::domain::error::So3Result;
use crate::domain::{ObjectCommand, ObjectResult};
use crate::storage::applied_command::repository::AppliedCommandStore;
use crate::storage::object::repository::ObjectRepository;

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
