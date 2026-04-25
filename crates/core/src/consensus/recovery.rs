use crate::consensus::executor::ReplicatedCommandExecutor;
use crate::consensus::journal::{JournalState, SqliteConsensusJournal};
use crate::domain::ObjectCommand;
use crate::domain::error::So3Result;

/// # Errors
///
/// Returns an error when committed commands cannot be decoded, replayed, or re-journaled.
pub async fn replay_committed_commands<E>(
    journal: &SqliteConsensusJournal,
    executor: &E,
) -> So3Result<()>
where
    E: ReplicatedCommandExecutor,
{
    for entry in journal.list_by_state(JournalState::Committed).await? {
        let command = ObjectCommand::from_bytes(&entry.command)?;
        let result = executor
            .execute_replicated(&entry.command_id, command)
            .await?;
        let result = result.to_bytes()?;
        let _ = journal
            .record_applied(&entry.command_id, &entry.command, &result)
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::replay_committed_commands;
    use crate::consensus::ConsensusCommandId;
    use crate::consensus::executor::PersistentReplicatedCommandExecutor;
    use crate::consensus::journal::{JournalState, SqliteConsensusJournal};
    use crate::domain::{ObjectCommand, ObjectKey, ObjectResult, WriteCommand};
    use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;

    async fn test_recovery_components() -> (
        SqliteConsensusJournal,
        PersistentReplicatedCommandExecutor<
            SqliteFsPersistentObjectRepository,
            SqliteObjectMetadataRepository,
        >,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path().join("metadata"))
            .await
            .unwrap();
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
            journal,
            PersistentReplicatedCommandExecutor::new(object_repository, metadata_repository),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn replay_committed_commands_applies_and_rejournals_entries() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let _ = journal
            .record_committed(&command_id, &command.to_bytes().unwrap())
            .await
            .unwrap();
        replay_committed_commands(&journal, &executor)
            .await
            .unwrap();

        let entry = journal.load(&command_id).await.unwrap().unwrap();
        let result = ObjectResult::from_bytes(&entry.result).unwrap();

        assert_eq!(entry.state, JournalState::Applied);
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };
        assert_eq!(write.object.value, FIRST_VALUE.to_vec());
    }

    #[tokio::test]
    async fn replay_committed_commands_is_idempotent_after_first_replay() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });

        let _ = journal
            .record_committed(&command_id, &command.to_bytes().unwrap())
            .await
            .unwrap();
        replay_committed_commands(&journal, &executor)
            .await
            .unwrap();
        let first = journal.load(&command_id).await.unwrap().unwrap();
        replay_committed_commands(&journal, &executor)
            .await
            .unwrap();
        let second = journal.load(&command_id).await.unwrap().unwrap();

        assert_eq!(first, second);
    }
}
