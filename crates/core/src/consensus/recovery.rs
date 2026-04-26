use crate::consensus::ConsensusCommandId;
use crate::consensus::executor::ReplicatedCommandExecutor;
use crate::consensus::journal::{JournalMetadata, JournalState, SqliteConsensusJournal};
use crate::domain::ObjectCommand;
use crate::domain::error::{So3Error, So3Result};
use crate::rpc_server::proto::DependencySet;

#[derive(Debug, PartialEq, Eq)]
pub struct BlockedCommittedCommand {
    pub command_id: ConsensusCommandId,
    pub wait_for: Vec<crate::rpc_server::proto::CommandId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ApplyCommittedCommandsOutcome {
    pub applied_count: usize,
    pub blocked: Vec<BlockedCommittedCommand>,
}

/// # Errors
///
/// Returns an error when committed commands cannot be decoded, replayed, or re-journaled.
pub async fn apply_committed_commands<E>(
    journal: &SqliteConsensusJournal,
    executor: &E,
) -> So3Result<ApplyCommittedCommandsOutcome>
where
    E: ReplicatedCommandExecutor,
{
    let mut applied_count = 0;

    loop {
        let committed = journal.list_by_state(JournalState::Committed).await?;
        if committed.is_empty() {
            return Ok(ApplyCommittedCommandsOutcome {
                applied_count,
                blocked: Vec::new(),
            });
        }

        let mut progressed = false;
        let mut blocked = Vec::new();

        for entry in committed {
            let wait_for =
                wait_for_unapplied_dependencies(journal, &entry.metadata.dependencies).await?;
            if !wait_for.is_empty() {
                blocked.push(BlockedCommittedCommand {
                    command_id: entry.command_id,
                    wait_for,
                });
                continue;
            }

            apply_committed_entry(
                journal,
                executor,
                &entry.command_id,
                &entry.command,
                &entry.metadata,
            )
            .await?;
            applied_count = applied_count.saturating_add(1);
            progressed = true;
        }

        if !progressed {
            return Ok(ApplyCommittedCommandsOutcome {
                applied_count,
                blocked,
            });
        }
    }
}

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
    let outcome = apply_committed_commands(journal, executor).await?;
    if outcome.blocked.is_empty() {
        return Ok(());
    }

    let blocked = outcome
        .blocked
        .into_iter()
        .map(|blocked| {
            format!(
                "{}:{} waiting for {:?}",
                blocked.command_id.origin_node_id(),
                blocked.command_id.sequence(),
                blocked.wait_for
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    Err(So3Error::InvalidRequest(format!(
        "recovery found committed commands with unresolved dependencies: {blocked}"
    )))
}

/// # Errors
///
/// Returns an error when the journal cannot inspect durable dependency state.
pub async fn wait_for_unapplied_dependencies(
    journal: &SqliteConsensusJournal,
    dependencies: &DependencySet,
) -> So3Result<Vec<crate::rpc_server::proto::CommandId>> {
    let mut wait_for = Vec::new();

    for dependency in &dependencies.commands {
        let command_id = ConsensusCommandId::try_from(dependency)?;
        let is_applied = journal
            .load(&command_id)
            .await?
            .is_some_and(|entry| entry.state == JournalState::Applied);
        if !is_applied {
            wait_for.push(dependency.clone());
        }
    }

    Ok(wait_for)
}

async fn apply_committed_entry<E>(
    journal: &SqliteConsensusJournal,
    executor: &E,
    command_id: &ConsensusCommandId,
    command_bytes: &[u8],
    metadata: &JournalMetadata,
) -> So3Result<()>
where
    E: ReplicatedCommandExecutor,
{
    let command = ObjectCommand::from_bytes(command_bytes)?;
    let result = executor.execute_replicated(command_id, command).await?;
    let result = result.to_bytes()?;
    let _ = journal
        .record_applied_with_metadata(command_id, command_bytes, &result, metadata.clone())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{apply_committed_commands, replay_committed_commands};
    use crate::consensus::ConsensusCommandId;
    use crate::consensus::executor::PersistentReplicatedCommandExecutor;
    use crate::consensus::journal::{JournalMetadata, JournalState, SqliteConsensusJournal};
    use crate::domain::{ObjectCommand, ObjectKey, ObjectResult, WriteCommand};
    use crate::rpc_server::proto::{CommandId, DependencySet};
    use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const COMMAND_SEQUENCE_TWO: u64 = 2;

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

    #[tokio::test]
    async fn apply_committed_commands_waits_for_unapplied_dependencies() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let first_command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let second_command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);
        let first = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });
        let second = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: SECOND_VALUE.to_vec(),
        });

        let _ = journal
            .record_committed_with_metadata(
                &second_command_id,
                &second.to_bytes().unwrap(),
                JournalMetadata {
                    dependencies: DependencySet {
                        commands: vec![command_id(COMMAND_SEQUENCE_ONE)],
                    },
                    ..JournalMetadata::default()
                },
            )
            .await
            .unwrap();
        let outcome = apply_committed_commands(&journal, &executor).await.unwrap();

        assert_eq!(outcome.applied_count, 0);
        assert_eq!(
            outcome.blocked,
            vec![super::BlockedCommittedCommand {
                command_id: second_command_id.clone(),
                wait_for: vec![command_id(COMMAND_SEQUENCE_ONE)],
            }]
        );
        assert_eq!(
            journal
                .load(&second_command_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            JournalState::Committed
        );

        let _ = journal
            .record_committed(&first_command_id, &first.to_bytes().unwrap())
            .await
            .unwrap();
        let outcome = apply_committed_commands(&journal, &executor).await.unwrap();

        assert_eq!(outcome.blocked, Vec::new());
        assert_eq!(outcome.applied_count, 2);
        assert_eq!(
            journal
                .load(&first_command_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            JournalState::Applied
        );
        let second_entry = journal.load(&second_command_id).await.unwrap().unwrap();
        assert_eq!(second_entry.state, JournalState::Applied);

        let result = ObjectResult::from_bytes(&second_entry.result).unwrap();
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };
        assert_eq!(write.object.value, SECOND_VALUE.to_vec());
    }

    #[tokio::test]
    async fn replay_committed_commands_fails_when_dependencies_remain_unresolved() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let second_command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);
        let second = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: SECOND_VALUE.to_vec(),
        });

        let _ = journal
            .record_committed_with_metadata(
                &second_command_id,
                &second.to_bytes().unwrap(),
                JournalMetadata {
                    dependencies: DependencySet {
                        commands: vec![command_id(COMMAND_SEQUENCE_ONE)],
                    },
                    ..JournalMetadata::default()
                },
            )
            .await
            .unwrap();
        let error = replay_committed_commands(&journal, &executor)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("committed commands with unresolved dependencies")
        );
    }

    fn command_id(sequence: u64) -> CommandId {
        CommandId {
            origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            sequence,
        }
    }
}
