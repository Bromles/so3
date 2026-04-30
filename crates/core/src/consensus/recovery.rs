use crate::consensus::executor::ReplicatedCommandExecutor;
use crate::consensus::journal::{JournalMetadata, JournalState, SqliteConsensusJournal};
use crate::domain::command::ObjectCommand;
use crate::domain::consensus::clock::{timestamp_is_after, LogicalTimestamp};
use crate::domain::consensus::command_id::CommandId;
use crate::domain::error::{So3Error, So3Result};

#[derive(Debug, PartialEq, Eq)]
pub struct BlockedCommittedCommand {
    pub command_id: CommandId,
    pub wait_for: Vec<CommandId>,
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
            let wait_for = wait_for_unapplied_dependencies(
                journal,
                &entry.metadata.dependencies,
                entry.metadata.timestamp.as_ref(),
            )
                .await?;
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
///
/// When `current_timestamp` is provided, deps whose known timestamp is strictly after
/// `current_timestamp` are excluded: in Accord, a command only waits for dependencies
/// whose committed timestamp is earlier than its own, breaking circular cross-node deadlocks.
pub async fn wait_for_unapplied_dependencies(
    journal: &SqliteConsensusJournal,
    dependencies: &DependencySet,
    current_timestamp: Option<&LogicalTimestamp>,
) -> So3Result<Vec<crate::rpc_server::proto::CommandId>> {
    let mut wait_for = Vec::new();

    for dependency in &dependencies.commands {
        let command_id = CommandId::try_from(dependency)?;
        let entry = journal.load(&command_id).await?;

        if entry
            .as_ref()
            .is_some_and(|e| e.state == JournalState::Applied)
        {
            continue;
        }

        // If the dep's known timestamp is strictly after ours, the dep is not in our causal
        // past and we do not block on it (it will wait for us instead).
        if let Some(current_ts) = current_timestamp
            && let Some(dep_ts) = entry.as_ref().and_then(|e| e.metadata.timestamp.as_ref())
            && timestamp_is_after(dep_ts, current_ts)
        {
            continue;
        }

        wait_for.push(dependency.clone());
    }

    Ok(wait_for)
}

async fn apply_committed_entry<E>(
    journal: &SqliteConsensusJournal,
    executor: &E,
    command_id: &CommandId,
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
    use crate::consensus::executor::PersistentReplicatedCommandExecutor;
    use crate::consensus::journal::{JournalMetadata, JournalState, SqliteConsensusJournal};
    use crate::consensus::CommandId;
    use crate::domain::blob::BlobMetadata;
    use crate::domain::command::{ObjectCommand, WriteCommand};
    use crate::domain::consensus::clock::LogicalTimestamp;
    use crate::domain::object_key::ObjectKey;
    use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::rpc_server::proto::Ballot;

    const ALPHA_KEY: &str = "alpha";
    const BETA_KEY: &str = "beta";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const PEER_ORIGIN_NODE_ID: &str = "node-b";
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
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
            last_modified: test_last_modified(),
        });

        let _ = journal
            .record_committed(&command_id, &command.to_bytes().unwrap())
            .await
            .unwrap();
        replay_committed_commands(&journal, &executor)
            .await
            .unwrap();

        let entry = journal.load(&command_id).await.unwrap().unwrap();
        assert!(!contains_subslice(&entry.result, FIRST_VALUE));
        let result = ObjectResult::from_bytes(&entry.result).unwrap();

        assert_eq!(entry.state, JournalState::Applied);
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };
        assert_eq!(write.record.content_length, FIRST_VALUE.len() as u64);
    }

    #[tokio::test]
    async fn replay_committed_commands_is_idempotent_after_first_replay() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let command_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
            last_modified: test_last_modified(),
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
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let second_command_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);
        let first = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
            last_modified: test_last_modified(),
        });
        let second = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(SECOND_VALUE.to_vec()),
            last_modified: test_last_modified(),
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
        assert!(!contains_subslice(&second_entry.result, SECOND_VALUE));

        let result = ObjectResult::from_bytes(&second_entry.result).unwrap();
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };
        assert_eq!(write.record.content_length, SECOND_VALUE.len() as u64);
    }

    #[tokio::test]
    async fn replay_committed_commands_fails_when_dependencies_remain_unresolved() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;
        let second_command_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);
        let second = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(SECOND_VALUE.to_vec()),
            last_modified: test_last_modified(),
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

    #[tokio::test]
    async fn next_sequence_is_monotonic_after_restart_with_mixed_state_commands() {
        let temp_dir = TempDir::new().unwrap();
        let journal_path = temp_dir.path().join("metadata");

        // Populate the journal with commands from two origins in various states.
        {
            let journal = SqliteConsensusJournal::new(&journal_path).await.unwrap();
            let pre_accepted_id =
                CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
            let accepted_id =
                CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);
            let peer_committed_id =
                CommandId::new(PEER_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
            let command = ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(ALPHA_KEY).unwrap(),
                metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
                last_modified: test_last_modified(),
            });
            let bytes = command.to_bytes().unwrap();

            let _ = journal
                .record_pre_accepted(&pre_accepted_id, &bytes)
                .await
                .unwrap();
            let _ = journal.record_accepted(&accepted_id, &bytes).await.unwrap();
            let _ = journal
                .record_committed(&peer_committed_id, &bytes)
                .await
                .unwrap();
        }

        // Simulate restart: reopen the journal from the same path.
        let reopened = SqliteConsensusJournal::new(&journal_path).await.unwrap();

        let next_local = reopened
            .next_sequence_for_origin(COMMAND_ORIGIN_NODE_ID)
            .await
            .unwrap();
        let next_peer = reopened
            .next_sequence_for_origin(PEER_ORIGIN_NODE_ID)
            .await
            .unwrap();

        // next_sequence must be strictly greater than any seen sequence number.
        assert_eq!(
            next_local, 3,
            "must advance past both local commands (seq 1, 2)"
        );
        assert_eq!(next_peer, 2, "must advance past the peer command (seq 1)");
    }

    #[tokio::test]
    async fn replay_applies_cross_origin_committed_commands_in_dependency_order_after_restart() {
        let temp_dir = TempDir::new().unwrap();
        let journal_path = temp_dir.path().join("journal");
        let metadata_path = temp_dir.path().join("metadata");
        let blobs_path = temp_dir.path().join("blobs");

        let local_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let peer_id = CommandId::new(PEER_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);

        // Record committed commands: peer's command depends on local's.
        {
            let journal = SqliteConsensusJournal::new(&journal_path).await.unwrap();
            let local_cmd = ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(ALPHA_KEY).unwrap(),
                metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
                last_modified: test_last_modified(),
            });
            let peer_cmd = ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(BETA_KEY).unwrap(),
                metadata: BlobMetadata::Inline(SECOND_VALUE.to_vec()),
                last_modified: test_last_modified(),
            });
            let _ = journal
                .record_committed(&local_id, &local_cmd.to_bytes().unwrap())
                .await
                .unwrap();
            let _ = journal
                .record_committed_with_metadata(
                    &peer_id,
                    &peer_cmd.to_bytes().unwrap(),
                    JournalMetadata {
                        dependencies: DependencySet {
                            commands: vec![CommandId {
                                origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                                sequence: COMMAND_SEQUENCE_ONE,
                            }],
                        },
                        ..JournalMetadata::default()
                    },
                )
                .await
                .unwrap();
        }

        // Simulate node restart: new journal and executor instances on the same data.
        let journal = SqliteConsensusJournal::new(&journal_path).await.unwrap();
        let metadata_repo = SqliteObjectMetadataRepository::new(&metadata_path)
            .await
            .unwrap();
        let object_repo = SqliteFsPersistentObjectRepository::new(&metadata_path, &blobs_path)
            .await
            .unwrap();
        let executor = PersistentReplicatedCommandExecutor::new(object_repo.clone(), metadata_repo);

        replay_committed_commands(&journal, &executor)
            .await
            .unwrap();

        // Both commands must now be applied, peer after local.
        assert_eq!(
            journal.load(&local_id).await.unwrap().unwrap().state,
            JournalState::Applied
        );
        let peer_entry = journal.load(&peer_id).await.unwrap().unwrap();
        assert_eq!(peer_entry.state, JournalState::Applied);

        // The objects written by each command must be durably readable.
        let alpha = object_repo
            .read(&ObjectKey::new(ALPHA_KEY).unwrap())
            .await
            .unwrap();
        let beta = object_repo
            .read(&ObjectKey::new(BETA_KEY).unwrap())
            .await
            .unwrap();
        assert!(alpha.is_some(), "alpha must be readable after replay");
        assert!(beta.is_some(), "beta must be readable after replay");
    }

    #[tokio::test]
    async fn pre_accepted_and_accepted_commands_are_skipped_during_replay() {
        let (journal, executor, _temp_dir) = test_recovery_components().await;

        let pre_accepted_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let accepted_id =
            CommandId::new(PEER_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let committed_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_TWO);

        let cmd = |key: &str, value: &[u8]| {
            ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(key).unwrap(),
                metadata: BlobMetadata::Inline(value.to_vec()),
                last_modified: test_last_modified(),
            })
                .to_bytes()
                .unwrap()
        };

        let _ = journal
            .record_pre_accepted_with_metadata(
                &pre_accepted_id,
                &cmd(ALPHA_KEY, FIRST_VALUE),
                JournalMetadata {
                    ballot: Some(Ballot {
                        round: 2,
                        node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                    }),
                    timestamp: Some(LogicalTimestamp {
                        epoch: 1,
                        counter: 0,
                        node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                    }),
                    ..JournalMetadata::default()
                },
            )
            .await
            .unwrap();
        let _ = journal
            .record_accepted(&accepted_id, &cmd(BETA_KEY, SECOND_VALUE))
            .await
            .unwrap();
        // Committed command depends on both pre-accepted and accepted commands.
        let _ = journal
            .record_committed_with_metadata(
                &committed_id,
                &cmd(ALPHA_KEY, SECOND_VALUE),
                JournalMetadata {
                    dependencies: DependencySet {
                        commands: vec![
                            CommandId {
                                origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                                sequence: COMMAND_SEQUENCE_ONE,
                            },
                            CommandId {
                                origin_node_id: PEER_ORIGIN_NODE_ID.to_owned(),
                                sequence: COMMAND_SEQUENCE_ONE,
                            },
                        ],
                    },
                    ..JournalMetadata::default()
                },
            )
            .await
            .unwrap();

        let outcome = apply_committed_commands(&journal, &executor).await.unwrap();

        // Committed command is blocked: its dependencies are not yet Applied.
        assert_eq!(outcome.applied_count, 0);
        assert_eq!(outcome.blocked.len(), 1);
        assert_eq!(outcome.blocked[0].command_id, committed_id);
        assert_eq!(outcome.blocked[0].wait_for.len(), 2);

        // Pre-accepted and accepted commands must remain in their original states.
        assert_eq!(
            journal.load(&pre_accepted_id).await.unwrap().unwrap().state,
            JournalState::PreAccepted
        );
        assert_eq!(
            journal.load(&accepted_id).await.unwrap().unwrap().state,
            JournalState::Accepted
        );
        // Durable ballot and timestamp on the pre-accepted entry must be preserved.
        let pre_accepted_entry = journal.load(&pre_accepted_id).await.unwrap().unwrap();
        assert_eq!(
            pre_accepted_entry.metadata.ballot,
            Some(Ballot {
                round: 2,
                node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            })
        );
    }

    fn command_id(sequence: u64) -> CommandId {
        CommandId {
            origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            sequence,
        }
    }

    fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn test_last_modified() -> crate::domain::ObjectLastModified {
        const TEST_LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;
        crate::domain::ObjectLastModified::try_from(TEST_LAST_MODIFIED_UNIX_MILLIS).unwrap()
    }
}
