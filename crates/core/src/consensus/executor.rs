use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tracing::warn;

use crate::consensus::ConsensusCommandId;
use crate::consensus::clock::HybridLogicalClock;
use crate::consensus::coordinator::{AccordCoordinator, AccordCoordinatorConfig};
use crate::consensus::state_machine::{LocalStateMachine, ObjectCommandExecutor};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectCommand, ObjectResult};
use crate::rpc_server::transport::{ConsensusTransportHandler, TonicConsensusPeerTransport};
use crate::storage::applied_command::repository::AppliedCommandStore;
use crate::storage::object::repository::ObjectRepository;

const INITIAL_COMMAND_SEQUENCE: u64 = 1;
const MAX_TRANSIENT_RETRIES: usize = 3;
const RETRY_BASE_DELAY_MS: u64 = 20;

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
    peer_ids: Vec<String>,
    peer_transport: TonicConsensusPeerTransport,
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
        Self::with_peers(
            node_id,
            local_transport,
            initial_sequence,
            Vec::new(),
            TonicConsensusPeerTransport::new(),
        )
    }

    #[must_use]
    pub fn with_peers(
        node_id: String,
        local_transport: H,
        initial_sequence: u64,
        peer_ids: Vec<String>,
        peer_transport: TonicConsensusPeerTransport,
    ) -> Self {
        Self {
            clock: HybridLogicalClock::new(node_id.clone()),
            node_id,
            local_transport,
            peer_ids,
            peer_transport,
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
        let config = AccordCoordinatorConfig {
            node_id: self.node_id.clone(),
            peer_ids: self.peer_ids.clone(),
        };

        let mut attempts: usize = 0;
        loop {
            // Each attempt gets a fresh command ID so that a previously-issued ID that may
            // have partially propagated is not confused with the retry.
            let command_id = self.next_command_id();
            let mut peer_transport = self.peer_transport.clone();
            let mut coordinator = AccordCoordinator::with_clock(
                self.clock.clone(),
                config.clone(),
                &self.local_transport,
                &mut peer_transport,
            );

            match coordinator.execute(&command_id, command.clone()).await {
                Ok(result) => return Ok(result),
                Err(So3Error::PeerUnavailable(ref msg)) if attempts < MAX_TRANSIENT_RETRIES => {
                    attempts += 1;
                    let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * (1u64 << attempts));
                    warn!(
                        attempt = attempts,
                        max = MAX_TRANSIENT_RETRIES,
                        delay_ms = delay.as_millis(),
                        reason = msg.as_str(),
                        "transient peer failure; retrying command"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{PersistentReplicatedCommandExecutor, ReplicatedCommandExecutor};
    use crate::consensus::ConsensusCommandId;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectLastModified, ObjectResult, ObjectVersion, ReadCommand,
        WriteCommand,
    };
    use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const COMMAND_SEQUENCE_TWO: u64 = 2;
    const FIRST_LAST_MODIFIED: i64 = 1_775_000_000_123;
    const SECOND_LAST_MODIFIED: i64 = 1_775_000_001_456;

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
            last_modified: last_modified(FIRST_LAST_MODIFIED),
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
                    last_modified: last_modified(SECOND_LAST_MODIFIED),
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
                    last_modified: last_modified(FIRST_LAST_MODIFIED),
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

    fn last_modified(unix_millis: i64) -> ObjectLastModified {
        ObjectLastModified::try_from(unix_millis).unwrap()
    }
}
