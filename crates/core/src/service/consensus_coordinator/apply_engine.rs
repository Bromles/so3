use crate::domain::clock::{LogicalTimestamp, physical_millis_now};
use crate::domain::command::{CasResult, CommandResult, ObjectCommand, ReadResult, WriteResult};
use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::journal::JournalEntry;
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::ApplyRequest;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, Instant, timeout_at};
use tracing::info;

type ReorderBuffer = DashMap<ObjectKey, BTreeMap<LogicalTimestamp, CommandId>>;
type KeyLocks = DashMap<ObjectKey, Arc<Mutex<()>>>;

pub(crate) struct AccordApplyEngine<CJR, OMR>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
{
    reorder_buffer: ReorderBuffer,
    key_locks: KeyLocks,
    apply_notify: Arc<Notify>,
    journal: Arc<CJR>,
    metadata: Arc<OMR>,
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn command_object_key(command: &ObjectCommand) -> &ObjectKey {
    match command {
        ObjectCommand::Read { key }
        | ObjectCommand::Write { key, .. }
        | ObjectCommand::Cas { key, .. }
        | ObjectCommand::Delete { key } => key,
    }
}

impl<CJR, OMR> AccordApplyEngine<CJR, OMR>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
{
    pub fn new(journal: Arc<CJR>, metadata: Arc<OMR>, apply_notify: Arc<Notify>) -> Self {
        Self {
            reorder_buffer: DashMap::new(),
            key_locks: DashMap::new(),
            apply_notify,
            journal,
            metadata,
        }
    }

    pub async fn peek_next_version(&self, key: &ObjectKey) -> So3Result<ObjectVersion> {
        Ok(self
            .metadata
            .load(key)
            .await?
            .map_or_else(ObjectVersion::initial, |m| m.version.next()))
    }

    pub fn populate_from_journal(&self, entries: Vec<JournalEntry>) {
        for e in entries {
            if let Some(ts) = e.timestamp {
                let key = command_object_key(&e.command).clone();
                self.reorder_buffer
                    .entry(key)
                    .or_default()
                    .insert(ts, e.command_id);
            }
        }
    }

    pub fn register_committed(
        &self,
        key: ObjectKey,
        timestamp: LogicalTimestamp,
        command_id: CommandId,
    ) {
        self.reorder_buffer
            .entry(key)
            .or_default()
            .insert(timestamp, command_id);
    }

    pub async fn apply(&self, req: &ApplyRequest) -> So3Result<CommandResult> {
        let apply_started = Instant::now();

        // Idempotency; synthesize row if we missed PreAccept/Accept/Commit entirely.
        match self.journal.load(&req.command_id).await? {
            Some(e) if e.state == JournalState::Applied => {
                let result = e
                    .result
                    .ok_or_else(|| So3Error::Storage("applied entry missing result".to_string()))?;
                return Ok(result);
            }
            None => {
                self.journal
                    .check_conflicts_and_record_pre_accepted(
                        &req.command_id,
                        &req.command,
                        &req.timestamp_zero,
                    )
                    .await?;
            }
            Some(_) => {}
        }

        let key = command_object_key(&req.command);
        let deadline = Instant::now() + Duration::from_secs(30);

        let (apply_reorder_buffer_size_start, mut earlier_blocking_count) = {
            let total: usize = self.reorder_buffer.iter().map(|r| r.value().len()).sum();
            let blocking = self
                .reorder_buffer
                .get(key)
                .map_or(0, |e| e.range(..req.timestamp.clone()).count());
            (total, blocking)
        };

        let reorder_wait_started = Instant::now();
        let mut reorder_wait_iterations = 0usize;
        loop {
            let notified = self.apply_notify.notified();
            let blocking_count = self
                .reorder_buffer
                .get(key)
                .map_or(0, |e| e.range(..req.timestamp.clone()).count());
            earlier_blocking_count = earlier_blocking_count.max(blocking_count);
            if blocking_count == 0 {
                break;
            }
            reorder_wait_iterations += 1;
            timeout_at(deadline, notified).await.map_err(|_| {
                So3Error::PeerUnavailable(
                    "reorder buffer: deadline exceeded waiting for earlier committed command"
                        .into(),
                )
            })?;
        }
        let reorder_wait_ms = elapsed_ms(reorder_wait_started);

        let explicit_dependency_count = req.dependencies.0.len();
        let dependency_wait_started = Instant::now();
        let mut pending_dependency_count = 0usize;
        let mut dependency_wait_iterations = 0usize;
        loop {
            let notified = self.apply_notify.notified();
            let mut pending_count = 0usize;
            let mut first_pending_sequence = None;
            for dep_id in &req.dependencies.0 {
                match self.journal.load(dep_id).await? {
                    None => {}
                    Some(e) if e.state == JournalState::Applied => {}
                    Some(e) if e.timestamp.as_ref() > Some(&req.timestamp) => {}
                    _ => {
                        pending_count += 1;
                        first_pending_sequence.get_or_insert(dep_id.sequence);
                    }
                }
            }
            pending_dependency_count = pending_dependency_count.max(pending_count);
            match first_pending_sequence {
                None => break,
                Some(seq) => {
                    dependency_wait_iterations += 1;
                    timeout_at(deadline, notified).await.map_err(|_| {
                        So3Error::PeerUnavailable(format!(
                            "dependency seq={seq} not applied within deadline"
                        ))
                    })?;
                }
            }
        }
        let dependency_wait_ms = elapsed_ms(dependency_wait_started);

        let key = command_object_key(&req.command);
        let key_lock = self
            .key_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _key_guard = key_lock.lock().await;

        let result = match &req.command {
            ObjectCommand::Read { key } => match self.metadata.load(key).await? {
                Some(m) => CommandResult::Read(ReadResult::Found(m)),
                None => CommandResult::Read(ReadResult::NotFound),
            },
            ObjectCommand::Write {
                key,
                blob_id,
                sha256,
                size,
            } => {
                let version = self
                    .metadata
                    .load(key)
                    .await?
                    .map_or_else(ObjectVersion::initial, |m| m.version.next());
                CommandResult::Write(WriteResult {
                    metadata: ObjectMetadata {
                        key: key.clone(),
                        version,
                        blob_id: blob_id.clone(),
                        sha256: *sha256,
                        size: *size,
                        last_modified_ms: physical_millis_now(),
                        deleted: false,
                    },
                })
            }
            ObjectCommand::Delete { .. } => CommandResult::Delete,
            ObjectCommand::Cas {
                key,
                expected_version,
                blob_id,
                sha256,
                size,
            } => match self.metadata.load(key).await? {
                Some(meta) if meta.version == *expected_version => {
                    CommandResult::Cas(CasResult::Updated(ObjectMetadata {
                        key: key.clone(),
                        version: meta.version.next(),
                        blob_id: blob_id.clone(),
                        sha256: *sha256,
                        size: *size,
                        last_modified_ms: physical_millis_now(),
                        deleted: false,
                    }))
                }
                Some(meta) => CommandResult::Cas(CasResult::Conflict {
                    current_version: meta.version,
                }),
                None if expected_version == &ObjectVersion::initial() => {
                    CommandResult::Cas(CasResult::Updated(ObjectMetadata {
                        key: key.clone(),
                        version: ObjectVersion::initial().next(),
                        blob_id: blob_id.clone(),
                        sha256: *sha256,
                        size: *size,
                        last_modified_ms: physical_millis_now(),
                        deleted: false,
                    }))
                }
                None => CommandResult::Cas(CasResult::Conflict {
                    current_version: ObjectVersion::initial(),
                }),
            },
        };

        let journal_apply_started = Instant::now();
        self.journal
            .record_applied(&req.command_id, &result)
            .await?;
        let journal_apply_ms = elapsed_ms(journal_apply_started);

        let metadata_apply_started = Instant::now();
        match (&req.command, &result) {
            (
                ObjectCommand::Write { .. } | ObjectCommand::Cas { .. },
                CommandResult::Write(WriteResult { metadata })
                | CommandResult::Cas(CasResult::Updated(metadata)),
            ) => {
                self.metadata.store(metadata).await?;
            }
            (ObjectCommand::Delete { key }, CommandResult::Delete) => {
                self.metadata.delete(key).await?;
            }
            _ => {}
        }
        let metadata_apply_ms = elapsed_ms(metadata_apply_started);

        let apply_reorder_buffer_size_end = {
            if let Some(mut entry) = self.reorder_buffer.get_mut(key) {
                entry.remove(&req.timestamp);
            }
            self.reorder_buffer
                .iter()
                .map(|r| r.value().len())
                .sum::<usize>()
        };
        self.apply_notify.notify_waiters();

        info!(
            coordination_event = "apply_backlog",
            backlog_event = "apply_engine",
            origin_node = req.command_id.origin_node_id.as_ref(),
            operation_id_sequence = req.command_id.sequence,
            apply_reorder_buffer_size_start,
            apply_reorder_buffer_size_end,
            earlier_blocking_count,
            explicit_dependency_count,
            pending_dependency_count,
            reorder_wait_iterations,
            dependency_wait_iterations,
            reorder_wait_ms,
            dependency_wait_ms,
            journal_apply_ms,
            metadata_apply_ms,
            apply_total_ms = elapsed_ms(apply_started),
            "apply engine"
        );

        Ok(result)
    }
}
