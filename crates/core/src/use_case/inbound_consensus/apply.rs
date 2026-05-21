use crate::client::interface::BlobPeerClient;
use crate::domain::clock::physical_millis_now;
use crate::domain::command::{CasResult, CommandResult, ObjectCommand, ReadResult, WriteResult};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{ApplyRequest, ApplyResponse};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use tokio::time::{timeout_at, Duration, Instant};
use tracing::info;

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn apply_internal(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        let apply_started = Instant::now();
        // Idempotency; synthesize row if we missed PreAccept/Accept/Commit entirely.
        match self.journal.load(&req.command_id).await? {
            Some(e) if e.state == JournalState::Applied => {
                let result = e
                    .result
                    .ok_or_else(|| So3Error::Storage("applied entry missing result".to_string()))?;
                return Ok(ApplyResponse { result });
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

        // Reorder buffer: wait until all committed commands with a strictly earlier timestamp
        // have been applied. We register the Notified future *before* checking the buffer to
        // avoid the TOCTOU race where an entry is removed between the check and the await.
        // The deadline is a single shared budget across all iterations of the wait loop.
        let deadline = Instant::now() + Duration::from_secs(30);
        let (apply_reorder_buffer_size_start, mut earlier_blocking_count) = {
            let buf = self.reorder_buffer.lock().await;
            let total: usize = buf.values().map(|m| m.len()).sum::<usize>();
            let key = Self::command_object_key(&req.command);
            let blocking = buf
                .get(key)
                .map(|m| m.range(..req.timestamp.clone()).count())
                .unwrap_or(0);
            (total, blocking)
        };
        let reorder_wait_started = Instant::now();
        let mut reorder_wait_iterations = 0usize;
        loop {
            let notified = self.apply_notify.notified();
            let blocking_count = {
                let buf = self.reorder_buffer.lock().await;
                let key = Self::command_object_key(&req.command);
                buf.get(key)
                    .map(|m| m.range(..req.timestamp.clone()).count())
                    .unwrap_or(0)
            };
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

        // Wait for explicit dependencies to be applied.
        // Register Notify before checking to avoid the TOCTOU where a dep is applied
        // between the check and the await. Reuses the same deadline as the reorder buffer.
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
                    None => {
                        // Dep not in local journal — it was introduced by a peer's
                        // conflict check and lives on that peer.  We cannot track its
                        // progress here, and it will be applied on the owning peer
                        // independently, so skip it.
                        continue;
                    }
                    Some(e) if e.state == JournalState::Applied => {}
                    Some(e) if e.timestamp.as_ref() > Some(&req.timestamp) => {
                        // Spurious dependency from concurrent PreAccept: the dep has a
                        // later committed timestamp, so the reorder buffer already ensures
                        // we apply before it. No need to wait.
                        continue;
                    }
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

        // Compute result — blob I/O is safe here (idempotent), but object metadata is NOT
        // mutated yet. Separating computation from persistence lets us journal-first below.
        let result = match &req.command {
            ObjectCommand::Read { key } => match self.object_metadata_repository.load(key).await? {
                Some(m) => CommandResult::Read(ReadResult::Found(m)),
                None => CommandResult::Read(ReadResult::NotFound),
            },
            ObjectCommand::Write {
                key,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repository.exists(blob_id).await? {
                    self.fetch_blob_from_any_peer(blob_id).await?;
                }
                let version = self
                    .object_metadata_repository
                    .load(key)
                    .await?
                    .map(|m| m.version.next())
                    .unwrap_or_else(ObjectVersion::initial);
                CommandResult::Write(WriteResult {
                    metadata: ObjectMetadata {
                        key: key.clone(),
                        version,
                        blob_id: blob_id.clone(),
                        sha256: sha256.clone(),
                        size: *size,
                        last_modified_ms: physical_millis_now(),
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
            } => {
                if !self.blob_repository.exists(blob_id).await? {
                    self.fetch_blob_from_any_peer(blob_id).await?;
                }
                match self.object_metadata_repository.load(key).await? {
                    Some(meta) if meta.version == *expected_version => {
                        CommandResult::Cas(CasResult::Updated(ObjectMetadata {
                            key: key.clone(),
                            version: meta.version.next(),
                            blob_id: blob_id.clone(),
                            sha256: sha256.clone(),
                            size: *size,
                            last_modified_ms: physical_millis_now(),
                        }))
                    }
                    Some(meta) => CommandResult::Cas(CasResult::Conflict {
                        current_version: meta.version,
                    }),
                    None => CommandResult::Cas(CasResult::Conflict {
                        current_version: ObjectVersion::initial(),
                    }),
                }
            }
        };

        // Journal-first: persist the result before mutating object metadata.
        // On a crash after this line the idempotency check above returns the stored result;
        // the startup reconciliation pass in Node::new re-applies any missing metadata changes.
        let journal_apply_started = Instant::now();
        self.journal
            .record_applied(&req.command_id, &result)
            .await?;
        let journal_apply_ms = elapsed_ms(journal_apply_started);

        // Apply object metadata side effects.
        let metadata_apply_started = Instant::now();
        match (&req.command, &result) {
            (ObjectCommand::Write { .. }, CommandResult::Write(WriteResult { metadata })) => {
                self.object_metadata_repository.store(metadata).await?;
            }
            (ObjectCommand::Delete { key }, CommandResult::Delete) => {
                self.object_metadata_repository.delete(key).await?;
            }
            (ObjectCommand::Cas { .. }, CommandResult::Cas(CasResult::Updated(metadata))) => {
                self.object_metadata_repository.store(metadata).await?;
            }
            _ => {}
        }
        let metadata_apply_ms = elapsed_ms(metadata_apply_started);

        let apply_reorder_buffer_size_end = {
            let mut buffer = self.reorder_buffer.lock().await;
            let key = Self::command_object_key(&req.command).clone();
            if let Some(inner) = buffer.get_mut(&key) {
                inner.remove(&req.timestamp);
                if inner.is_empty() {
                    buffer.remove(&key);
                }
            }
            buffer.values().map(|m| m.len()).sum::<usize>()
        };
        self.apply_notify.notify_waiters();

        info!(
            coordination_event = "apply_backlog",
            backlog_event = "apply",
            node = self.node_id.as_ref(),
            origin_node = req.command_id.origin_node_id.as_ref(),
            operation_id_sequence = req.command_id.sequence,
            operation = Self::command_operation(&req.command),
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
            "inbound apply backlog"
        );

        Ok(ApplyResponse { result })
    }
}
