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
use tokio::time::{Duration, Instant, timeout_at};

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn apply_internal(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
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
        loop {
            let notified = self.apply_notify.notified();
            {
                let buf = self.reorder_buffer.lock().await;
                if buf.range(..req.timestamp.clone()).next().is_none() {
                    break;
                }
            }
            timeout_at(deadline, notified).await.map_err(|_| {
                So3Error::PeerUnavailable(
                    "reorder buffer: deadline exceeded waiting for earlier committed command"
                        .into(),
                )
            })?;
        }

        // Wait for explicit dependencies to be applied.
        // Register Notify before checking to avoid the TOCTOU where a dep is applied
        // between the check and the await. Reuses the same deadline as the reorder buffer.
        loop {
            let notified = self.apply_notify.notified();
            let mut pending = None;
            for dep_id in &req.dependencies.0 {
                match self.journal.load(dep_id).await? {
                    Some(e) if e.state == JournalState::Applied => {}
                    _ => {
                        pending = Some(dep_id.sequence);
                        break;
                    }
                }
            }
            match pending {
                None => break,
                Some(seq) => {
                    timeout_at(deadline, notified).await.map_err(|_| {
                        So3Error::PeerUnavailable(format!(
                            "dependency seq={seq} not applied within deadline"
                        ))
                    })?;
                }
            }
        }

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
                    let payload = self.fetch_blob_from_any_peer(blob_id).await?;
                    self.blob_repository.store(blob_id, &payload).await?;
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
                    let payload = self.fetch_blob_from_any_peer(blob_id).await?;
                    self.blob_repository.store(blob_id, &payload).await?;
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
        self.journal
            .record_applied(&req.command_id, &result)
            .await?;

        // Apply object metadata side effects.
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

        self.reorder_buffer.lock().await.remove(&req.timestamp);
        self.apply_notify.notify_waiters();

        Ok(ApplyResponse { result })
    }
}
