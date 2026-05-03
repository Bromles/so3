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

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn apply_internal(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        // Idempotency
        if let Some(entry) = self.journal.load(&req.command_id).await? {
            if entry.state == JournalState::Applied {
                let result = entry
                    .result
                    .ok_or_else(|| So3Error::Storage("applied entry missing result".to_string()))?;
                return Ok(ApplyResponse { result });
            }
        }

        // Wait for dependencies
        for dep_id in &req.dependencies.0 {
            match self.journal.load(dep_id).await? {
                Some(e) if e.state == JournalState::Applied => {}
                _ => {
                    return Err(So3Error::PeerUnavailable(format!(
                        "dependency seq={} not yet applied",
                        dep_id.sequence
                    )));
                }
            }
        }

        // Execute command inline
        let result = match req.command {
            ObjectCommand::Read { ref key } => match self.metadata_repo.load(key).await? {
                Some(m) => CommandResult::Read(ReadResult::Found(m)),
                None => CommandResult::Read(ReadResult::NotFound),
            },
            ObjectCommand::Write {
                ref key,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repo.exists(&blob_id).await? {
                    let payload = self.fetch_blob_from_any_peer(&blob_id).await?;
                    self.blob_repo.store(&blob_id, &payload).await?;
                }
                let version = self
                    .metadata_repo
                    .load(key)
                    .await?
                    .map(|m| m.version.next())
                    .unwrap_or_else(ObjectVersion::initial);
                let metadata = ObjectMetadata {
                    key: key.clone(),
                    version,
                    blob_id,
                    sha256,
                    size,
                    last_modified_ms: physical_millis_now(),
                };
                self.metadata_repo.store(&metadata).await?;
                CommandResult::Write(WriteResult { metadata })
            }
            ObjectCommand::Delete { ref key } => {
                self.metadata_repo.delete(key).await?;
                CommandResult::Delete
            }
            ObjectCommand::Cas {
                ref key,
                ref expected_version,
                blob_id,
                sha256,
                size,
            } => {
                if !self.blob_repo.exists(&blob_id).await? {
                    let payload = self.fetch_blob_from_any_peer(&blob_id).await?;
                    self.blob_repo.store(&blob_id, &payload).await?;
                }
                match self.metadata_repo.load(key).await? {
                    Some(meta) if meta.version == *expected_version => {
                        let new_meta = ObjectMetadata {
                            key: key.clone(),
                            version: meta.version.next(),
                            blob_id,
                            sha256,
                            size,
                            last_modified_ms: physical_millis_now(),
                        };
                        self.metadata_repo.store(&new_meta).await?;
                        CommandResult::Cas(CasResult::Updated(new_meta))
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

        // TODO: metadata store and record_applied must be atomic — same crash window as object use case.
        self.journal
            .record_applied(&req.command_id, &result)
            .await?;

        Ok(ApplyResponse { result })
    }
}
