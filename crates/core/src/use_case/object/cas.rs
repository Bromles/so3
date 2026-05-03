use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::clock::physical_millis_now;
use crate::domain::command::{CasResult, CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CCS, CJR, OMR, BR, BC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn cas_internal(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        payload: BlobPayload,
    ) -> So3Result<CasResult> {
        let blob_id = BlobId::new();
        let sha256 = Sha256Digest::compute(payload.as_bytes());
        let size = payload.len() as u64;

        self.blob_repository.store(&blob_id, &payload).await?;

        for (peer_id, client) in &self.blob_client_map {
            client.push(peer_id, blob_id, &payload).await?;
        }

        let command_id = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Cas {
                key: key.clone(),
                expected_version: expected_version.clone(),
                blob_id,
                sha256,
                size,
            })
            .await?;

        let current = self.object_metadata_repository.load(&key).await?;

        let cas_result = match current {
            Some(meta) if meta.version == expected_version => {
                let last_modified_ms = physical_millis_now();

                let new_metadata = ObjectMetadata {
                    key,
                    version: meta.version.next(),
                    blob_id,
                    sha256,
                    size,
                    last_modified_ms,
                };

                // TODO: metadata store and record_applied must be atomic — same crash window as write.
                self.object_metadata_repository.store(&new_metadata).await?;
                CasResult::Updated(new_metadata)
            }
            Some(meta) => CasResult::Conflict {
                current_version: meta.version,
            },
            None => CasResult::Conflict {
                current_version: ObjectVersion::initial(),
            },
        };

        self.consensus_journal_repository
            .record_applied(&command_id, &CommandResult::Cas(cas_result.clone()))
            .await?;

        Ok(cas_result)
    }
}
