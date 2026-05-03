use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::clock::physical_millis_now;
use crate::domain::command::{CommandResult, ObjectCommand, WriteResult};
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
    pub async fn write_internal(
        &self,
        key: ObjectKey,
        payload: BlobPayload,
    ) -> So3Result<ObjectMetadata> {
        let blob_id = BlobId::new();
        let sha256 = Sha256Digest::compute(payload.as_bytes());
        let size = payload.len() as u64;

        self.blob_repository.store(&blob_id, &payload).await?;

        for (node_id, client) in &self.blob_client_map {
            // TODO - make parallel
            client.push(node_id, blob_id, &payload).await?;
        }

        let command = ObjectCommand::Write {
            key: key.clone(),
            blob_id,
            sha256,
            size,
        };

        let command_id = self
            .consensus_coordinator_service
            .coordinate(command)
            .await?;

        let version = self
            .object_metadata_repository
            .load(&key)
            .await?
            .map(|m| m.version.next())
            .unwrap_or_else(ObjectVersion::initial);

        let last_modified_ms = physical_millis_now();

        let metadata = ObjectMetadata {
            key,
            version,
            blob_id,
            sha256,
            size,
            last_modified_ms,
        };

        // TODO: metadata store and record_applied must be atomic — crash between them leaves
        // the command as Committed in the journal, causing Accord recovery to re-apply and
        // produce a duplicate version increment. Requires UoW or shared transaction.
        self.object_metadata_repository.store(&metadata).await?;

        let result = CommandResult::Write(WriteResult {
            metadata: metadata.clone(),
        });

        self.consensus_journal_repository
            .record_applied(&command_id, &result)
            .await?;

        Ok(metadata)
    }
}
