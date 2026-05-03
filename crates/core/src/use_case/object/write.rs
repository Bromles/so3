use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
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

        for client in self.blob_client_map.values() {
            // TODO make parallel
            client.push(blob_id, &payload).await?;
        }

        let result = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Write {
                key,
                blob_id,
                sha256,
                size,
            })
            .await?;

        match result {
            CommandResult::Write(r) => Ok(r.metadata),
            _ => Err(So3Error::Storage(
                "unexpected result from Write coordinate".into(),
            )),
        }
    }
}
