use crate::client::interface::BlobPeerClient;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CJ, BR, BC> ObjectUseCaseImpl<CJ, BR, BC>
where
    CJ: ConsensusJournalRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn write_internal(&self, key: ObjectKey, payload: BlobPayload) -> So3Result<ObjectMetadata> {
        let blob_id = BlobId::new();
        self.blob_repository.store(&blob_id, payload).await?;

        match self
            .state_machine
            .execute(ObjectCommand::Write {
                key,
                metadata: BlobMetadata {
                    blob_id: blob.blob_id,
                    content_length: blob.content_length,
                    checksum_sha256: blob.checksum_sha256,
                },
                last_modified,
            })
            .await?
        {
            CommandResult::Write(result) => Ok(result.metadata),
            result => Self::unexpected_result("Write", &result),
        }
    }
}