use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand, ReadResult};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::StoredObject;
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
    pub async fn read_internal(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let command_id = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Read { key: key.clone() })
            .await?;

        let metadata = self.object_metadata_repository.load(key).await?;

        let Some(metadata) = metadata else {
            self.consensus_journal_repository
                .record_applied(&command_id, &CommandResult::Read(ReadResult::NotFound))
                .await?;

            return Ok(None);
        };

        let payload = self.blob_repository.load(&metadata.blob_id).await?;

        self.consensus_journal_repository
            .record_applied(
                &command_id,
                &CommandResult::Read(ReadResult::Found(metadata.clone())),
            )
            .await?;

        Ok(Some(StoredObject {
            metadata,
            blob: payload,
        }))
    }
}
