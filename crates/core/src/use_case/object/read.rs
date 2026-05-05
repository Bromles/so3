use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand, ReadResult};
use crate::domain::error::{So3Error, So3Result};
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
        let result = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Read { key: key.clone() })
            .await?;

        let metadata = match result {
            CommandResult::Read(ReadResult::Found(m)) => m,
            CommandResult::Read(ReadResult::NotFound) => return Ok(None),
            _ => {
                return Err(So3Error::Storage(
                    "unexpected result from Read coordinate".into(),
                ));
            }
        };

        if !self.blob_repository.exists(&metadata.blob_id).await? {
            self.fetch_blob_from_any_peer(&metadata.blob_id).await?;
        }

        let blob = self.blob_repository.open_reader(&metadata.blob_id).await?;

        Ok(Some(StoredObject { metadata, blob }))
    }
}
