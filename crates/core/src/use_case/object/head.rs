use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::{ObjectMetadata, StoredObject};
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CJ, BR, BC> ObjectUseCaseImpl<CJ, BR, BC>
where
    CJ: ConsensusJournalRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn head_internal(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        match self
            .state_machine
            .execute(ObjectCommand::Read { key })
            .await?
        {
            CommandResult::Read(result) => match result.metadata {
                Some(metadata) => {
                    let blob_payload = self
                        .blob_repository
                        .load(&metadata.blob_metadata.blob_id)
                        .await?;

                    Ok(Some(StoredObject {
                        metadata,
                        blob: blob_payload,
                    }))
                }
                None => Ok(None),
            },
            result => Self::unexpected_result("Read", &result),
        }
    }
}
