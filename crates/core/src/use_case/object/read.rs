use crate::domain::blobs::Blob;
use crate::domain::command::{CommandResult, ObjectCommand, ReadCommand};
use crate::domain::error::So3Result;
use crate::domain::object::StoredObject;
use crate::domain::object_key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<B: BlobRepository> ObjectUseCaseImpl<B> {
    pub async fn read_internal(&self, key: ObjectKey) -> So3Result<Option<StoredObject>> {
        match self.state_machine.execute(ObjectCommand::Read(ReadCommand{key})).await? {
            CommandResult::Read(result) => match result.metadata {
                Some(metadata) => {
                    let blob_payload = self.blob_repository.load(&metadata.blob_metadata.blob_id).await?;

                    Ok(Some(StoredObject{
                        metadata,
                        blob: blob_payload,
                    }))
                }
                None => Ok(None),
            }
            result => Self::unexpected_result("Read", &result),
        }
    }
}