use crate::domain::blobs::{BlobMetadata, BlobPayload};
use crate::domain::command::{CommandResult, ObjectCommand, WriteCommand};
use crate::domain::error::So3Result;
use crate::domain::object::{ObjectLastModified, ObjectMetadata};
use crate::domain::object_key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<B: BlobRepository> ObjectUseCaseImpl<B> {
    pub async fn write_internal(&self, key: ObjectKey, value: BlobPayload) -> So3Result<ObjectMetadata> {
        let last_modified = ObjectLastModified::now()?;
        let blob = self.blob_repository.store(value).await?;
        
        match self
            .state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key,
                metadata: BlobMetadata {
                    blob_id: blob.blob_id,
                    content_length: blob.content_length,
                    checksum_sha256: blob.checksum_sha256,
                },
                last_modified,
            }))
            .await?
        {
            CommandResult::Write(result) => Ok(result.metadata),
            result => Self::unexpected_result("Write", &result),
        }
    }
}