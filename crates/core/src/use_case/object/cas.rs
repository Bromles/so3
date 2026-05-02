use crate::domain::blob::payload::BlobPayload;
use crate::domain::blobs::{BlobMetadata, BlobPayload};
use crate::domain::command::{CasCommand, CasResult, CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::ObjectLastModified;
use crate::domain::object::version::ObjectVersion;
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<B: BlobRepository> ObjectUseCaseImpl<B> {
    pub async fn cas_internal(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        value: BlobPayload,
    ) -> So3Result<CasResult> {
        let last_modified = ObjectLastModified::now()?;
        let blob = self.blob_repository.store(value).await?;

        match self
            .state_machine
            .execute(ObjectCommand::Cas(CasCommand {
                key,
                expected_version,
                metadata: BlobMetadata {
                    blob_id: blob.blob_id,
                    content_length: blob.content_length,
                    checksum_sha256: blob.checksum_sha256,
                },
                last_modified,
            }))
            .await?
        {
            CommandResult::Cas(result) => Ok(result),
            result => Self::unexpected_result("Cas", &result),
        }
    }
}
