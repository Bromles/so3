use crate::domain::blobs::BlobPayload;
use crate::domain::command::{CasResult, CommandResult};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::{ObjectMetadata, StoredObject};
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::use_case::object::ObjectUseCase;
use async_trait::async_trait;

pub struct ObjectUseCaseImpl<B: BlobRepository> {
    pub blob_repository: B,
}

impl<B: BlobRepository> ObjectUseCaseImpl<B> {
    pub fn new(blob_repository: B) -> Self {
        Self { blob_repository }
    }

    pub fn unexpected_result<T>(operation: &str, result: &CommandResult) -> So3Result<T> {
        Err(So3Error::InvalidRequest(format!(
            "unexpected state machine result for {operation}: {result}"
        )))
    }
}

#[async_trait]
impl<B: BlobRepository> ObjectUseCase for ObjectUseCaseImpl<B> {
    async fn read(&self, key: ObjectKey) -> So3Result<Option<StoredObject>> {
        self.read_internal(key).await
    }

    async fn write(&self, key: ObjectKey, value: BlobPayload) -> So3Result<ObjectMetadata> {
        self.write_internal(key, value).await
    }

    async fn delete(&self, key: ObjectKey) -> So3Result<()> {
        self.delete_internal(key).await
    }

    async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        value: BlobPayload,
    ) -> So3Result<CasResult> {
        self.cas_internal(key, expected_version, value).await
    }
}
