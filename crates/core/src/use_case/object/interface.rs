use async_trait::async_trait;
use crate::domain::blob::BlobPayload;
use crate::domain::command::CasResult;
use crate::domain::error::So3Result;
use crate::domain::object::{ObjectMetadata, StoredObject};
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;

#[async_trait]
pub trait ObjectUseCase {
    async fn read(&self, key: ObjectKey) -> So3Result<Option<StoredObject>>;
    async fn write(&self, key: ObjectKey, value: BlobPayload) -> So3Result<ObjectMetadata>;
    async fn delete(&self, key: ObjectKey) -> So3Result<()>;
    async fn cas(&self, key: ObjectKey, expected_version: ObjectVersion, value: BlobPayload) -> So3Result<CasResult>;
}