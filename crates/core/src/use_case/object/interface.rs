use crate::domain::blob::payload::BlobPayload;
use crate::domain::command::CasResult;
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::{ObjectMetadata, StoredObject};
use crate::domain::object::version::ObjectVersion;
use async_trait::async_trait;

#[async_trait]
pub trait ObjectUseCase {
    async fn head(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>>;
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>>;
    async fn write(&self, key: ObjectKey, bytes: BlobPayload) -> So3Result<ObjectMetadata>;
    async fn delete(&self, key: &ObjectKey) -> So3Result<()>;
    async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        bytes: BlobPayload,
    ) -> So3Result<CasResult>;
}
