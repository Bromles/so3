use crate::domain::blob::stream::BlobStream;
use crate::domain::command::CasResult;
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::{ObjectMetadata, StoredObject};
use crate::domain::object::version::ObjectVersion;
use async_trait::async_trait;

#[async_trait]
pub trait ObjectUseCase: Send + Sync + 'static {
    async fn head(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>>;
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>>;
    async fn write(&self, key: ObjectKey, body: BlobStream) -> So3Result<ObjectMetadata>;
    async fn delete(&self, key: &ObjectKey) -> So3Result<()>;
    async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        body: BlobStream,
    ) -> So3Result<CasResult>;
}
