use crate::domain::blobs::{Blob, BlobPayload};
use crate::domain::command::CasResult;
use crate::domain::error::So3Result;
use crate::domain::object::{ObjectLastModified, ObjectMetadata, StoredObject};
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use async_trait::async_trait;

#[async_trait]
pub trait ObjectService {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>>;

    async fn write(
        &self,
        key: &ObjectKey,
        last_modified: ObjectLastModified,
        blob: Blob,
    ) -> So3Result<ObjectMetadata>;

    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        payload: BlobPayload,
        last_modified: ObjectLastModified,
    ) -> So3Result<CasResult>;

    async fn delete(&self, key: &ObjectKey) -> So3Result<()>;
}
