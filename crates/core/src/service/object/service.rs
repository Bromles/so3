use crate::domain::blobs::{Blob, BlobPayload};
use crate::domain::command::CasResult;
use crate::domain::error::So3Result;
use crate::domain::object::{ObjectLastModified, ObjectMetadata, StoredObject};
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::object::ObjectService;
use async_trait::async_trait;

pub struct ObjectServiceImpl<M: ObjectMetadataRepository, B: BlobRepository> {
    metadata_repository: M,
    blob_repository: B,
}

impl<M: ObjectMetadataRepository, B: BlobRepository> ObjectServiceImpl<M, B> {
    #[must_use]
    pub fn from_parts(metadata_repository: M, blob_repository: B) -> Self {
        Self {
            metadata_repository,
            blob_repository,
        }
    }

    async fn write_next_version(
        &self,
        key: &ObjectKey,
        version: ObjectVersion,
        last_modified: ObjectLastModified,
        blob: Blob,
    ) -> So3Result<ObjectMetadata> {
        let blob_metadata = self.blob_repository.store(blob.payload).await?;

        let object_metadata = ObjectMetadata {
            key: key.clone(),
            version,
            blob_metadata,
            last_modified,
        };

        self.metadata_repository.write(&object_metadata).await?;

        Ok(object_metadata)
    }
}

#[async_trait]
impl<M: ObjectMetadataRepository, B: BlobRepository> ObjectService for ObjectServiceImpl<M, B> {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let Some(metadata) = self.metadata_repository.read(key).await? else {
            return Ok(None);
        };

        let blob_payload = self
            .blob_repository
            .load(metadata.blob_metadata.blob_id.clone())
            .await?;

        let blob = Blob {
            metadata: metadata.blob_metadata.clone(),
            payload: blob_payload,
        };

        Ok(Some(StoredObject {
            metadata,
            blob,
        }))
    }

    async fn write(
        &self,
        key: &ObjectKey,
        last_modified: ObjectLastModified,
        blob: Blob,
    ) -> So3Result<ObjectMetadata> {
        let next_version = self
            .metadata_repository
            .read(key)
            .await?
            .map_or_else(ObjectVersion::initial, |metadata| metadata.version.next());

        self.write_next_version(key, next_version, last_modified, blob)
    }

    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        payload: BlobPayload,
        last_modified: ObjectLastModified,
    ) -> So3Result<CasResult> {
        let Some(current) = self.metadata_repository.read(key).await? else {
            return Ok(CasResult::NotFound);
        };

        if current.version != expected_version {
            return Ok(CasResult::Mismatch {
                current_version: current.version,
            });
        }

        let blob = Blob {
            metadata: current.blob_metadata,
            payload,
        };

        let metadata = self
            .write_next_version(key, current.version.next(), last_modified, blob)
            .await?;

        Ok(CasResult::Applied(metadata))
    }

    async fn delete(&self, key: &ObjectKey) -> So3Result<()> {
        let Some(metadata) = self.metadata_repository.read(key).await? else {
            return Ok(());
        };

        self.metadata_repository.delete(key).await?;
        self.blob_repository
            .delete(metadata.blob_metadata.blob_id)
            .await?;

        Ok(())
    }
}
