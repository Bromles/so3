use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::domain::error::So3Result;
use crate::domain::{ObjectKey, ObjectRecord, ObjectVersion, StoredObject};
use crate::storage::blob_repository::BlobRepository;
use crate::storage::fs_blob_repository::FileSystemBlobRepository;
use crate::storage::metadata_repository::ObjectMetadataRepository;
use crate::storage::repository::{CasWriteOutcome, ObjectRepository};
use crate::storage::sqlite_metadata_repository::SqliteObjectMetadataRepository;

pub struct PersistentObjectRepository {
    metadata_repository: Arc<dyn ObjectMetadataRepository>,
    blob_repository: Arc<dyn BlobRepository>,
    write_lock: Mutex<()>,
}

impl PersistentObjectRepository {
    /// # Errors
    ///
    /// Returns an error if the local metadata or blob repositories cannot be created.
    pub async fn new(metadata_dir: impl AsRef<Path>, blob_dir: impl AsRef<Path>) -> So3Result<Self> {
        let metadata_repository = Arc::new(SqliteObjectMetadataRepository::new(metadata_dir).await?);
        let blob_repository = Arc::new(FileSystemBlobRepository::new(blob_dir).await?);

        Ok(Self {
            metadata_repository,
            blob_repository,
            write_lock: Mutex::new(()),
        })
    }

    async fn write_next_version(
        &self,
        key: &ObjectKey,
        version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<StoredObject> {
        let blob = self.blob_repository.store(&value).await?;
        let record = ObjectRecord {
            key: key.clone(),
            version,
            blob_id: blob.blob_id,
            content_length: blob.content_length,
            checksum: blob.checksum,
        };
        self.metadata_repository.write(&record).await?;

        Ok(StoredObject { record, value })
    }
}

#[async_trait]
impl ObjectRepository for PersistentObjectRepository {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let Some(record) = self.metadata_repository.read(key).await? else {
            return Ok(None);
        };

        let value = self.blob_repository.load(&record.blob_id).await?;
        Ok(Some(StoredObject { record, value }))
    }

    async fn write(&self, key: &ObjectKey, value: Vec<u8>) -> So3Result<StoredObject> {
        let _guard = self.write_lock.lock().await;
        let next_version = self
            .metadata_repository
            .read(key)
            .await?
            .map_or_else(ObjectVersion::initial, |record| record.version.next());

        self.write_next_version(key, next_version, value).await
    }

    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasWriteOutcome> {
        let _guard = self.write_lock.lock().await;
        let Some(current) = self.metadata_repository.read(key).await? else {
            return Ok(CasWriteOutcome::NotFound);
        };

        if current.version != expected_version {
            return Ok(CasWriteOutcome::Mismatch {
                current_version: current.version,
            });
        }

        let object = self
            .write_next_version(key, current.version.next(), value)
            .await?;
        Ok(CasWriteOutcome::Applied(object))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::PersistentObjectRepository;
    use crate::domain::{ObjectKey, ObjectVersion};
    use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

    const FIRST_PAYLOAD: &[u8] = b"first";
    const SECOND_PAYLOAD: &[u8] = b"second";
    const HELLO_PAYLOAD: &[u8] = b"hello";
    const STALE_VERSION_NUMBER: i64 = 99;
    const KEY_ALPHA: &str = "alpha";
    const KEY_BETA: &str = "beta";
    const KEY_GAMMA: &str = "gamma";

    #[tokio::test]
    async fn write_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_ALPHA).unwrap();

        let repository = PersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let written = repository.write(&key, HELLO_PAYLOAD.to_vec()).await.unwrap();
        assert_eq!(written.record.version, ObjectVersion::initial());
        drop(repository);

        let reopened = PersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
            .await
            .unwrap();
        let loaded = reopened.read(&key).await.unwrap().unwrap();

        assert_eq!(loaded.record.version, ObjectVersion::initial());
        assert_eq!(loaded.value, HELLO_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn cas_reports_mismatch_without_overwriting() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_BETA).unwrap();
        let repository = PersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();

        let written = repository.write(&key, FIRST_PAYLOAD.to_vec()).await.unwrap();
        let outcome = repository
            .cas(
                &key,
                ObjectVersion::try_from(STALE_VERSION_NUMBER).unwrap(),
                SECOND_PAYLOAD.to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            outcome,
            CasWriteOutcome::Mismatch {
                current_version: written.record.version,
            }
        );

        let loaded = repository.read(&key).await.unwrap().unwrap();
        assert_eq!(loaded.value, FIRST_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn cas_applies_new_value_and_bumps_version() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_GAMMA).unwrap();
        let repository = PersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();

        let written = repository.write(&key, FIRST_PAYLOAD.to_vec()).await.unwrap();
        let outcome = repository
            .cas(&key, written.record.version, SECOND_PAYLOAD.to_vec())
            .await
            .unwrap();

        let CasWriteOutcome::Applied(object) = outcome else {
            panic!("expected applied cas outcome");
        };

        assert_eq!(object.record.version, written.record.version.next());
        assert_eq!(object.value, SECOND_PAYLOAD.to_vec());
    }
}
