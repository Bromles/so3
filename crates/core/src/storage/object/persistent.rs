use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::domain::error::So3Result;
use crate::domain::{ObjectKey, ObjectLastModified, ObjectRecord, ObjectVersion, StoredObject};
use crate::storage::blob::fs::FileSystemBlobRepository;
use crate::storage::blob::repository::BlobRepository;
use crate::storage::metadata::repository::ObjectMetadataRepository;
use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
use crate::storage::object::repository::{CasWriteOutcome, ObjectRepository};

#[derive(Clone)]
pub struct PersistentObjectRepository<M: ObjectMetadataRepository, B: BlobRepository> {
    metadata_repository: M,
    blob_repository: B,
    write_lock: Arc<Mutex<()>>,
}

impl<M: ObjectMetadataRepository, B: BlobRepository> PersistentObjectRepository<M, B> {
    #[must_use]
    pub fn from_parts(metadata_repository: M, blob_repository: B) -> Self {
        Self {
            metadata_repository,
            blob_repository,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    async fn write_next_version(
        &self,
        key: &ObjectKey,
        version: ObjectVersion,
        value: Vec<u8>,
        last_modified: ObjectLastModified,
    ) -> So3Result<StoredObject> {
        let blob = self.blob_repository.store(&value).await?;
        let record = ObjectRecord {
            key: key.clone(),
            version,
            blob_id: blob.blob_id,
            content_length: blob.content_length,
            checksum: blob.checksum,
            last_modified,
        };
        self.metadata_repository.write(&record).await?;

        Ok(StoredObject { record, value })
    }
}

impl PersistentObjectRepository<SqliteObjectMetadataRepository, FileSystemBlobRepository> {
    /// # Errors
    ///
    /// Returns an error if the local metadata or blob repositories cannot be created.
    pub async fn new(
        metadata_dir: impl AsRef<Path>,
        blob_dir: impl AsRef<Path>,
    ) -> So3Result<Self> {
        let metadata_repository = SqliteObjectMetadataRepository::new(metadata_dir).await?;
        let blob_repository = FileSystemBlobRepository::new(blob_dir).await?;

        Ok(Self::from_parts(metadata_repository, blob_repository))
    }
}

#[async_trait]
impl<M, B> ObjectRepository for PersistentObjectRepository<M, B>
where
    M: ObjectMetadataRepository,
    B: BlobRepository,
{
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let Some(record) = self.metadata_repository.read(key).await? else {
            return Ok(None);
        };

        let value = self.blob_repository.load(&record.blob_id).await?;
        Ok(Some(StoredObject { record, value }))
    }

    async fn write(
        &self,
        key: &ObjectKey,
        value: Vec<u8>,
        last_modified: ObjectLastModified,
    ) -> So3Result<StoredObject> {
        let _guard = self.write_lock.lock().await;
        let next_version = self
            .metadata_repository
            .read(key)
            .await?
            .map_or_else(ObjectVersion::initial, |record| record.version.next());

        self.write_next_version(key, next_version, value, last_modified)
            .await
    }

    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
        last_modified: ObjectLastModified,
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
            .write_next_version(key, current.version.next(), value, last_modified)
            .await?;
        Ok(CasWriteOutcome::Applied(object))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    use super::PersistentObjectRepository;
    use crate::domain::{ObjectKey, ObjectLastModified, ObjectVersion};
    use crate::storage::blob::fs::FileSystemBlobRepository;
    use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::storage::object::repository::{CasWriteOutcome, ObjectRepository};

    const FIRST_PAYLOAD: &[u8] = b"first";
    const SECOND_PAYLOAD: &[u8] = b"second";
    const HELLO_PAYLOAD: &[u8] = b"hello";
    const STALE_VERSION_NUMBER: i64 = 99;
    const KEY_ALPHA: &str = "alpha";
    const KEY_BETA: &str = "beta";
    const KEY_GAMMA: &str = "gamma";
    const KEY_DELTA: &str = "delta";
    const FIRST_LAST_MODIFIED: i64 = 1_775_000_000_123;
    const SECOND_LAST_MODIFIED: i64 = 1_775_000_001_456;
    const THIRD_LAST_MODIFIED: i64 = 1_775_000_002_789;

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
        let written = repository
            .write(
                &key,
                HELLO_PAYLOAD.to_vec(),
                last_modified(FIRST_LAST_MODIFIED),
            )
            .await
            .unwrap();
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
        assert_eq!(loaded.record.last_modified, written.record.last_modified);
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

        let written = repository
            .write(
                &key,
                FIRST_PAYLOAD.to_vec(),
                last_modified(FIRST_LAST_MODIFIED),
            )
            .await
            .unwrap();
        let outcome = repository
            .cas(
                &key,
                ObjectVersion::try_from(STALE_VERSION_NUMBER).unwrap(),
                SECOND_PAYLOAD.to_vec(),
                last_modified(SECOND_LAST_MODIFIED),
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

        let written = repository
            .write(
                &key,
                FIRST_PAYLOAD.to_vec(),
                last_modified(FIRST_LAST_MODIFIED),
            )
            .await
            .unwrap();
        let outcome = repository
            .cas(
                &key,
                written.record.version,
                SECOND_PAYLOAD.to_vec(),
                last_modified(SECOND_LAST_MODIFIED),
            )
            .await
            .unwrap();

        let CasWriteOutcome::Applied(object) = outcome else {
            panic!("expected applied cas outcome");
        };

        assert_eq!(object.record.version, written.record.version.next());
        assert_eq!(
            object.record.last_modified,
            last_modified(SECOND_LAST_MODIFIED)
        );
        assert_eq!(object.value, SECOND_PAYLOAD.to_vec());
    }

    #[tokio::test]
    async fn concurrent_cas_allows_exactly_one_winner_for_a_version() {
        let temp_dir = TempDir::new().unwrap();
        let key = ObjectKey::new(KEY_DELTA).unwrap();
        let repository = PersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let written = repository
            .write(
                &key,
                FIRST_PAYLOAD.to_vec(),
                last_modified(FIRST_LAST_MODIFIED),
            )
            .await
            .unwrap();

        let left_task = spawn_cas(
            repository.clone(),
            key.clone(),
            written.record.version,
            SECOND_PAYLOAD.to_vec(),
            last_modified(SECOND_LAST_MODIFIED),
        );
        let right_task = spawn_cas(
            repository.clone(),
            key.clone(),
            written.record.version,
            HELLO_PAYLOAD.to_vec(),
            last_modified(THIRD_LAST_MODIFIED),
        );

        let left = left_task.await.unwrap();
        let right = right_task.await.unwrap();
        let outcomes = [left, right];
        let applied_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CasWriteOutcome::Applied(_)))
            .count();
        let mismatch_count = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CasWriteOutcome::Mismatch { .. }))
            .count();
        let loaded = repository.read(&key).await.unwrap().unwrap();

        assert_eq!(applied_count, 1);
        assert_eq!(mismatch_count, 1);
        assert_eq!(loaded.record.version, written.record.version.next());
        assert!(loaded.value == SECOND_PAYLOAD.to_vec() || loaded.value == HELLO_PAYLOAD.to_vec());
    }

    fn spawn_cas(
        repository: PersistentObjectRepository<
            SqliteObjectMetadataRepository,
            FileSystemBlobRepository,
        >,
        key: ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
        last_modified: ObjectLastModified,
    ) -> JoinHandle<CasWriteOutcome> {
        tokio::spawn(async move {
            repository
                .cas(&key, expected_version, value, last_modified)
                .await
                .unwrap()
        })
    }

    fn last_modified(unix_millis: i64) -> ObjectLastModified {
        ObjectLastModified::try_from(unix_millis).unwrap()
    }
}
