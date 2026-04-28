use crate::consensus::state_machine::ObjectCommandExecutor;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{
    BlobMetadata, CasCommand, CasResult, DeleteCommand, ObjectCommand, ObjectKey,
    ObjectLastModified, ObjectRecord, ObjectResult, ObjectVersion, ReadCommand, StoredObject,
    WriteCommand,
};
use crate::repository::blob::interface::BlobRepository;

#[derive(Clone)]
pub struct ObjectService<E: ObjectCommandExecutor, B: BlobRepository> {
    executor: E,
    blob_repository: B,
}

impl<E: ObjectCommandExecutor, B: BlobRepository> ObjectService<E, B> {
    #[must_use]
    pub fn new(executor: E, blob_repository: B) -> Self {
        Self {
            executor,
            blob_repository,
        }
    }

    /// Returns a reference to the underlying blob repository.
    #[must_use]
    pub fn blob_repository(&self) -> &B {
        &self.blob_repository
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Read` command,
    /// or a repository error when loading blob bytes for the returned record.
    pub async fn read(&self, key: ObjectKey) -> So3Result<Option<StoredObject>> {
        match self
            .executor
            .execute_command(ObjectCommand::Read(ReadCommand { key }))
            .await?
        {
            ObjectResult::Read(result) => match result.record {
                Some(record) => {
                    let value = self.blob_repository.load(&record.blob_id).await?;
                    Ok(Some(StoredObject { record, value }))
                }
                None => Ok(None),
            },
            result => unexpected_result("Read", &result),
        }
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Write` command.
    pub async fn write(&self, key: ObjectKey, value: Vec<u8>) -> So3Result<ObjectRecord> {
        let last_modified = ObjectLastModified::now()?;
        let blob = self.blob_repository.store(&value).await?;
        match self
            .executor
            .execute_command(ObjectCommand::Write(WriteCommand {
                key,
                metadata: BlobMetadata {
                    blob_id: blob.blob_id,
                    content_length: blob.content_length,
                    checksum: blob.checksum_sha256,
                },
                last_modified,
            }))
            .await?
        {
            ObjectResult::Write(result) => Ok(result.record),
            result => unexpected_result("Write", &result),
        }
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Delete` command.
    pub async fn delete(&self, key: ObjectKey) -> So3Result<()> {
        match self
            .executor
            .execute_command(ObjectCommand::Delete(DeleteCommand { key }))
            .await?
        {
            ObjectResult::Delete(_) => Ok(()),
            result => unexpected_result("Delete", &result),
        }
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Cas` command.
    pub async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasResult> {
        let last_modified = ObjectLastModified::now()?;
        let blob = self.blob_repository.store(&value).await?;
        match self
            .executor
            .execute_command(ObjectCommand::Cas(CasCommand {
                key,
                expected_version,
                metadata: BlobMetadata {
                    blob_id: blob.blob_id,
                    content_length: blob.content_length,
                    checksum: blob.checksum_sha256,
                },
                last_modified,
            }))
            .await?
        {
            ObjectResult::Cas(result) => Ok(result),
            result => unexpected_result("Cas", &result),
        }
    }
}

fn unexpected_result<T>(operation: &str, result: &ObjectResult) -> So3Result<T> {
    Err(So3Error::InvalidRequest(format!(
        "unexpected state machine result for {operation}: {result:?}"
    )))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::ObjectService;
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::domain::{CasResult, ObjectKey, ObjectVersion};
    use crate::repository::blob::fs::FileSystemBlobRepository;
    use crate::repository::registry::SqliteFsPersistentObjectRepository;

    const MISSING_KEY: &str = "missing";
    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const INITIAL_VERSION: i64 = 1;
    const NEXT_VERSION: i64 = 2;
    const VERSION_INCREMENT: i64 = 1;

    async fn test_service() -> (
        ObjectService<
            LocalStateMachine<SqliteFsPersistentObjectRepository>,
            FileSystemBlobRepository,
        >,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let blob_repository = repository.blob_repository().clone();
        let state_machine = LocalStateMachine::new(repository);
        (ObjectService::new(state_machine, blob_repository), temp_dir)
    }

    #[tokio::test]
    async fn read_returns_none_for_missing_key() {
        let (service, _temp_dir) = test_service().await;

        let loaded = service
            .read(ObjectKey::new(MISSING_KEY).unwrap())
            .await
            .unwrap();

        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn write_persists_and_increments_version() {
        let (service, _temp_dir) = test_service().await;
        let key = ObjectKey::new(ALPHA_KEY).unwrap();

        let first = service
            .write(key.clone(), FIRST_VALUE.to_vec())
            .await
            .unwrap();
        let second = service.write(key, SECOND_VALUE.to_vec()).await.unwrap();

        assert_eq!(first.version.get(), INITIAL_VERSION);
        assert_eq!(second.version.get(), NEXT_VERSION);
        // Blob bytes are not in ObjectRecord; read back to verify payload.
        let read = service
            .read(ObjectKey::new(ALPHA_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.value, SECOND_VALUE.to_vec());
    }

    #[tokio::test]
    async fn cas_returns_structured_mismatch() {
        let (service, _temp_dir) = test_service().await;
        let key = ObjectKey::new(ALPHA_KEY).unwrap();
        let written = service
            .write(key.clone(), FIRST_VALUE.to_vec())
            .await
            .unwrap();

        let result = service
            .cas(
                key,
                ObjectVersion::try_from(written.version.get() + VERSION_INCREMENT).unwrap(),
                SECOND_VALUE.to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            CasResult::Mismatch {
                current_version: written.version,
            }
        );
    }
}
