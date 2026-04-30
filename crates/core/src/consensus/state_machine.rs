use crate::domain::command::{CasResult, DeleteResult, ObjectCommand, ReadResult, WriteResult};
use crate::domain::error::So3Result;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[async_trait]
pub trait ObjectCommandExecutor: Send + Sync {
    /// # Errors
    ///
    /// Returns any error raised while executing the deterministic object command.
    async fn execute_command(&self, command: ObjectCommand) -> So3Result<ObjectResult>;
}

#[derive(Clone)]
pub struct LocalStateMachine<R: ObjectRepository> {
    repository: R,
}

impl<R: ObjectRepository> LocalStateMachine<R> {
    #[must_use]
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    #[must_use]
    pub fn repository(&self) -> &R {
        &self.repository
    }

    /// # Errors
    ///
    /// Returns any repository error raised while executing the deterministic object command.
    pub async fn execute(&self, command: ObjectCommand) -> So3Result<ObjectResult> {
        self.execute_command(command).await
    }
}

#[async_trait]
impl<R> ObjectCommandExecutor for LocalStateMachine<R>
where
    R: ObjectRepository,
{
    async fn execute_command(&self, command: ObjectCommand) -> So3Result<ObjectResult> {
        match command {
            ObjectCommand::Read(command) => {
                let record = self.repository.read(&command.key).await?;
                Ok(ObjectResult::Read(ReadResult { metadata: record }))
            }
            ObjectCommand::Write(command) => {
                let record = self
                    .repository
                    .write(&command.key, command.metadata, command.last_modified)
                    .await?;
                Ok(ObjectResult::Write(WriteResult { metadata: record }))
            }
            ObjectCommand::Cas(command) => match self
                .repository
                .cas(
                    &command.key,
                    command.expected_version,
                    command.metadata,
                    command.last_modified,
                )
                .await?
            {
                CasWriteOutcome::Applied(record) => {
                    Ok(ObjectResult::Cas(CasResult::Applied(record)))
                }
                CasWriteOutcome::NotFound => Ok(ObjectResult::Cas(CasResult::NotFound)),
                CasWriteOutcome::Mismatch { current_version } => {
                    Ok(ObjectResult::Cas(CasResult::Mismatch { current_version }))
                }
            },
            ObjectCommand::Delete(command) => {
                self.repository.delete(&command.key).await?;
                Ok(ObjectResult::Delete(DeleteResult))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Write as FmtWrite;

    use async_trait::async_trait;
    use sha2::{Digest, Sha256};
    use tokio::sync::Mutex;

    use super::LocalStateMachine;
    use crate::domain::blob::BlobMetadata;
    use crate::domain::command::{CasCommand, CasResult, ObjectCommand, ReadCommand, ReadResult, WriteCommand};
    use crate::domain::error::So3Result;
    use crate::domain::object::ObjectLastModified;
    use crate::domain::object_key::ObjectKey;
    use crate::domain::object_version::ObjectVersion;

    const KEY_ALPHA: &str = "alpha";
    const MISSING_KEY: &str = "missing";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const WRITE_VALUE: &[u8] = b"value";
    const STALE_VERSION: i64 = 99;

    /// A minimal in-memory object repository used only in unit tests.
    ///
    /// Blobs are stored in a separate map keyed by `blob_id` so that `load_value` works
    /// correctly when the caller supplies a `BlobPayload::Inline` value.
    struct InMemoryRepository {
        objects: Mutex<HashMap<String, ObjectRecord>>,
        blobs: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl InMemoryRepository {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
                blobs: Mutex::new(HashMap::new()),
            }
        }

        /// Persist an inline payload: derive a content-addressed `blob_id`, store the bytes,
        /// and return an `ObjectRecord` with the resulting metadata.
        async fn store_inline(
            &self,
            key: &ObjectKey,
            version: ObjectVersion,
            value: Vec<u8>,
            last_modified: ObjectLastModified,
        ) -> ObjectRecord {
            let checksum = sha256_hex(&value);
            let blob_id = format!("{checksum}.blob");
            self.blobs
                .lock()
                .await
                .insert(blob_id.clone(), value.clone());
            ObjectRecord {
                key: key.clone(),
                version,
                blob_id,
                content_length: value.len() as u64,
                checksum,
                last_modified,
            }
        }
    }

    #[async_trait]
    impl ObjectRepository for InMemoryRepository {
        async fn read(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>> {
            Ok(self.objects.lock().await.get(key.as_str()).cloned())
        }

        async fn load_value(&self, blob_id: &str) -> So3Result<Vec<u8>> {
            Ok(self
                .blobs
                .lock()
                .await
                .get(blob_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn write(
            &self,
            key: &ObjectKey,
            metadata: BlobMetadata,
            last_modified: ObjectLastModified,
        ) -> So3Result<ObjectRecord> {
            let objects = self.objects.lock().await;
            let version = objects
                .get(key.as_str())
                .map_or_else(ObjectVersion::initial, |r| r.version.next());
            drop(objects);

            let record = match metadata {
                BlobMetadata::Inline(value) => {
                    self.store_inline(key, version, value, last_modified).await
                }
                BlobMetadata::Stored {
                    blob_id,
                    content_length,
                    checksum,
                } => ObjectRecord {
                    key: key.clone(),
                    version,
                    blob_id,
                    content_length,
                    checksum,
                    last_modified,
                },
            };
            self.objects
                .lock()
                .await
                .insert(key.as_str().to_owned(), record.clone());
            Ok(record)
        }

        async fn cas(
            &self,
            key: &ObjectKey,
            expected_version: ObjectVersion,
            metadata: BlobMetadata,
            last_modified: ObjectLastModified,
        ) -> So3Result<CasWriteOutcome> {
            let objects = self.objects.lock().await;
            let Some(current_record) = objects.get(key.as_str()).cloned() else {
                return Ok(CasWriteOutcome::NotFound);
            };

            if current_record.version != expected_version {
                return Ok(CasWriteOutcome::Mismatch {
                    current_version: current_record.version,
                });
            }

            let next_version = current_record.version.next();
            drop(objects);

            let record = match metadata {
                BlobMetadata::Inline(value) => {
                    self.store_inline(key, next_version, value, last_modified)
                        .await
                }
                BlobMetadata::Stored {
                    blob_id,
                    content_length,
                    checksum,
                } => ObjectRecord {
                    key: key.clone(),
                    version: next_version,
                    blob_id,
                    content_length,
                    checksum,
                    last_modified,
                },
            };
            self.objects
                .lock()
                .await
                .insert(key.as_str().to_owned(), record.clone());
            Ok(CasWriteOutcome::Applied(record))
        }

        async fn delete(&self, key: &ObjectKey) -> So3Result<()> {
            self.objects.lock().await.remove(key.as_str());
            Ok(())
        }
    }

    fn sha256_hex(value: &[u8]) -> String {
        let digest = Sha256::digest(value);
        let mut out = String::with_capacity(digest.len() * 2);
        for byte in digest {
            let _ = FmtWrite::write_fmt(&mut out, format_args!("{byte:02x}"));
        }
        out
    }

    #[tokio::test]
    async fn execute_returns_written_object() {
        let state_machine = LocalStateMachine::new(InMemoryRepository::new());

        let result = state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(KEY_ALPHA).unwrap(),
                metadata: BlobMetadata::Inline(WRITE_VALUE.to_vec()),
                last_modified: test_last_modified(),
            }))
            .await
            .unwrap();

        match result {
            ObjectResult::Write(write) => {
                assert_eq!(write.record.version, ObjectVersion::initial());
                assert_eq!(write.record.content_length, WRITE_VALUE.len() as u64);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_reports_cas_mismatch() {
        let state_machine = LocalStateMachine::new(InMemoryRepository::new());
        state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(KEY_ALPHA).unwrap(),
                metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
                last_modified: test_last_modified(),
            }))
            .await
            .unwrap();

        let result = state_machine
            .execute(ObjectCommand::Cas(CasCommand {
                key: ObjectKey::new(KEY_ALPHA).unwrap(),
                expected_version: ObjectVersion::try_from(STALE_VERSION).unwrap(),
                metadata: BlobMetadata::Inline(SECOND_VALUE.to_vec()),
                last_modified: test_last_modified(),
            }))
            .await
            .unwrap();

        assert_eq!(
            result,
            ObjectResult::Cas(CasResult::Mismatch {
                current_version: ObjectVersion::initial(),
            })
        );
    }

    #[tokio::test]
    async fn execute_read_returns_none_for_missing_key() {
        let state_machine = LocalStateMachine::new(InMemoryRepository::new());

        let result = state_machine
            .execute(ObjectCommand::Read(ReadCommand {
                key: ObjectKey::new(MISSING_KEY).unwrap(),
            }))
            .await
            .unwrap();

        assert_eq!(result, ObjectResult::Read(ReadResult { metadata: None }));
    }

    fn test_last_modified() -> crate::domain::ObjectLastModified {
        const TEST_LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;
        crate::domain::ObjectLastModified::try_from(TEST_LAST_MODIFIED_UNIX_MILLIS).unwrap()
    }
}
