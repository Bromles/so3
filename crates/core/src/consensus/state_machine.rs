use crate::domain::error::So3Result;
use crate::domain::{CasResult, ObjectCommand, ObjectResult, ReadResult, WriteResult};
use crate::storage::object::repository::{CasWriteOutcome, ObjectRepository};

#[derive(Clone)]
pub struct LocalStateMachine<R: ObjectRepository> {
    repository: R,
}

impl<R: ObjectRepository> LocalStateMachine<R> {
    #[must_use]
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    /// # Errors
    ///
    /// Returns any storage error raised while executing the deterministic object command.
    pub async fn execute(&self, command: ObjectCommand) -> So3Result<ObjectResult> {
        match command {
            ObjectCommand::Read(command) => {
                let object = self.repository.read(&command.key).await?;
                Ok(ObjectResult::Read(ReadResult { object }))
            }
            ObjectCommand::Write(command) => {
                let object = self.repository.write(&command.key, command.value).await?;
                Ok(ObjectResult::Write(WriteResult { object }))
            }
            ObjectCommand::Cas(command) => match self
                .repository
                .cas(&command.key, command.expected_version, command.value)
                .await?
            {
                CasWriteOutcome::Applied(object) => {
                    Ok(ObjectResult::Cas(CasResult::Applied(object)))
                }
                CasWriteOutcome::NotFound => Ok(ObjectResult::Cas(CasResult::NotFound)),
                CasWriteOutcome::Mismatch { current_version } => {
                    Ok(ObjectResult::Cas(CasResult::Mismatch { current_version }))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::LocalStateMachine;
    use crate::domain::error::So3Result;
    use crate::domain::{
        CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectRecord, ObjectResult, ObjectVersion,
        ReadCommand, ReadResult, StoredObject, WriteCommand,
    };
    use crate::storage::object::repository::{CasWriteOutcome, ObjectRepository};

    const KEY_ALPHA: &str = "alpha";
    const MISSING_KEY: &str = "missing";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const WRITE_VALUE: &[u8] = b"value";
    const CHECKSUM: &str = "checksum";
    const STALE_VERSION: i64 = 99;

    struct InMemoryRepository {
        objects: Mutex<HashMap<String, StoredObject>>,
    }

    impl InMemoryRepository {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl ObjectRepository for InMemoryRepository {
        async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
            Ok(self.objects.lock().await.get(key.as_str()).cloned())
        }

        async fn write(&self, key: &ObjectKey, value: Vec<u8>) -> So3Result<StoredObject> {
            let mut objects = self.objects.lock().await;
            let version = objects
                .get(key.as_str())
                .map_or_else(ObjectVersion::initial, |object| {
                    object.record.version.next()
                });
            let object = stored_object(key.clone(), version, value);
            objects.insert(key.as_str().to_owned(), object.clone());
            Ok(object)
        }

        async fn cas(
            &self,
            key: &ObjectKey,
            expected_version: ObjectVersion,
            value: Vec<u8>,
        ) -> So3Result<CasWriteOutcome> {
            let mut objects = self.objects.lock().await;
            let Some(current) = objects.get(key.as_str()).cloned() else {
                return Ok(CasWriteOutcome::NotFound);
            };

            if current.record.version != expected_version {
                return Ok(CasWriteOutcome::Mismatch {
                    current_version: current.record.version,
                });
            }

            let object = stored_object(key.clone(), current.record.version.next(), value);
            objects.insert(key.as_str().to_owned(), object.clone());
            Ok(CasWriteOutcome::Applied(object))
        }
    }

    fn stored_object(key: ObjectKey, version: ObjectVersion, value: Vec<u8>) -> StoredObject {
        StoredObject {
            record: ObjectRecord {
                key,
                version,
                blob_id: format!("blob-{}", version.get()),
                content_length: value.len() as u64,
                checksum: CHECKSUM.to_owned(),
            },
            value,
        }
    }

    #[tokio::test]
    async fn execute_returns_written_object() {
        let state_machine = LocalStateMachine::new(InMemoryRepository::new());

        let result = state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new(KEY_ALPHA).unwrap(),
                value: WRITE_VALUE.to_vec(),
            }))
            .await
            .unwrap();

        match result {
            ObjectResult::Write(write) => {
                assert_eq!(write.object.record.version, ObjectVersion::initial());
                assert_eq!(write.object.value, WRITE_VALUE.to_vec());
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
                value: FIRST_VALUE.to_vec(),
            }))
            .await
            .unwrap();

        let result = state_machine
            .execute(ObjectCommand::Cas(CasCommand {
                key: ObjectKey::new(KEY_ALPHA).unwrap(),
                expected_version: ObjectVersion::try_from(STALE_VERSION).unwrap(),
                value: SECOND_VALUE.to_vec(),
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

        assert_eq!(result, ObjectResult::Read(ReadResult { object: None }));
    }
}
