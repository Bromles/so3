use std::sync::Arc;

use crate::domain::error::So3Result;
use crate::domain::types::{CasResult, ObjectCommand, ObjectResult, ReadResult, WriteResult};
use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

#[derive(Clone)]
pub struct LocalStateMachine {
    repository: Arc<dyn ObjectRepository>,
}

impl LocalStateMachine {
    pub fn new(repository: Arc<dyn ObjectRepository>) -> Self {
        Self { repository }
    }

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
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::LocalStateMachine;
    use crate::domain::error::So3Result;
    use crate::domain::types::{
        CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectRecord, ObjectResult, ObjectVersion,
        ReadCommand, StoredObject, WriteCommand,
    };
    use crate::storage::repository::{CasWriteOutcome, ObjectRepository};

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
                .map(|object| object.record.version.next())
                .unwrap_or_else(ObjectVersion::initial);
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
                checksum: "checksum".to_owned(),
                updated_at_unix_ms: 1,
            },
            value,
        }
    }

    #[tokio::test]
    async fn execute_returns_written_object() {
        let state_machine = LocalStateMachine::new(Arc::new(InMemoryRepository::new()));

        let result = state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new("alpha").unwrap(),
                value: b"value".to_vec(),
            }))
            .await
            .unwrap();

        match result {
            ObjectResult::Write(write) => {
                assert_eq!(write.object.record.version, ObjectVersion::initial());
                assert_eq!(write.object.value, b"value".to_vec());
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_reports_cas_mismatch() {
        let state_machine = LocalStateMachine::new(Arc::new(InMemoryRepository::new()));
        state_machine
            .execute(ObjectCommand::Write(WriteCommand {
                key: ObjectKey::new("alpha").unwrap(),
                value: b"first".to_vec(),
            }))
            .await
            .unwrap();

        let result = state_machine
            .execute(ObjectCommand::Cas(CasCommand {
                key: ObjectKey::new("alpha").unwrap(),
                expected_version: ObjectVersion::try_from(99).unwrap(),
                value: b"second".to_vec(),
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
        let state_machine = LocalStateMachine::new(Arc::new(InMemoryRepository::new()));

        let result = state_machine
            .execute(ObjectCommand::Read(ReadCommand {
                key: ObjectKey::new("missing").unwrap(),
            }))
            .await
            .unwrap();

        assert_eq!(
            result,
            ObjectResult::Read(crate::domain::types::ReadResult { object: None })
        );
    }
}
