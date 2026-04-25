use std::sync::Arc;

use crate::consensus::state_machine::LocalStateMachine;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{
    CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand,
    StoredObject, WriteCommand,
};

#[derive(Clone)]
pub struct ObjectService {
    state_machine: Arc<LocalStateMachine>,
}

impl ObjectService {
    #[must_use]
    pub fn new(state_machine: Arc<LocalStateMachine>) -> Self {
        Self { state_machine }
    }

    /// # Errors
    ///
    /// Propagates state machine failures while executing the deterministic `Read` command.
    pub async fn read(&self, key: ObjectKey) -> So3Result<Option<StoredObject>> {
        match self
            .state_machine
            .execute(ObjectCommand::Read(ReadCommand { key }))
            .await?
        {
            ObjectResult::Read(result) => Ok(result.object),
            result => unexpected_result("Read", &result),
        }
    }

    /// # Errors
    ///
    /// Propagates state machine failures while executing the deterministic `Write` command.
    pub async fn write(&self, key: ObjectKey, value: Vec<u8>) -> So3Result<StoredObject> {
        match self
            .state_machine
            .execute(ObjectCommand::Write(WriteCommand { key, value }))
            .await?
        {
            ObjectResult::Write(result) => Ok(result.object),
            result => unexpected_result("Write", &result),
        }
    }

    /// # Errors
    ///
    /// Propagates state machine failures while executing the deterministic `Cas` command.
    pub async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasResult> {
        match self
            .state_machine
            .execute(ObjectCommand::Cas(CasCommand {
                key,
                expected_version,
                value,
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
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::ObjectService;
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::domain::{CasResult, ObjectKey, ObjectVersion};
    use crate::storage::persistent_object_repository::PersistentObjectRepository;

    async fn test_service() -> (ObjectService, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repository = Arc::new(
            PersistentObjectRepository::new(
                temp_dir.path().join("metadata"),
                temp_dir.path().join("blobs"),
            )
            .await
            .unwrap(),
        );
        let state_machine = Arc::new(LocalStateMachine::new(repository));
        (ObjectService::new(state_machine), temp_dir)
    }

    #[tokio::test]
    async fn read_returns_none_for_missing_key() {
        let (service, _temp_dir) = test_service().await;

        let loaded = service.read(ObjectKey::new("missing").unwrap()).await.unwrap();

        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn write_persists_and_increments_version() {
        let (service, _temp_dir) = test_service().await;
        let key = ObjectKey::new("alpha").unwrap();

        let first = service.write(key.clone(), b"first".to_vec()).await.unwrap();
        let second = service.write(key, b"second".to_vec()).await.unwrap();

        assert_eq!(first.record.version.get(), 1);
        assert_eq!(second.record.version.get(), 2);
        assert_eq!(second.value, b"second".to_vec());
    }

    #[tokio::test]
    async fn cas_returns_structured_mismatch() {
        let (service, _temp_dir) = test_service().await;
        let key = ObjectKey::new("alpha").unwrap();
        let written = service.write(key.clone(), b"first".to_vec()).await.unwrap();

        let result = service
            .cas(
                key,
                ObjectVersion::try_from(written.record.version.get() + 1).unwrap(),
                b"second".to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            CasResult::Mismatch {
                current_version: written.record.version,
            }
        );
    }
}
