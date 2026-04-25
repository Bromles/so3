use crate::consensus::state_machine::ObjectCommandExecutor;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{
    CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand,
    StoredObject, WriteCommand,
};

#[derive(Clone)]
pub struct ObjectService<E: ObjectCommandExecutor> {
    executor: E,
}

impl<E: ObjectCommandExecutor> ObjectService<E> {
    #[must_use]
    pub fn new(executor: E) -> Self {
        Self { executor }
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Read` command.
    pub async fn read(&self, key: ObjectKey) -> So3Result<Option<StoredObject>> {
        match self
            .executor
            .execute_command(ObjectCommand::Read(ReadCommand { key }))
            .await?
        {
            ObjectResult::Read(result) => Ok(result.object),
            result => unexpected_result("Read", &result),
        }
    }

    /// # Errors
    ///
    /// Returns any error from the state machine while executing the deterministic `Write` command.
    pub async fn write(&self, key: ObjectKey, value: Vec<u8>) -> So3Result<StoredObject> {
        match self
            .executor
            .execute_command(ObjectCommand::Write(WriteCommand { key, value }))
            .await?
        {
            ObjectResult::Write(result) => Ok(result.object),
            result => unexpected_result("Write", &result),
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
        match self
            .executor
            .execute_command(ObjectCommand::Cas(CasCommand {
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
    use tempfile::TempDir;

    use super::ObjectService;
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::domain::{CasResult, ObjectKey, ObjectVersion};
    use crate::storage::registry::SqliteFsPersistentObjectRepository;

    const MISSING_KEY: &str = "missing";
    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const SECOND_VALUE: &[u8] = b"second";
    const INITIAL_VERSION: i64 = 1;
    const NEXT_VERSION: i64 = 2;
    const VERSION_INCREMENT: i64 = 1;

    async fn test_service() -> (
        ObjectService<LocalStateMachine<SqliteFsPersistentObjectRepository>>,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let state_machine = LocalStateMachine::new(repository);
        (ObjectService::new(state_machine), temp_dir)
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

        assert_eq!(first.record.version.get(), INITIAL_VERSION);
        assert_eq!(second.record.version.get(), NEXT_VERSION);
        assert_eq!(second.value, SECOND_VALUE.to_vec());
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
                ObjectVersion::try_from(written.record.version.get() + VERSION_INCREMENT).unwrap(),
                SECOND_VALUE.to_vec(),
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
