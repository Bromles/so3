use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::{ObjectKey, ObjectVersion, StoredObject};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasWriteOutcome {
    Applied(StoredObject),
    NotFound,
    Mismatch { current_version: ObjectVersion },
}

#[async_trait]
pub trait ObjectRepository: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the repository cannot load committed object state.
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>>;

    /// # Errors
    ///
    /// Returns an error when the repository cannot durably commit a new object version.
    async fn write(&self, key: &ObjectKey, value: Vec<u8>) -> So3Result<StoredObject>;

    /// # Errors
    ///
    /// Returns an error when the repository cannot evaluate or durably apply the CAS command.
    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasWriteOutcome>;
}
