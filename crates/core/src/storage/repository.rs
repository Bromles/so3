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
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>>;
    async fn write(&self, key: &ObjectKey, value: Vec<u8>) -> So3Result<StoredObject>;
    async fn cas(
        &self,
        key: &ObjectKey,
        expected_version: ObjectVersion,
        value: Vec<u8>,
    ) -> So3Result<CasWriteOutcome>;
}
