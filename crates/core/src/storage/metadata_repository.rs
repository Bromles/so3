use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::{ObjectKey, ObjectRecord};

#[async_trait]
pub trait ObjectMetadataRepository: Send + Sync {
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>>;
    async fn write(&self, record: &ObjectRecord) -> So3Result<()>;
}
