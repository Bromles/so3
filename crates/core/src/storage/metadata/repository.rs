use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::{ObjectKey, ObjectRecord};

#[async_trait]
pub trait ObjectMetadataRepository: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when metadata cannot be loaded from durable storage.
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<ObjectRecord>>;

    /// # Errors
    ///
    /// Returns an error when metadata cannot be durably written.
    async fn write(&self, record: &ObjectRecord) -> So3Result<()>;

    /// # Errors
    ///
    /// Returns an error when the metadata record cannot be removed from durable storage.
    async fn delete(&self, key: &ObjectKey) -> So3Result<()>;
}
