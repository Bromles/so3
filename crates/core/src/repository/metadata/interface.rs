use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::object::ObjectMetadata;
use crate::domain::object_key::ObjectKey;

#[async_trait]
pub trait ObjectMetadataRepository {
    /// # Errors
    ///
    /// Returns an error when metadata cannot be loaded from durable repository.
    async fn read(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>>;

    /// # Errors
    ///
    /// Returns an error when metadata cannot be durably written.
    async fn write(&self, metadata: &ObjectMetadata) -> So3Result<()>;

    /// # Errors
    ///
    /// Returns an error when the metadata record cannot be removed from durable repository.
    async fn delete(&self, key: &ObjectKey) -> So3Result<()>;
}
