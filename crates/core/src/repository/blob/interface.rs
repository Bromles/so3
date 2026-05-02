use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait BlobRepository {
    /// # Errors
    ///
    /// Returns an error when blob bytes cannot be durably staged and committed.
    async fn store(&self, blob_id: &BlobId, payload: BlobPayload) -> So3Result<()>;

    /// # Errors
    ///
    /// Returns an error when the committed blob is missing or cannot be read.
    async fn load(&self, blob_id: &BlobId) -> So3Result<BlobPayload>;

    /// # Errors
    ///
    /// Returns an error when the existence check cannot be performed.
    async fn exists(&self, blob_id: &BlobId) -> So3Result<bool>;

    /// # Errors
    ///
    /// Returns an error when the committed blob cannot be removed from durable repository.
    async fn delete(&self, blob_id: &BlobId) -> So3Result<()>;
}
