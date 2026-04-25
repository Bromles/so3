use async_trait::async_trait;

use crate::domain::error::So3Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMetadata {
    pub blob_id: String,
    pub content_length: u64,
    pub checksum: String,
}

#[async_trait]
pub trait BlobRepository: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when blob bytes cannot be durably staged and committed.
    async fn store(&self, value: &[u8]) -> So3Result<BlobMetadata>;

    /// # Errors
    ///
    /// Returns an error when the committed blob is missing or cannot be read.
    async fn load(&self, blob_id: &str) -> So3Result<Vec<u8>>;
}
