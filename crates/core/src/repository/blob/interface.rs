use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::error::So3Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait BlobRepository: Send + Sync + 'static {
    async fn append_chunk(&self, blob_id: &BlobId, chunk: Bytes) -> So3Result<()>;
    async fn commit(&self, blob_id: &BlobId) -> So3Result<()>;
    async fn abort(&self, blob_id: &BlobId) -> So3Result<()>;
    async fn open_reader(&self, blob_id: &BlobId) -> So3Result<BlobStream>;
    async fn exists(&self, blob_id: &BlobId) -> So3Result<bool>;
    async fn delete(&self, blob_id: &BlobId) -> So3Result<()>;
}
