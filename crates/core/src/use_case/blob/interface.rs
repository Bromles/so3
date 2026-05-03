use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::error::So3Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio_stream::Stream;
use crate::domain::blob::payload::BlobPayload;

#[async_trait]
pub trait BlobUseCase: Send + Sync + 'static {
    async fn store(
        &self,
        blob_id: BlobId,
        size: u64,
        sha256: Sha256Digest,
        chunks: impl Stream<Item=Bytes> + Send,
    ) -> So3Result<bool>;

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload>;
}
