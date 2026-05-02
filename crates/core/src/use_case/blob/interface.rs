use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::error::So3Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio_stream::Stream;

#[async_trait]
pub trait BlobUseCase {
    async fn store(
        &self,
        blob_id: BlobId,
        size: u64,
        sha256: Sha256Digest,
        chunks: impl Stream<Item=Bytes>,
    ) -> So3Result<bool>;
}
