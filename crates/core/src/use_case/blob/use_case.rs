use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::{So3Error, So3Result};
use crate::repository::blob::BlobRepository;
use crate::use_case::blob::BlobUseCase;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::pin;
use tokio_stream::{Stream, StreamExt};

pub struct BlobUseCaseImpl<BR: BlobRepository> {
    pub blob_repository: Arc<BR>,
}

impl<BR: BlobRepository> BlobUseCaseImpl<BR> {
    pub fn new(blob_repository: Arc<BR>) -> Self {
        Self { blob_repository }
    }
}

#[async_trait]
impl<BR: BlobRepository> BlobUseCase for BlobUseCaseImpl<BR> {
    async fn store(
        &self,
        blob_id: BlobId,
        size: u64,
        sha256: Sha256Digest,
        chunks: impl Stream<Item = Bytes> + Send,
    ) -> So3Result<bool> {
        if self.blob_repository.exists(&blob_id).await? {
            return Ok(true);
        }

        let mut buf = BytesMut::with_capacity(size as usize);
        pin!(chunks);
        while let Some(chunk) = chunks.next().await {
            buf.extend_from_slice(&chunk);
        }
        let bytes = buf.freeze();

        if bytes.len() as u64 != size {
            return Err(So3Error::InvalidRequest(format!(
                "blob size mismatch: expected {size}, got {}",
                bytes.len()
            )));
        }

        if Sha256Digest::compute(&bytes) != sha256 {
            return Err(So3Error::InvalidRequest("blob sha256 mismatch".to_string()));
        }

        self.blob_repository
            .store(&blob_id, &BlobPayload::new(bytes))
            .await?;

        Ok(false)
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload> {
        self.blob_repository.load(blob_id).await
    }
}
