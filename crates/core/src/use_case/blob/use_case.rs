use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::use_case::blob::BlobUseCase;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

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
    async fn exists(&self, blob_id: &BlobId) -> So3Result<bool> {
        self.blob_repository.exists(blob_id).await
    }

    async fn append_chunk(&self, blob_id: &BlobId, chunk: Bytes) -> So3Result<()> {
        self.blob_repository.append_chunk(blob_id, chunk).await
    }

    async fn commit(&self, blob_id: &BlobId) -> So3Result<()> {
        self.blob_repository.commit(blob_id).await
    }

    async fn abort(&self, blob_id: &BlobId) -> So3Result<()> {
        self.blob_repository.abort(blob_id).await
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobStream> {
        self.blob_repository.open_reader(blob_id).await
    }
}
