use crate::proto::blob::blob_service_server::BlobService as ProtoBlobService;
use crate::proto::blob::{
    FetchBlobRequest, FetchBlobResponse, StoreBlobRequest, StoreBlobResponse,
};
use crate::use_case::blob::BlobUseCase;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

pub struct BlobService<B: BlobUseCase> {
    blob_use_case: Arc<B>,
}

impl<B: BlobUseCase> BlobService<B> {
    pub fn new(blob_use_case: Arc<B>) -> Self {
        Self { blob_use_case }
    }
}

#[async_trait]
impl<B: BlobUseCase> ProtoBlobService for BlobService<B> {
    async fn store_blob(
        &self,
        request: Request<Streaming<StoreBlobRequest>>,
    ) -> Result<Response<StoreBlobResponse>, Status> {
        todo!()
    }

    type FetchBlobStream = Pin<Box<dyn Stream<Item = Result<FetchBlobResponse, Status>> + Send>>;

    async fn fetch_blob(
        &self,
        request: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        todo!()
    }
}
