use crate::domain::blob::checksum::{Sha256Digest, Sha256Hasher};
use crate::domain::blob::id::BlobId;
use crate::proto::blob::blob_service_server::BlobService as ProtoBlobService;
use crate::proto::blob::store_blob_request::Payload;
use crate::proto::blob::{
    FetchBlobRequest, FetchBlobResponse, StoreBlobRequest, StoreBlobResponse,
};
use crate::use_case::blob::BlobUseCase;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
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
        let mut stream = request.into_inner();

        let header = match stream.next().await {
            Some(Ok(msg)) => match msg.payload {
                Some(Payload::Header(h)) => h,
                _ => return Err(Status::invalid_argument("expected header as first message")),
            },
            Some(Err(e)) => return Err(e),
            None => return Err(Status::invalid_argument("empty stream")),
        };

        let blob_id = BlobId::try_from(header.blob_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        if self
            .blob_use_case
            .exists(&blob_id)
            .await
            .map_err(Status::from)?
        {
            return Ok(Response::new(StoreBlobResponse {
                already_existed: true,
            }));
        }

        let mut hasher = Sha256Hasher::new();
        let mut total: u64 = 0;
        let mut committed = false;

        while let Some(msg) = stream.next().await {
            match msg.map_err(|e| Status::internal(e.to_string()))?.payload {
                Some(Payload::Chunk(c)) => {
                    let expected_chunk = Sha256Digest::try_from(c.sha256)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    if Sha256Digest::compute(&c.chunk) != expected_chunk {
                        self.blob_use_case
                            .abort(&blob_id)
                            .await
                            .map_err(Status::from)?;
                        return Err(Status::data_loss("chunk sha256 mismatch"));
                    }
                    total = total.saturating_add(c.chunk.len() as u64);
                    if total > header.size {
                        self.blob_use_case
                            .abort(&blob_id)
                            .await
                            .map_err(Status::from)?;
                        return Err(Status::invalid_argument("blob exceeds declared size"));
                    }
                    hasher.update(&c.chunk);
                    self.blob_use_case
                        .append_chunk(&blob_id, c.chunk)
                        .await
                        .map_err(Status::from)?;
                }
                Some(Payload::Footer(f)) => {
                    let expected = Sha256Digest::try_from(f.sha256)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    let computed = hasher.finalize();
                    if computed != expected || total != header.size {
                        self.blob_use_case
                            .abort(&blob_id)
                            .await
                            .map_err(Status::from)?;
                        return Err(Status::data_loss("blob sha256 mismatch"));
                    }
                    self.blob_use_case
                        .commit(&blob_id)
                        .await
                        .map_err(Status::from)?;
                    committed = true;
                    break;
                }
                _ => {
                    self.blob_use_case
                        .abort(&blob_id)
                        .await
                        .map_err(Status::from)?;
                    return Err(Status::invalid_argument("unexpected message in stream"));
                }
            }
        }

        if !committed {
            self.blob_use_case
                .abort(&blob_id)
                .await
                .map_err(Status::from)?;
            return Err(Status::invalid_argument("missing footer"));
        }

        Ok(Response::new(StoreBlobResponse {
            already_existed: false,
        }))
    }

    type FetchBlobStream = Pin<Box<dyn Stream<Item = Result<FetchBlobResponse, Status>> + Send>>;

    async fn fetch_blob(
        &self,
        request: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        let blob_id = BlobId::try_from(request.into_inner().blob_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let stream = self
            .blob_use_case
            .fetch(&blob_id)
            .await
            .map_err(Status::from)?;

        let response_stream = stream.map(|r| {
            r.map(|chunk| FetchBlobResponse { chunk })
                .map_err(Status::from)
        });

        Ok(Response::new(Box::pin(response_stream)))
    }
}
