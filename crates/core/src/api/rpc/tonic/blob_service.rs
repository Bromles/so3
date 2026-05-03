use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::proto::blob::blob_service_server::BlobService as ProtoBlobService;
use crate::proto::blob::store_blob_request::Payload;
use crate::proto::blob::{
    FetchBlobRequest, FetchBlobResponse, StoreBlobRequest, StoreBlobResponse,
};
use crate::use_case::blob::BlobUseCase;
use async_trait::async_trait;
use bytes::Bytes;
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

        // Header
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

        // Chunks + Footer
        let mut chunks: Vec<Bytes> = Vec::new();
        let mut footer_sha256: Option<Sha256Digest> = None;

        while let Some(msg) = stream.next().await {
            match msg.map_err(|e| Status::internal(e.to_string()))?.payload {
                Some(Payload::Chunk(c)) => {
                    let expected = Sha256Digest::try_from(c.sha256)
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    if Sha256Digest::compute(&c.chunk) != expected {
                        return Err(Status::data_loss("chunk sha256 mismatch"));
                    }
                    chunks.push(c.chunk);
                }
                Some(Payload::Footer(f)) => {
                    footer_sha256 = Some(
                        Sha256Digest::try_from(f.sha256)
                            .map_err(|e| Status::invalid_argument(e.to_string()))?,
                    );
                    break;
                }
                _ => return Err(Status::invalid_argument("unexpected message in stream")),
            }
        }

        let sha256 = footer_sha256.ok_or_else(|| Status::invalid_argument("missing footer"))?;

        let already_existed = self
            .blob_use_case
            .store(blob_id, header.size, sha256, tokio_stream::iter(chunks))
            .await
            .map_err(Status::from)?;

        Ok(Response::new(StoreBlobResponse { already_existed }))
    }

    type FetchBlobStream = Pin<Box<dyn Stream<Item = Result<FetchBlobResponse, Status>> + Send>>;

    async fn fetch_blob(
        &self,
        request: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        let blob_id = BlobId::try_from(request.into_inner().blob_id.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let payload = self
            .blob_use_case
            .fetch(&blob_id)
            .await
            .map_err(Status::from)?;

        let bytes = payload.as_bytes().clone();
        let chunk_size = 64 * 1024;

        let stream = tokio_stream::iter((0..bytes.len()).step_by(chunk_size).map(move |offset| {
            let end = (offset + chunk_size).min(bytes.len());
            Ok(FetchBlobResponse {
                chunk: bytes.slice(offset..end),
            })
        }));

        Ok(Response::new(Box::pin(stream)))
    }
}
