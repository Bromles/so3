use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::{So3Error, So3Result};
use crate::proto::blob::blob_service_client::BlobServiceClient as ProtoClient;
use crate::proto::blob::store_blob_request::Payload;
use crate::proto::blob::{
    FetchBlobRequest, StoreBlobChunk, StoreBlobFooter, StoreBlobHeader, StoreBlobRequest,
};
use async_trait::async_trait;
use bytes::BytesMut;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

const CHUNK_SIZE: usize = 64 * 1024;

pub struct BlobClient {
    channel: Channel,
}

impl BlobClient {
    pub async fn new(endpoint: String) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|e| So3Error::InvalidRequest(e.to_string()))?
            .connect()
            .await
            .map_err(|e| So3Error::Io(e.to_string()))?;

        Ok(Self { channel })
    }

    fn raw_client(&self) -> ProtoClient<Channel> {
        ProtoClient::new(self.channel.clone())
    }
}

#[async_trait]
impl BlobPeerClient for BlobClient {
    async fn push(&self, blob_id: BlobId, payload: &BlobPayload) -> So3Result<()> {
        let bytes = payload.as_bytes().clone();
        let total_sha256 = Sha256Digest::compute(&bytes);

        let mut messages = Vec::with_capacity(2 + bytes.len().div_ceil(CHUNK_SIZE));

        messages.push(StoreBlobRequest {
            payload: Some(Payload::Header(StoreBlobHeader {
                blob_id: blob_id.to_string(),
                size: bytes.len() as u64,
            })),
        });

        for offset in (0..bytes.len()).step_by(CHUNK_SIZE) {
            let chunk = bytes.slice(offset..(offset + CHUNK_SIZE).min(bytes.len()));
            messages.push(StoreBlobRequest {
                payload: Some(Payload::Chunk(StoreBlobChunk {
                    sha256: Sha256Digest::compute(&chunk).as_bytes().to_vec().into(),
                    chunk,
                })),
            });
        }

        messages.push(StoreBlobRequest {
            payload: Some(Payload::Footer(StoreBlobFooter {
                sha256: total_sha256.as_bytes().to_vec().into(),
            })),
        });

        self.raw_client()
            .store_blob(tokio_stream::iter(messages))
            .await
            .map_err(|s| So3Error::PeerUnavailable(s.to_string()))?;

        Ok(())
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload> {
        let mut stream = self
            .raw_client()
            .fetch_blob(FetchBlobRequest {
                blob_id: blob_id.to_string(),
            })
            .await
            .map_err(|s| So3Error::PeerUnavailable(s.to_string()))?
            .into_inner();

        let mut buf = BytesMut::new();
        while let Some(response) = stream.next().await {
            let chunk = response
                .map_err(|s| So3Error::PeerUnavailable(s.to_string()))?
                .chunk;
            buf.extend_from_slice(&chunk);
        }

        Ok(BlobPayload::new(buf.freeze()))
    }
}
