use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::error::{So3Error, So3Result};
use crate::proto::blob::blob_service_client::BlobServiceClient as ProtoClient;
use crate::proto::blob::store_blob_request::Payload;
use crate::proto::blob::{
    FetchBlobRequest, StoreBlobChunk, StoreBlobFooter, StoreBlobHeader, StoreBlobRequest,
};
use async_trait::async_trait;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};

pub struct BlobClient {
    channel: Channel,
}

impl BlobClient {
    pub fn new(endpoint: String) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|e| So3Error::InvalidRequest(e.to_string()))?
            .connect_lazy();
        Ok(Self { channel })
    }

    fn raw_client(&self) -> ProtoClient<Channel> {
        ProtoClient::new(self.channel.clone())
    }
}

#[async_trait]
impl BlobPeerClient for BlobClient {
    async fn push(
        &self,
        blob_id: BlobId,
        size: u64,
        sha256: Sha256Digest,
        data: BlobStream,
    ) -> So3Result<()> {
        let header = tokio_stream::iter(std::iter::once(StoreBlobRequest {
            payload: Some(Payload::Header(StoreBlobHeader {
                blob_id: blob_id.to_string(),
                size,
            })),
        }));

        let footer = tokio_stream::iter(std::iter::once(StoreBlobRequest {
            payload: Some(Payload::Footer(StoreBlobFooter {
                sha256: sha256.as_bytes().to_vec().into(),
            })),
        }));

        let chunks = data.take_while(std::result::Result::is_ok).map(|r| {
            let chunk = r.unwrap();
            StoreBlobRequest {
                payload: Some(Payload::Chunk(StoreBlobChunk {
                    sha256: Sha256Digest::compute(&chunk).as_bytes().to_vec().into(),
                    chunk,
                })),
            }
        });

        self.raw_client()
            .store_blob(header.chain(chunks).chain(footer))
            .await
            .map_err(|s| So3Error::PeerUnavailable(s.to_string()))?;

        Ok(())
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobStream> {
        let stream = self
            .raw_client()
            .fetch_blob(FetchBlobRequest {
                blob_id: blob_id.to_string(),
            })
            .await
            .map_err(|s| So3Error::PeerUnavailable(s.to_string()))?
            .into_inner()
            .map(|r| {
                r.map(|resp| resp.chunk)
                    .map_err(|s| So3Error::PeerUnavailable(s.to_string()))
            });

        Ok(BlobStream::new(stream))
    }
}
