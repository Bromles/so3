use crate::client::interface::BlobPeerClient;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::error::So3Result;
use crate::domain::node::NodeId;
use crate::proto::blob::blob_service_client::BlobServiceClient as ProtoClient;
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};

pub struct BlobClient {
    channel: Channel,
}

impl BlobClient {
    fn raw_client(&self) -> ProtoClient<Channel> {
        ProtoClient::new(self.channel.clone())
    }
}

#[async_trait]
impl BlobPeerClient for BlobClient {
    async fn new(endpoint: String) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;

        Ok(Self { channel })
    }


    async fn push(&self, peer: &NodeId, blob_id: BlobId, payload: &BlobPayload) -> So3Result<()> {
        todo!()
    }
}
