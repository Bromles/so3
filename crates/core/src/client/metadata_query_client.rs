use crate::client::interface::MetadataQueryClient;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::proto::mappers::map_tonic_status;
use crate::proto::metadata_query::metadata_query_client::MetadataQueryClient as ProtoClient;
use crate::proto::metadata_query::GetMetadataRequest as ProtoRequest;
use crate::proto::metadata_query_mappers::proto_response_to_metadata_option;
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint};
use tonic::Response;

pub struct MetadataQueryTonicClient {
    channel: Channel,
    rpc_deadline: Duration,
}

impl MetadataQueryTonicClient {
    pub fn new(endpoint: String, rpc_deadline: Duration) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)?.connect_lazy();
        Ok(Self {
            channel,
            rpc_deadline,
        })
    }
}

#[async_trait]
impl MetadataQueryClient for MetadataQueryTonicClient {
    async fn get_metadata(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        let mut client = ProtoClient::new(self.channel.clone());
        let req = ProtoRequest {
            key: key.as_ref().to_string(),
        };
        let proto_res = timeout(self.rpc_deadline, client.get_metadata(req))
            .await
            .map_err(|_| So3Error::PeerUnavailable("get_metadata RPC deadline exceeded".into()))?
            .map_err(map_tonic_status)
            .map(Response::into_inner)?;

        proto_response_to_metadata_option(proto_res)
    }
}
