use crate::client::interface::ConsensusPeerClient;
use crate::client::mappers::{
    accept_req_to_proto, accept_res_to_domain, commit_req_to_proto, commit_res_to_domain,
    map_tonic_status, pre_accept_req_to_proto, pre_accept_res_to_domain, recover_req_to_proto,
    recover_res_to_domain,
};
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, CommitRequest, CommitResponse, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use crate::proto::consensus_transport_client::ConsensusTransportClient;
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tonic::Response;

pub struct TonicPeerClient {
    channel: Channel,
}

impl TonicPeerClient {
    pub async fn new(endpoint: String) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)?.connect().await?;

        Ok(Self { channel })
    }

    fn raw_client(&self) -> ConsensusTransportClient<Channel> {
        ConsensusTransportClient::new(self.channel.clone())
    }
}

#[async_trait]
impl ConsensusPeerClient for TonicPeerClient {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        let mut client = self.raw_client();

        let req = pre_accept_req_to_proto(req);

        client
            .pre_accept(req)
            .await
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(pre_accept_res_to_domain)
    }

    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        let mut client = self.raw_client();

        let req = accept_req_to_proto(req);

        client
            .accept(req)
            .await
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(accept_res_to_domain)
    }

    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        let mut client = self.raw_client();

        let req = commit_req_to_proto(req);

        client
            .commit(req)
            .await
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(commit_res_to_domain)
    }

    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let mut client = self.raw_client();

        let req = recover_req_to_proto(req);

        client
            .recover(req)
            .await
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(recover_res_to_domain)
    }
}
