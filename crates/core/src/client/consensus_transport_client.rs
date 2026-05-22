use crate::client::interface::ConsensusPeerClient;
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use crate::proto::consensus::consensus_transport_client::ConsensusTransportClient as ProtoClient;
use crate::proto::mappers::{
    accept_req_to_proto, accept_res_to_domain, apply_req_to_proto, apply_res_to_domain,
    commit_req_to_proto, commit_res_to_domain, map_tonic_status, pre_accept_req_to_proto,
    pre_accept_res_to_domain, recover_req_to_proto, recover_res_to_domain,
};
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::timeout;
use tonic::Response;
use tonic::transport::{Channel, Endpoint};

pub struct ConsensusTransportClient {
    channel: Channel,
    rpc_deadline: Duration,
}

impl ConsensusTransportClient {
    pub fn new(endpoint: String) -> So3Result<Self> {
        Self::with_deadline(endpoint, Duration::from_secs(3))
    }

    pub fn with_deadline(endpoint: String, rpc_deadline: Duration) -> So3Result<Self> {
        let channel = Endpoint::from_shared(endpoint)?.connect_lazy();
        Ok(Self {
            channel,
            rpc_deadline,
        })
    }

    fn raw_client(&self) -> ProtoClient<Channel> {
        ProtoClient::new(self.channel.clone())
    }
}

#[async_trait]
impl ConsensusPeerClient for ConsensusTransportClient {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        let mut client = self.raw_client();
        let req = pre_accept_req_to_proto(req);
        timeout(self.rpc_deadline, client.pre_accept(req))
            .await
            .map_err(|_| {
                crate::domain::error::So3Error::PeerUnavailable(
                    "pre_accept RPC deadline exceeded".into(),
                )
            })?
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(pre_accept_res_to_domain)?
    }

    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        let mut client = self.raw_client();
        let req = accept_req_to_proto(req);
        timeout(self.rpc_deadline, client.accept(req))
            .await
            .map_err(|_| {
                crate::domain::error::So3Error::PeerUnavailable(
                    "accept RPC deadline exceeded".into(),
                )
            })?
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(accept_res_to_domain)?
    }

    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        let mut client = self.raw_client();
        let req = commit_req_to_proto(req);
        timeout(self.rpc_deadline, client.commit(req))
            .await
            .map_err(|_| {
                crate::domain::error::So3Error::PeerUnavailable(
                    "commit RPC deadline exceeded".into(),
                )
            })?
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(commit_res_to_domain)?
    }

    async fn apply(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        let mut client = self.raw_client();
        let req = apply_req_to_proto(req);
        timeout(self.rpc_deadline, client.apply(req))
            .await
            .map_err(|_| {
                crate::domain::error::So3Error::PeerUnavailable(
                    "apply RPC deadline exceeded".into(),
                )
            })?
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(apply_res_to_domain)?
    }

    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let mut client = self.raw_client();
        let req = recover_req_to_proto(req);
        timeout(self.rpc_deadline, client.recover(req))
            .await
            .map_err(|_| {
                crate::domain::error::So3Error::PeerUnavailable(
                    "recover RPC deadline exceeded".into(),
                )
            })?
            .map_err(map_tonic_status)
            .map(Response::into_inner)
            .map(recover_res_to_domain)?
    }
}
