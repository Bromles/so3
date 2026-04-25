use std::sync::Arc;

use tonic::{Request, Response, Status, async_trait};

use crate::rpc_server::proto::consensus_transport_server::ConsensusTransport;
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

pub struct ConsensusTransportService {
    handler: Arc<dyn ConsensusTransportHandler>,
}

impl ConsensusTransportService {
    #[must_use]
    pub fn new(handler: Arc<dyn ConsensusTransportHandler>) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl ConsensusTransport for ConsensusTransportService {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        self.handler
            .pre_accept(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        self.handler
            .commit(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        self.handler
            .accept(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        self.handler
            .apply(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        self.handler
            .recover(request.into_inner())
            .await
            .map(Response::new)
    }
}
