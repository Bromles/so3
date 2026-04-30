use tonic::{async_trait, Request, Response, Status};

use crate::proto::consensus_transport_server::ConsensusTransport;
use crate::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest,
    RecoverResponse,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

pub struct ConsensusTransportService<H: ConsensusTransportHandler> {
    handler: H,
}

impl<H: ConsensusTransportHandler> ConsensusTransportService<H> {
    #[must_use]
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H> ConsensusTransport for ConsensusTransportService<H>
where
    H: ConsensusTransportHandler + 'static,
{
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        self.handler
            .pre_accept(request.into_inner())
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

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        self.handler
            .commit(request.into_inner())
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
