use crate::proto::consensus_transport_server::ConsensusTransport;
use crate::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use async_trait::async_trait;
use tonic::{Request, Response, Status};

pub struct ConsensusTransportService {}

impl ConsensusTransportService {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ConsensusTransport for ConsensusTransportService {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        todo!()
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        todo!()
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        todo!()
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        todo!()
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        todo!()
    }
}
