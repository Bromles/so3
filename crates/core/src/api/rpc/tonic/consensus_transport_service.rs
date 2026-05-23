use crate::proto::consensus::consensus_transport_server::ConsensusTransport;
use crate::proto::consensus::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::proto::mappers::{
    accept_req_to_domain, accept_res_to_proto, apply_req_to_domain, apply_res_to_proto,
    commit_req_to_domain, commit_res_to_proto, pre_accept_req_to_domain, pre_accept_res_to_proto,
    recover_req_to_domain, recover_res_to_proto,
};
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ConsensusTransportService<I: InboundConsensusUseCase> {
    inbound_consensus_use_case: Arc<I>,
}

impl<I: InboundConsensusUseCase> ConsensusTransportService<I> {
    pub fn new(inbound_consensus_use_case: Arc<I>) -> Self {
        Self {
            inbound_consensus_use_case,
        }
    }
}

#[async_trait]
impl<I: InboundConsensusUseCase> ConsensusTransport for ConsensusTransportService<I> {
    async fn pre_accept(
        &self,
        request: Request<PreAcceptRequest>,
    ) -> Result<Response<PreAcceptResponse>, Status> {
        let domain_req = pre_accept_req_to_domain(request.into_inner())?;

        let domain_res = self
            .inbound_consensus_use_case
            .pre_accept(domain_req)
            .await?;

        Ok(Response::new(pre_accept_res_to_proto(&domain_res)))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        let domain_req = accept_req_to_domain(request.into_inner())?;

        let domain_res = self.inbound_consensus_use_case.accept(domain_req).await?;

        Ok(Response::new(accept_res_to_proto(&domain_res)))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let domain_req = commit_req_to_domain(request.into_inner())?;

        let domain_res = self.inbound_consensus_use_case.commit(domain_req).await?;

        Ok(Response::new(commit_res_to_proto(domain_res)))
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        let domain_req = apply_req_to_domain(request.into_inner())?;

        let domain_res = self.inbound_consensus_use_case.apply(domain_req).await?;

        Ok(Response::new(apply_res_to_proto(domain_res)))
    }

    async fn recover(
        &self,
        request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        let domain_req = recover_req_to_domain(request.into_inner())?;

        let domain_res = self.inbound_consensus_use_case.recover(domain_req).await?;

        Ok(Response::new(recover_res_to_proto(domain_res)))
    }
}
