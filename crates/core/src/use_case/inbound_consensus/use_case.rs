use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;

pub struct InboundConsensusUseCaseImpl {}

impl InboundConsensusUseCaseImpl {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl InboundConsensusUseCase for InboundConsensusUseCaseImpl {
    async fn pre_accept(&self, request: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        self.pre_accept_internal(request).await
    }

    async fn accept(&self, request: AcceptRequest) -> So3Result<AcceptResponse> {
        self.accept_internal(request).await
    }

    async fn commit(&self, request: CommitRequest) -> So3Result<CommitResponse> {
        self.commit_internal(request).await
    }

    async fn apply(&self, request: ApplyRequest) -> So3Result<ApplyResponse> {
        self.apply_internal(request).await
    }

    async fn recover(&self, request: RecoverRequest) -> So3Result<RecoverResponse> {
        self.recover_internal(request).await
    }
}
