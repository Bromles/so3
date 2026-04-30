use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusService {
    async fn pre_accept(&self, request: PreAcceptRequest) -> So3Result<PreAcceptResponse>;
    async fn accept(&self, request: AcceptRequest) -> So3Result<AcceptResponse>;
    async fn commit(&self, request: CommitRequest) -> So3Result<CommitResponse>;
    async fn apply(&self, request: ApplyRequest) -> So3Result<ApplyResponse>;
    async fn recover(&self, request: RecoverRequest) -> So3Result<RecoverResponse>;
}
