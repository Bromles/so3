use async_trait::async_trait;
use crate::domain::consensus::transport::{AcceptRequest, AcceptResponse, CommitRequest, CommitResponse, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse};
use crate::domain::error::So3Result;
use crate::domain::node::NodeId;

#[async_trait]
pub trait ConsensusPeerClient: Send + Sync {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse>;
    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse>;
    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse>;
    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse>;
}