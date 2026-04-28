use async_trait::async_trait;
use tonic::Status;

use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    FetchBlobRequest, FetchBlobResponse, PreAcceptRequest, PreAcceptResponse, RecoverRequest,
    RecoverResponse,
};

#[async_trait]
pub trait ConsensusTransportHandler: Send + Sync {
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status>;
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status>;
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status>;
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status>;
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status>;
    async fn fetch_blob(&self, request: FetchBlobRequest) -> Result<FetchBlobResponse, Status>;
}
