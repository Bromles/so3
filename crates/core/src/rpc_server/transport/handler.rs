use async_trait::async_trait;
use tonic::Status;


#[async_trait]
pub trait ConsensusTransportHandler: Send + Sync {
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status>;
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status>;
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status>;
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status>;
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status>;
}
