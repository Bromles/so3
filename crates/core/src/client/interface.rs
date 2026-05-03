use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusPeerClient: Send + Sync + 'static {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse>;
    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse>;
    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse>;
    async fn apply(&self, req: ApplyRequest) -> So3Result<ApplyResponse>;
    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse>;
}

#[async_trait]
pub trait BlobPeerClient: Send + Sync + 'static {
    async fn push(&self, blob_id: BlobId, payload: &BlobPayload) -> So3Result<()>;
    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload>;
}
