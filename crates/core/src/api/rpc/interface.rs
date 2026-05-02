use crate::domain::error::So3Result;
use crate::use_case::blob::BlobUseCase;
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait RpcApi {
    async fn start<I: InboundConsensusUseCase, B: BlobUseCase>(
        self,
        listener: TcpListener,
        cancellation_token: CancellationToken,
        inbound_consensus_use_case: Arc<I>,
        blob_use_case: Arc<B>,
    ) -> So3Result<()>;
}
