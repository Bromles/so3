use crate::api::rpc::RpcApi;
use crate::api::rpc::tonic::blob_service::BlobService;
use crate::api::rpc::tonic::consensus_transport_service::ConsensusTransportService;
use crate::domain::error::{So3Error, So3Result};
use crate::proto::blob::blob_service_server::BlobServiceServer;
use crate::proto::consensus::consensus_transport_server::ConsensusTransportServer;
use crate::use_case::blob::BlobUseCase;
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;

pub struct TonicRpcServer {}

impl TonicRpcServer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl RpcApi for TonicRpcServer {
    async fn start<I: InboundConsensusUseCase, B: BlobUseCase>(
        self,
        listener: TcpListener,
        cancellation_token: CancellationToken,
        inbound_consensus_use_case: Arc<I>,
        blob_use_case: Arc<B>,
    ) -> So3Result<()> {
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "rpc server started");
        let mut server = Server::builder();

        server
            .add_service(ConsensusTransportServer::new(
                ConsensusTransportService::new(inbound_consensus_use_case),
            ))
            .add_service(BlobServiceServer::new(BlobService::new(blob_use_case)))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                cancellation_token.cancelled().await;
            })
            .await
            .map_err(|error| So3Error::Io(error.to_string()))
    }
}
