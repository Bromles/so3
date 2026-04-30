use crate::api::rpc::consensus_transport_service::ConsensusTransportService;
use crate::api::rpc::RpcApi;
use crate::domain::error::{So3Error, So3Result};
use crate::proto::consensus_transport_server::ConsensusTransportServer;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;

pub struct TonicRpcServer {}

impl TonicRpcServer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RpcApi for TonicRpcServer {
    async fn start(
        self,
        listener: TcpListener,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "rpc server started");
        let mut server = Server::builder();

        server
            .add_service(ConsensusTransportServer::new(
                ConsensusTransportService::new(),
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                cancellation_token.cancelled().await;
            })
            .await
            .map_err(|error| So3Error::Io(error.to_string()))
    }
}
