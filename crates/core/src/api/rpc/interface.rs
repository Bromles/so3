use crate::domain::error::So3Result;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait RpcApi {
    async fn start(self, listener: TcpListener, cancellation_token: CancellationToken) -> So3Result<()>;
}