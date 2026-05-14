use crate::domain::error::So3Result;
use crate::node::config::NodeConfig;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait S3Api {
    async fn start(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        cancellation_token: CancellationToken,
    ) -> So3Result<()>;
}
