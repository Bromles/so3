use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::domain::error::{So3Error, So3Result};

pub struct RpcServer;

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcServer {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(
        self,
        listener: TcpListener,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        let local_addr = listener.local_addr().map_err(So3Error::from)?;
        info!(%local_addr, "rpc server placeholder is bound and waiting for shutdown");
        cancellation_token.cancelled().await;
        drop(listener);
        Ok(())
    }
}
