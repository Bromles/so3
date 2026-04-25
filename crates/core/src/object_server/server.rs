use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::consensus::state_machine::LocalStateMachine;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::object_server::controller::{ObjectApiState, object_controller};

pub struct ObjectServer;

impl Default for ObjectServer {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectServer {
    pub fn new() -> ObjectServer {
        Self
    }

    pub async fn run(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        state_machine: Arc<LocalStateMachine>,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        axum::serve(
            listener,
            object_controller(ObjectApiState {
                state_machine,
                request_timeout: config.object_request_timeout,
            }),
        )
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
        })
        .await
        .map_err(|error| So3Error::Io(error.to_string()))
    }
}
