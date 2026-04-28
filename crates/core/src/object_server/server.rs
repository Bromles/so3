use axum::serve as axum_serve;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::consensus::state_machine::ObjectCommandExecutor;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::object_server::controller::{object_controller, ObjectApiState};
use crate::object_server::service::ObjectService;
use crate::repository::blob::interface::BlobRepository;

pub struct ObjectServer;

impl Default for ObjectServer {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectServer {
    #[must_use]
    pub fn new() -> ObjectServer {
        Self
    }

    /// # Errors
    ///
    /// Returns an error if the HTTP server fails while serving requests.
    pub async fn run<E, B>(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        service: ObjectService<E, B>,
        cancellation_token: CancellationToken,
    ) -> So3Result<()>
    where
        E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
        B: BlobRepository + Clone + Send + Sync + 'static,
    {
        axum_serve(
            listener,
            object_controller(ObjectApiState {
                service,
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
