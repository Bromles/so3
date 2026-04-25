use axum::serve as axum_serve;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::object_server::controller::{ObjectApiState, object_controller};
use crate::object_server::service::ObjectService;
use crate::storage::object::repository::ObjectRepository;

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
    pub async fn run<R>(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        service: ObjectService<R>,
        cancellation_token: CancellationToken,
    ) -> So3Result<()>
    where
        R: ObjectRepository + Clone + Send + Sync + 'static,
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
