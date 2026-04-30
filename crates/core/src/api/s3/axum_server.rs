use crate::api::s3::controller::{object_controller, ObjectApiState};
use crate::api::s3::S3Api;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use async_trait::async_trait;
use axum::serve as axum_serve;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub struct AxumS3Server;

impl AxumS3Server {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl S3Api for AxumS3Server {
    async fn start(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        axum_serve(
            listener,
            object_controller(ObjectApiState {
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
