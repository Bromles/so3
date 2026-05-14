use crate::api::s3::axum::controller::ObjectApiController;
use crate::api::s3::S3Api;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::use_case::object::ObjectUseCase;
use async_trait::async_trait;
use axum::serve as axum_serve;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub struct AxumS3Server<O: ObjectUseCase> {
    object_use_case: Arc<O>,
}

impl<O: ObjectUseCase> AxumS3Server<O> {
    pub fn new(object_use_case: Arc<O>) -> Self {
        Self { object_use_case }
    }
}

#[async_trait]
impl<O: ObjectUseCase> S3Api for AxumS3Server<O> {
    async fn start(
        self,
        listener: TcpListener,
        config: &NodeConfig,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        let object_controller = Arc::new(ObjectApiController::new(
            config.object_request_timeout,
            self.object_use_case.clone(),
        ));

        axum_serve(listener, object_controller.router())
            .with_graceful_shutdown(async move {
                cancellation_token.cancelled().await;
            })
            .await
            .map_err(|error| So3Error::Io(error.to_string()))
    }
}
