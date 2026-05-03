use tokio::net::TcpListener;
use tokio::pin;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;

pub struct Node {
    config: NodeConfig,
}

pub struct BoundNode {
    config: NodeConfig,
    object_listener: TcpListener,
    rpc_listener: TcpListener,
}

impl Node {
    /// # Errors
    ///
    /// Returns an error if durable local repository cannot be opened.
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        config.validate()?;

        unimplemented!()
    }

    /// # Errors
    ///
    /// Returns an error if either local endpoint cannot be bound.
    pub async fn bind(self) -> So3Result<BoundNode> {
        let object_listener = TcpListener::bind(self.config.object_api_addr).await?;
        let rpc_listener = TcpListener::bind(self.config.rpc_api_addr).await?;

        let mut config = self.config;
        config.object_api_addr = object_listener.local_addr()?;
        config.rpc_api_addr = rpc_listener.local_addr()?;

        unimplemented!()
    }

    /// # Errors
    ///
    /// Returns an error if either public or internal server fails to bind or exits with an error.
    pub async fn run(self, cancellation_token: CancellationToken) -> So3Result<()> {
        self.bind().await?.run(cancellation_token).await
    }
}

impl BoundNode {
    #[must_use]
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// # Errors
    ///
    /// Returns an error if either bound server exits with an error.
    pub async fn run(self, cancellation_token: CancellationToken) -> So3Result<()> {
        info!(
            node_id = %self.config.node_id,
            object_api_addr = %self.config.object_api_addr,
            rpc_api_addr = %self.config.rpc_api_addr,
            metadata_dir = %self.config.metadata_dir.display(),
            blob_dir = %self.config.blob_dir.display(),
            peer_count = self.config.cluster.peers.len(),
            "node started"
        );

        let object_token = cancellation_token.child_token();
        let rpc_token = cancellation_token.child_token();

        let config = self.config.clone();
        let object_server = self.object_server;
        let rpc_server = self.rpc_server;
        let object_service = self.object_service.clone();
        let object_listener = self.object_listener;
        let rpc_listener = self.rpc_listener;

        let object_task = spawn(async move {
            object_server
                .run(object_listener, &config, object_service, object_token)
                .await
        });
        let rpc_task = spawn(async move { rpc_server.run(rpc_listener, rpc_token).await });

        fail_fast_join(cancellation_token, object_task, rpc_task).await
    }
}

async fn fail_fast_join(
    cancellation_token: CancellationToken,
    object_task: JoinHandle<So3Result<()>>,
    rpc_task: JoinHandle<So3Result<()>>,
) -> So3Result<()> {
    let object_task = flatten_join(object_task);
    let rpc_task = flatten_join(rpc_task);
    pin!(object_task);
    pin!(rpc_task);

    tokio::select! {
        object_result = &mut object_task => {
            cancellation_token.cancel();
            let rpc_result = (&mut rpc_task).await;
            object_result?;
            rpc_result
        }
        rpc_result = &mut rpc_task => {
            cancellation_token.cancel();
            let object_result = (&mut object_task).await;
            rpc_result?;
            object_result
        }
    }
}

async fn flatten_join<T>(handle: JoinHandle<So3Result<T>>) -> So3Result<T> {
    handle
        .await
        .map_err(|error| So3Error::Io(format!("task join error: {error}")))?
}
