use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::pin;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::consensus::state_machine::LocalStateMachine;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::object_server::server::ObjectServer;
use crate::object_server::service::ObjectService;
use crate::rpc_server::server::RpcServer;
use crate::rpc_server::transport::RejectingConsensusTransport;
use crate::storage::persistent_object_repository::PersistentObjectRepository;

pub struct Node {
    config: NodeConfig,
    object_server: ObjectServer,
    rpc_server: RpcServer,
    object_service: Arc<ObjectService>,
}

impl Node {
    /// # Errors
    ///
    /// Returns an error if durable local storage cannot be opened.
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        config.validate()?;
        let node_id = config.node_id;
        let repository = Arc::new(
            PersistentObjectRepository::new(&config.metadata_dir, &config.blob_dir).await?,
        );
        let state_machine = Arc::new(LocalStateMachine::new(repository));
        let object_service = Arc::new(ObjectService::new(state_machine));

        Ok(Self {
            config,
            object_server: ObjectServer::new(),
            rpc_server: RpcServer::new(Arc::new(RejectingConsensusTransport::new(node_id))),
            object_service,
        })
    }

    /// # Errors
    ///
    /// Returns an error if either public or internal server fails to bind or exits with an error.
    pub async fn run(self, cancellation_token: CancellationToken) -> So3Result<()> {
        let object_listener = TcpListener::bind(self.config.object_api_addr).await?;
        let rpc_listener = TcpListener::bind(self.config.rpc_api_addr).await?;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::spawn;
    use tokio::sync::Notify;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{Node, fail_fast_join};
    use crate::domain::error::So3Error;
    use crate::node::config::{ClusterConfig, NodeConfig};

    const OBJECT_API_ADDR: &str = "127.0.0.1:3000";
    const RPC_API_ADDR: &str = "127.0.0.1:4000";
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const PEER_SHUTDOWN_TIMEOUT_SECS: u64 = 1;

    #[tokio::test]
    async fn new_initializes_node_with_persistent_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = NodeConfig {
            node_id: Uuid::nil(),
            object_api_addr: OBJECT_API_ADDR.parse().unwrap(),
            rpc_api_addr: RPC_API_ADDR.parse().unwrap(),
            object_request_timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            metadata_dir: temp_dir.path().join("metadata"),
            blob_dir: temp_dir.path().join("blobs"),
            cluster: ClusterConfig::default(),
        };

        let node = Node::new(config).await;

        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn fail_fast_join_cancels_peer_when_first_task_fails() {
        let cancellation_token = CancellationToken::new();
        let peer_stopped = Arc::new(Notify::new());
        let peer_stopped_waiter = peer_stopped.clone();
        let peer_token = cancellation_token.child_token();

        let object_task = spawn(async move { Err(So3Error::RpcNotImplemented) });
        let rpc_task = spawn(async move {
            peer_token.cancelled().await;
            peer_stopped.notify_one();
            Ok(())
        });

        let result = fail_fast_join(cancellation_token.clone(), object_task, rpc_task).await;

        assert!(matches!(result, Err(So3Error::RpcNotImplemented)));
        assert!(cancellation_token.is_cancelled());
        timeout(
            Duration::from_secs(PEER_SHUTDOWN_TIMEOUT_SECS),
            peer_stopped_waiter.notified(),
        )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_fast_join_returns_ok_after_both_tasks_finish_cleanly() {
        let cancellation_token = CancellationToken::new();
        let object_task = spawn(async move { Ok(()) });
        let rpc_task = spawn(async move { Ok(()) });

        let result = fail_fast_join(cancellation_token.clone(), object_task, rpc_task).await;

        assert!(result.is_ok());
        assert!(cancellation_token.is_cancelled());
    }
}
