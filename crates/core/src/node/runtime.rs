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
use crate::rpc_server::server::RpcServer;
use crate::storage::sqlite_fs::PersistentObjectStore;

pub struct Node {
    config: NodeConfig,
    object_server: ObjectServer,
    rpc_server: RpcServer,
    state_machine: Arc<LocalStateMachine>,
}

impl Node {
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        let repository = Arc::new(PersistentObjectStore::open(&config.data_dir).await?);
        let state_machine = Arc::new(LocalStateMachine::new(repository));

        Ok(Self {
            config,
            object_server: ObjectServer::new(),
            rpc_server: RpcServer::new(),
            state_machine,
        })
    }

    pub async fn run(self, cancellation_token: CancellationToken) -> So3Result<()> {
        let object_listener = TcpListener::bind(self.config.object_api_addr).await?;
        let rpc_listener = TcpListener::bind(self.config.rpc_api_addr).await?;

        info!(
            node_id = %self.config.node_id,
            object_api_addr = %self.config.object_api_addr,
            rpc_api_addr = %self.config.rpc_api_addr,
            data_dir = %self.config.data_dir.display(),
            peer_count = self.config.cluster.peers.len(),
            "node started"
        );

        let object_token = cancellation_token.child_token();
        let rpc_token = cancellation_token.child_token();

        let config = self.config.clone();
        let object_server = self.object_server;
        let rpc_server = self.rpc_server;
        let state_machine = self.state_machine.clone();

        let object_task = spawn(async move {
            object_server
                .run(object_listener, &config, state_machine, object_token)
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

    #[tokio::test]
    async fn new_initializes_node_with_persistent_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = NodeConfig {
            node_id: Uuid::nil(),
            object_api_addr: "127.0.0.1:0".parse().unwrap(),
            rpc_api_addr: "127.0.0.1:0".parse().unwrap(),
            object_request_timeout: Duration::from_secs(10),
            data_dir: temp_dir.path().to_path_buf(),
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
        timeout(Duration::from_secs(1), peer_stopped_waiter.notified())
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
