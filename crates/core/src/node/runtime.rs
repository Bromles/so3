use std::sync::Arc;

use tokio::net::TcpListener;
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

        tokio::try_join!(flatten_join(object_task), flatten_join(rpc_task))?;
        Ok(())
    }
}

async fn flatten_join<T>(handle: JoinHandle<So3Result<T>>) -> So3Result<T> {
    handle
        .await
        .map_err(|error| So3Error::Io(format!("task join error: {error}")))?
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::Node;
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
}
