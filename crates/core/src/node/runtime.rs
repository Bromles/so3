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
use crate::rpc_server::transport::ApplyingConsensusTransport;
use crate::storage::object::persistent::SqliteFsObjectRepository;

pub struct Node {
    config: NodeConfig,
    object_server: ObjectServer,
    rpc_server: RpcServer<ApplyingConsensusTransport<SqliteFsObjectRepository>>,
    object_service: ObjectService<SqliteFsObjectRepository>,
}

pub struct BoundNode {
    config: NodeConfig,
    object_listener: TcpListener,
    rpc_listener: TcpListener,
    object_server: ObjectServer,
    rpc_server: RpcServer<ApplyingConsensusTransport<SqliteFsObjectRepository>>,
    object_service: ObjectService<SqliteFsObjectRepository>,
}

impl Node {
    /// # Errors
    ///
    /// Returns an error if durable local storage cannot be opened.
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        config.validate()?;
        let node_id = config.node_id;
        let repository =
            SqliteFsObjectRepository::new(&config.metadata_dir, &config.blob_dir).await?;
        let state_machine = LocalStateMachine::new(repository);
        let rpc_state_machine = state_machine.clone();
        let object_service = ObjectService::new(state_machine);

        Ok(Self {
            config,
            object_server: ObjectServer::new(),
            rpc_server: RpcServer::new(ApplyingConsensusTransport::new(node_id, rpc_state_machine)),
            object_service,
        })
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

        Ok(BoundNode {
            config,
            object_listener,
            rpc_listener,
            object_server: self.object_server,
            rpc_server: self.rpc_server,
            object_service: self.object_service,
        })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use reqwest::{Client, StatusCode};
    use tempfile::TempDir;
    use tokio::spawn;
    use tokio::sync::Notify;
    use tokio::time::sleep;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{Node, fail_fast_join};
    use crate::domain::error::So3Error;
    use crate::node::config::{ClusterConfig, NodeConfig};

    const NODE_ID_NIL: Uuid = Uuid::nil();
    const METADATA_DIR_NAME: &str = "metadata";
    const BLOB_DIR_NAME: &str = "blobs";
    const MISSING_OBJECT_PATH: &str = "/objects/missing";
    const OBJECT_API_ADDR: &str = "127.0.0.1:3000";
    const RPC_API_ADDR: &str = "127.0.0.1:4000";
    const EPHEMERAL_LOOPBACK_ADDR: &str = "127.0.0.1:0";
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const PEER_SHUTDOWN_TIMEOUT_SECS: u64 = 1;
    const SERVER_START_RETRIES: usize = 40;
    const SERVER_START_RETRY_DELAY_MILLIS: u64 = 25;
    const OBJECT_PATH: &str = "objects/alpha";
    const FIRST_PAYLOAD: &[u8] = b"first";

    #[tokio::test]
    async fn new_initializes_node_with_persistent_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = NodeConfig {
            node_id: NODE_ID_NIL,
            object_api_addr: OBJECT_API_ADDR.parse().unwrap(),
            rpc_api_addr: RPC_API_ADDR.parse().unwrap(),
            object_request_timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            metadata_dir: temp_dir.path().join(METADATA_DIR_NAME),
            blob_dir: temp_dir.path().join(BLOB_DIR_NAME),
            cluster: ClusterConfig::default(),
        };

        let node = Node::new(config).await;

        assert!(node.is_ok());
    }

    #[tokio::test]
    async fn bind_resolves_ephemeral_addresses() {
        let temp_dir = TempDir::new().unwrap();
        let bound_node = Node::new(test_config(temp_dir.path()))
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();

        assert_ne!(
            bound_node.config().object_api_addr.to_string(),
            EPHEMERAL_LOOPBACK_ADDR
        );
        assert_ne!(
            bound_node.config().rpc_api_addr.to_string(),
            EPHEMERAL_LOOPBACK_ADDR
        );
    }

    #[tokio::test]
    async fn node_restart_preserves_committed_object() {
        let temp_dir = TempDir::new().unwrap();
        let client = Client::new();

        let first_bound = Node::new(test_config(temp_dir.path()))
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();
        let first_base_url = format!("http://{}", first_bound.config().object_api_addr);
        let first_token = CancellationToken::new();
        let first_shutdown = first_token.clone();
        let first_task = spawn(async move { first_bound.run(first_shutdown).await });

        wait_for_http_ready(&client, &first_base_url).await;
        let put_response = client
            .put(format!("{first_base_url}/{OBJECT_PATH}"))
            .body(FIRST_PAYLOAD.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        first_token.cancel();
        first_task.await.unwrap().unwrap();

        let second_bound = Node::new(test_config(temp_dir.path()))
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();
        let second_base_url = format!("http://{}", second_bound.config().object_api_addr);
        let second_token = CancellationToken::new();
        let second_shutdown = second_token.clone();
        let second_task = spawn(async move { second_bound.run(second_shutdown).await });

        wait_for_http_ready(&client, &second_base_url).await;
        let get_response = client
            .get(format!("{second_base_url}/{OBJECT_PATH}"))
            .send()
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(get_response.bytes().await.unwrap().as_ref(), FIRST_PAYLOAD);

        second_token.cancel();
        second_task.await.unwrap().unwrap();
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

    fn test_config(data_dir: &std::path::Path) -> NodeConfig {
        NodeConfig {
            node_id: NODE_ID_NIL,
            object_api_addr: EPHEMERAL_LOOPBACK_ADDR.parse().unwrap(),
            rpc_api_addr: EPHEMERAL_LOOPBACK_ADDR.parse().unwrap(),
            object_request_timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            metadata_dir: data_dir.join(METADATA_DIR_NAME),
            blob_dir: data_dir.join(BLOB_DIR_NAME),
            cluster: ClusterConfig::default(),
        }
    }

    async fn wait_for_http_ready(client: &Client, base_url: &str) {
        let health_url = format!("{base_url}{MISSING_OBJECT_PATH}");

        for _ in 0..SERVER_START_RETRIES {
            if let Ok(response) = client.get(&health_url).send().await
                && response.status() == StatusCode::NOT_FOUND
            {
                return;
            }

            sleep(Duration::from_millis(SERVER_START_RETRY_DELAY_MILLIS)).await;
        }

        panic!("node object server did not become ready at {base_url}");
    }
}
