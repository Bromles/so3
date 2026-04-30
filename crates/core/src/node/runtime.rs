use tokio::net::TcpListener;
use tokio::pin;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::consensus::executor::{
    LocalConsensusObjectCommandExecutor, PersistentReplicatedCommandExecutor,
};
use crate::consensus::recovery::replay_committed_commands;
use crate::domain::error::{So3Error, So3Result};
use crate::node::config::NodeConfig;
use crate::object_server::server::ObjectServer;
use crate::object_server::service::ObjectService;
use crate::repository::blob::fs::FileSystemBlobRepository;
use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;
use crate::repository::registry::RepositoryRegistry;
use crate::rpc_server::server::RpcServer;
use crate::rpc_server::transport::{ApplyingConsensusTransport, TonicConsensusPeerTransport};

pub struct Node {
    config: NodeConfig,
    object_server: ObjectServer,
    rpc_server: RpcServer<
        ApplyingConsensusTransport<
            PersistentReplicatedCommandExecutor<
                SqliteFsPersistentObjectRepository,
                SqliteObjectMetadataRepository,
            >,
        >,
    >,
    object_service: ObjectService<
        LocalConsensusObjectCommandExecutor<
            ApplyingConsensusTransport<
                PersistentReplicatedCommandExecutor<
                    SqliteFsPersistentObjectRepository,
                    SqliteObjectMetadataRepository,
                >,
            >,
        >,
        FileSystemBlobRepository,
    >,
}

pub struct BoundNode {
    config: NodeConfig,
    object_listener: TcpListener,
    rpc_listener: TcpListener,
    object_server: ObjectServer,
    rpc_server: RpcServer<
        ApplyingConsensusTransport<
            PersistentReplicatedCommandExecutor<
                SqliteFsPersistentObjectRepository,
                SqliteObjectMetadataRepository,
            >,
        >,
    >,
    object_service: ObjectService<
        LocalConsensusObjectCommandExecutor<
            ApplyingConsensusTransport<
                PersistentReplicatedCommandExecutor<
                    SqliteFsPersistentObjectRepository,
                    SqliteObjectMetadataRepository,
                >,
            >,
        >,
        FileSystemBlobRepository,
    >,
}

impl Node {
    /// # Errors
    ///
    /// Returns an error if durable local repository cannot be opened.
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        config.validate()?;

        let node_id = config.node_id;
        let storage = RepositoryRegistry::new(&config.metadata_dir, &config.blob_dir).await?;
        let blob_repository = storage.object_repository.blob_repository().clone();
        let executor = PersistentReplicatedCommandExecutor::new(
            storage.object_repository.clone(),
            storage.metadata_repository.clone(),
        );
        replay_committed_commands(&storage.consensus_journal, &executor).await?;
        let node_id = node_id.to_string();
        let next_sequence = storage
            .consensus_journal
            .next_sequence_for_origin(&node_id)
            .await?;
        let local_transport =
            ApplyingConsensusTransport::new(node_id.clone(), executor, storage.consensus_journal);
        let peer_ids = config
            .cluster
            .peers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let peer_transport = TonicConsensusPeerTransport::from_peer_ids(peer_ids.clone())?;
        let object_service = ObjectService::new(
            LocalConsensusObjectCommandExecutor::with_peers(
                node_id,
                local_transport.clone(),
                next_sequence,
                peer_ids,
                peer_transport,
            ),
            blob_repository,
        );

        Ok(Self {
            config,
            object_server: ObjectServer::new(),
            rpc_server: RpcServer::new(local_transport),
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

    use super::{fail_fast_join, Node};
    use crate::consensus::journal::{JournalState, SqliteConsensusJournal};
    use crate::domain::blob::BlobMetadata;
    use crate::domain::command::{ObjectCommand, WriteCommand};
    use crate::domain::error::So3Error;
    use crate::domain::object::ObjectLastModified;
    use crate::domain::object_key::ObjectKey;
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
    const ALPHA_KEY: &str = "alpha";
    const FIRST_PAYLOAD: &[u8] = b"first";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;

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
    async fn object_api_write_is_recorded_in_consensus_journal() {
        let temp_dir = TempDir::new().unwrap();
        let client = Client::new();
        let bound = Node::new(test_config(temp_dir.path()))
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();
        let base_url = format!("http://{}", bound.config().object_api_addr);
        let token = CancellationToken::new();
        let shutdown = token.clone();
        let task = spawn(async move { bound.run(shutdown).await });

        wait_for_http_ready(&client, &base_url).await;
        let journal = SqliteConsensusJournal::new(temp_dir.path().join(METADATA_DIR_NAME))
            .await
            .unwrap();
        let applied_before = journal
            .list_by_state(JournalState::Applied)
            .await
            .unwrap()
            .len();
        let response = client
            .put(format!("{base_url}/{OBJECT_PATH}"))
            .body(FIRST_PAYLOAD.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        token.cancel();
        task.await.unwrap().unwrap();

        let applied = journal.list_by_state(JournalState::Applied).await.unwrap();

        assert_eq!(applied.len(), applied_before + 1);
        assert!(
            applied
                .iter()
                .all(|entry| entry.command_id.origin_node_id() == NODE_ID_NIL.to_string())
        );
        assert!(applied.iter().all(|entry| !entry.result.is_empty()));
    }

    #[tokio::test]
    async fn object_api_write_is_committed_to_configured_rpc_peer() {
        let temp_dir = TempDir::new().unwrap();
        let client = Client::new();
        let follower_bound = Node::new(test_config_with_node(
            &temp_dir.path().join("follower"),
            "123e4567-e89b-12d3-a456-426614174001",
        ))
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();
        let follower_rpc_addr = follower_bound.config().rpc_api_addr;
        let follower_base_url = format!("http://{}", follower_bound.config().object_api_addr);
        let follower_token = CancellationToken::new();
        let follower_shutdown = follower_token.clone();
        let follower_task = spawn(async move { follower_bound.run(follower_shutdown).await });
        wait_for_http_ready(&client, &follower_base_url).await;

        let mut leader_config = test_config_with_node(
            &temp_dir.path().join("leader"),
            "123e4567-e89b-12d3-a456-426614174002",
        );
        leader_config.cluster.peers = vec![follower_rpc_addr];
        let leader_bound = Node::new(leader_config)
            .await
            .unwrap()
            .bind()
            .await
            .unwrap();
        let leader_base_url = format!("http://{}", leader_bound.config().object_api_addr);
        let leader_token = CancellationToken::new();
        let leader_shutdown = leader_token.clone();
        let leader_task = spawn(async move { leader_bound.run(leader_shutdown).await });
        wait_for_http_ready(&client, &leader_base_url).await;

        let put_response = client
            .put(format!("{leader_base_url}/{OBJECT_PATH}"))
            .body(FIRST_PAYLOAD.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        let follower_get = client
            .get(format!("{follower_base_url}/{OBJECT_PATH}"))
            .send()
            .await
            .unwrap();
        assert_eq!(follower_get.status(), StatusCode::OK);
        assert_eq!(follower_get.bytes().await.unwrap().as_ref(), FIRST_PAYLOAD);

        leader_token.cancel();
        follower_token.cancel();
        leader_task.await.unwrap().unwrap();
        follower_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn new_replays_committed_commands_from_journal() {
        let temp_dir = TempDir::new().unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path().join(METADATA_DIR_NAME))
            .await
            .unwrap();
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(FIRST_PAYLOAD.to_vec()),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        });
        let command_id = crate::consensus::CommandId::new(
            COMMAND_ORIGIN_NODE_ID.to_owned(),
            COMMAND_SEQUENCE_ONE,
        );

        let _ = journal
            .record_committed(&command_id, &command.to_bytes().unwrap())
            .await
            .unwrap();

        let node = Node::new(test_config(temp_dir.path())).await.unwrap();
        drop(node);

        let replayed = journal.load(&command_id).await.unwrap().unwrap();
        assert_eq!(replayed.state, JournalState::Applied);

        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join(METADATA_DIR_NAME),
            temp_dir.path().join(BLOB_DIR_NAME),
        )
            .await
            .unwrap();
        let object = repository
            .read(&ObjectKey::new(ALPHA_KEY).unwrap())
            .await
            .unwrap()
            .unwrap();

        let loaded_value = repository.load_value(&object.blob_id).await.unwrap();
        assert_eq!(loaded_value, FIRST_PAYLOAD.to_vec());
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
        test_config_with_node(data_dir, NODE_ID_NIL.to_string())
    }

    fn test_config_with_node(data_dir: &std::path::Path, node_id: impl AsRef<str>) -> NodeConfig {
        NodeConfig {
            node_id: Uuid::parse_str(node_id.as_ref()).unwrap(),
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
