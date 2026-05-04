use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::rpc::RpcApi;
use crate::api::rpc::tonic::tonic_server::TonicRpcServer;
use crate::api::s3::S3Api;
use crate::api::s3::axum::axum_server::AxumS3Server;
use crate::client::blob_client::BlobClient;
use crate::client::consensus_transport_client::ConsensusTransportClient;
use tokio::net::TcpListener;
use tokio::pin;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::domain::command::{CasResult, CommandResult, ObjectCommand, WriteResult};
use crate::domain::consensus::journal::JournalState;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::node::config::NodeConfig;
use crate::repository::blob::fs::FileSystemBlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::consensus_journal::sqlite::SqliteConsensusJournal;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;
use crate::repository::node_identity::fs::FileSystemNodeIdentityRepository;
use crate::repository::registry::RepositoryRegistry;
use crate::service::consensus_coordinator::service::AccordConsensusCoordinatorService;
use crate::use_case::blob::use_case::BlobUseCaseImpl;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use crate::use_case::node_identity::NodeIdentityUseCase;
use crate::use_case::node_identity::use_case::NodeIdentityUseCaseImpl;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

type Journal = SqliteConsensusJournal;
type MetadataRepository = SqliteObjectMetadataRepository;
type BlobRepository = FileSystemBlobRepository;
type ConsensusClient = ConsensusTransportClient;
type BlobPeer = BlobClient;
type Coordinator = AccordConsensusCoordinatorService<
    Journal,
    ConsensusClient,
    MetadataRepository,
    BlobRepository,
    BlobPeer,
>;
type ObjectUseCase =
    ObjectUseCaseImpl<Coordinator, Journal, MetadataRepository, BlobRepository, BlobPeer>;
type InboundConsensusUseCase =
    InboundConsensusUseCaseImpl<Journal, MetadataRepository, BlobRepository, BlobPeer>;
type LocalBlobUseCase = BlobUseCaseImpl<BlobRepository>;

pub struct Node {
    config: NodeConfig,
    object_use_case: Arc<ObjectUseCase>,
    inbound_consensus_use_case: Arc<InboundConsensusUseCase>,
    blob_use_case: Arc<LocalBlobUseCase>,
}

pub struct BoundNode {
    config: NodeConfig,
    object_listener: TcpListener,
    rpc_listener: TcpListener,
    object_use_case: Arc<ObjectUseCase>,
    inbound_consensus_use_case: Arc<InboundConsensusUseCase>,
    blob_use_case: Arc<LocalBlobUseCase>,
}

impl Node {
    /// # Errors
    ///
    /// Returns an error if durable local repository cannot be opened.
    pub async fn new(config: NodeConfig) -> So3Result<Self> {
        config.validate()?;

        let repositories = RepositoryRegistry::new(&config.metadata_dir, &config.blob_dir).await?;
        let metadata_repository = Arc::new(repositories.metadata_repository);
        let blob_repository = Arc::new(repositories.blob_repository);
        let consensus_journal = Arc::new(repositories.consensus_journal);

        let mut consensus_clients = HashMap::new();
        let mut blob_clients = HashMap::new();
        for peer_addr in &config.cluster.peers {
            let peer_id = peer_node_id(*peer_addr);
            let endpoint = rpc_endpoint(*peer_addr);
            consensus_clients.insert(
                peer_id.clone(),
                Arc::new(ConsensusTransportClient::new(endpoint.clone())?),
            );
            blob_clients.insert(peer_id, Arc::new(BlobClient::new(endpoint)?));
        }

        reconcile_applied_metadata(&consensus_journal, &metadata_repository).await?;

        let node_uuid = {
            let repo = Arc::new(FileSystemNodeIdentityRepository::new(&config.metadata_dir).await?);
            NodeIdentityUseCaseImpl::new(repo)
                .ensure(config.node_id)
                .await?
        };
        let node_id = NodeId::new(node_uuid.to_string());
        let apply_notify = Arc::new(tokio::sync::Notify::new());
        let coordinator = AccordConsensusCoordinatorService::new(
            node_id.clone(),
            0,
            0,
            consensus_clients,
            Arc::clone(&consensus_journal),
            Arc::clone(&metadata_repository),
            Arc::clone(&blob_repository),
            blob_clients.clone(),
            Arc::clone(&apply_notify),
        )
        .await?;
        let inbound_consensus_use_case = Arc::new(InboundConsensusUseCaseImpl::new(
            node_id,
            0,
            Arc::clone(&consensus_journal),
            Arc::clone(&metadata_repository),
            Arc::clone(&blob_repository),
            blob_clients.clone(),
            apply_notify,
        ));
        let object_use_case = Arc::new(ObjectUseCaseImpl::new(
            coordinator,
            Arc::clone(&consensus_journal),
            metadata_repository,
            Arc::clone(&blob_repository),
            blob_clients,
        ));
        let blob_use_case = Arc::new(BlobUseCaseImpl::new(blob_repository));

        Ok(Self {
            config,
            object_use_case,
            inbound_consensus_use_case,
            blob_use_case,
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
            object_use_case: self.object_use_case,
            inbound_consensus_use_case: self.inbound_consensus_use_case,
            blob_use_case: self.blob_use_case,
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
        let object_token = cancellation_token.child_token();
        let rpc_token = cancellation_token.child_token();
        let object_config = self.config.clone();
        let object_listener = self.object_listener;
        let rpc_listener = self.rpc_listener;
        let object_use_case = Arc::clone(&self.object_use_case);
        let inbound_consensus_use_case = Arc::clone(&self.inbound_consensus_use_case);
        let blob_use_case = Arc::clone(&self.blob_use_case);

        let object_task = tokio::spawn(async move {
            AxumS3Server::new(object_use_case)
                .start(object_listener, &object_config, object_token)
                .await
        });
        let rpc_task = tokio::spawn(async move {
            TonicRpcServer::new()
                .start(
                    rpc_listener,
                    rpc_token,
                    inbound_consensus_use_case,
                    blob_use_case,
                )
                .await
        });

        fail_fast_join(cancellation_token, object_task, rpc_task).await
    }
}

async fn reconcile_applied_metadata(
    journal: &SqliteConsensusJournal,
    metadata_repository: &SqliteObjectMetadataRepository,
) -> So3Result<()> {
    let mut applied = journal.list_by_state(JournalState::Applied).await?;
    // Process in timestamp order so that for the same key the last command wins.
    applied.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    for entry in &applied {
        let Some(ref result) = entry.result else {
            continue;
        };
        match (&entry.command, result) {
            (ObjectCommand::Write { .. }, CommandResult::Write(WriteResult { metadata }))
            | (ObjectCommand::Cas { .. }, CommandResult::Cas(CasResult::Updated(metadata))) => {
                let current = metadata_repository.load(&metadata.key).await?;
                if current.as_ref().map(|m| &m.version) != Some(&metadata.version) {
                    metadata_repository.store(metadata).await?;
                }
            }
            (ObjectCommand::Delete { key }, CommandResult::Delete) => {
                if metadata_repository.load(key).await?.is_some() {
                    metadata_repository.delete(key).await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn peer_node_id(addr: SocketAddr) -> NodeId {
    NodeId::new(addr.to_string())
}

fn rpc_endpoint(addr: SocketAddr) -> String {
    format!("http://{addr}")
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
    use std::time::Duration;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::node::config::{ClusterConfig, NodeConfig};
    use crate::node::runtime::Node;

    #[tokio::test]
    async fn node_new_and_bind_builds_runtime_components() {
        let temp_dir = TempDir::new().unwrap();
        let config = NodeConfig {
            node_id: Some(Uuid::nil()),
            object_api_addr: "127.0.0.1:0".parse().unwrap(),
            rpc_api_addr: "127.0.0.1:0".parse().unwrap(),
            object_request_timeout: Duration::from_secs(1),
            metadata_dir: temp_dir.path().join("metadata"),
            blob_dir: temp_dir.path().join("blobs"),
            cluster: ClusterConfig::default(),
        };

        let node = Node::new(config).await.unwrap();
        let bound = node.bind().await.unwrap();

        assert_ne!(bound.config().object_api_addr.port(), 0);
        assert_ne!(bound.config().rpc_api_addr.port(), 0);
    }
}
