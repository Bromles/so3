use std::collections::HashMap;
use std::sync::Arc;

use so3_core::domain::error::So3Result;
use so3_core::domain::node::NodeId;
use so3_core::repository::blob::fs::FileSystemBlobRepository;
use so3_core::repository::consensus_journal::sqlite::SqliteConsensusJournal;
use so3_core::repository::metadata::sqlite::SqliteObjectMetadataRepository;
use so3_core::service::consensus_coordinator::service::AccordConsensusCoordinatorService;
use so3_core::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use so3_core::use_case::object::use_case::ObjectUseCaseImpl;

use crate::config::StorageRoots;
use crate::runtime::peer::{MaelstromBlobPeerClient, MaelstromConsensusPeerClient};
use crate::runtime::types::{RuntimeComponents, SharedState};
use crate::service::MaelstromService;

pub(super) async fn build_components(
    storage_roots: &StorageRoots,
    node_id: &str,
    peer_ids: Vec<String>,
    shared: Arc<SharedState>,
) -> So3Result<RuntimeComponents> {
    let node_dirs = storage_roots.for_node(node_id);

    let journal = Arc::new(SqliteConsensusJournal::new(&node_dirs.metadata_dir).await?);
    let meta = Arc::new(SqliteObjectMetadataRepository::new(&node_dirs.metadata_dir).await?);
    let blobs = Arc::new(FileSystemBlobRepository::new(&node_dirs.blob_dir).await?);

    let peer_clients: HashMap<NodeId, Arc<MaelstromConsensusPeerClient>> = peer_ids
        .iter()
        .map(|id| {
            (
                NodeId::new(id.clone()),
                Arc::new(MaelstromConsensusPeerClient {
                    peer_id: id.clone(),
                    shared: Arc::clone(&shared),
                }),
            )
        })
        .collect();

    let blob_clients: HashMap<NodeId, Arc<MaelstromBlobPeerClient>> = peer_ids
        .iter()
        .map(|id| {
            (
                NodeId::new(id.clone()),
                Arc::new(MaelstromBlobPeerClient {
                    peer_id: id.clone(),
                    shared: Arc::clone(&shared),
                }),
            )
        })
        .collect();

    let apply_notify = Arc::new(tokio::sync::Notify::new());
    let coordinator = AccordConsensusCoordinatorService::new(
        NodeId::new(node_id.to_owned()),
        0,
        0,
        peer_clients,
        Arc::clone(&journal),
        Arc::clone(&meta),
        Arc::clone(&blobs),
        blob_clients.clone(),
        Arc::clone(&apply_notify),
    )
    .await?;

    // Recover any consensus entries left in PreAccepted/Accepted state from a
    // prior crash — must happen before the coordinator starts serving new requests.
    coordinator.recover_stalled_entries().await;

    let local_handler = Arc::new(
        InboundConsensusUseCaseImpl::new(
            NodeId::new(node_id.to_owned()),
            0,
            Arc::clone(&journal),
            Arc::clone(&meta),
            Arc::clone(&blobs),
            blob_clients.clone(),
            apply_notify,
        )
        .await?,
    );

    let object_uc = ObjectUseCaseImpl::new(
        coordinator,
        Arc::clone(&journal),
        Arc::clone(&meta),
        Arc::clone(&blobs),
        blob_clients,
    );

    Ok(RuntimeComponents {
        service: MaelstromService::new(object_uc),
        local_handler,
        local_blobs: blobs,
    })
}
