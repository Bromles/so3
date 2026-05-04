use crate::client::interface::BlobPeerClient;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::clock::{HybridLogicalClock, LogicalTimestamp};
use crate::domain::consensus::command_id::CommandId;
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};

/// Committed commands not yet applied, keyed by commit timestamp.
/// Apply gates execution on all earlier-timestamped entries being absent.
type ReorderBuffer = BTreeMap<LogicalTimestamp, CommandId>;

pub struct InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub node_id: NodeId,
    pub epoch: AtomicU64,
    pub hlc: Mutex<HybridLogicalClock>,
    pub journal: Arc<CJR>,
    pub object_metadata_repository: Arc<OMR>,
    pub blob_repository: Arc<BR>,
    pub blob_clients: HashMap<NodeId, Arc<BPC>>,
    pub(super) reorder_buffer: Mutex<ReorderBuffer>,
    pub(super) apply_notify: Arc<Notify>,
}

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub fn new(
        node_id: NodeId,
        epoch: u64,
        journal: Arc<CJR>,
        metadata_repo: Arc<OMR>,
        blob_repo: Arc<BR>,
        blob_clients: HashMap<NodeId, Arc<BPC>>,
        apply_notify: Arc<Notify>,
    ) -> Self {
        Self {
            hlc: Mutex::new(HybridLogicalClock::new(node_id.clone())),
            node_id,
            epoch: AtomicU64::new(epoch),
            journal,
            object_metadata_repository: metadata_repo,
            blob_repository: blob_repo,
            blob_clients,
            reorder_buffer: Mutex::new(BTreeMap::new()),
            apply_notify,
        }
    }

    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Release);
    }

    pub(super) async fn observe(&self, remote: &LogicalTimestamp) -> LogicalTimestamp {
        self.hlc
            .lock()
            .await
            .observe(self.epoch.load(Ordering::Acquire), remote)
    }

    pub(super) async fn fetch_blob_from_any_peer(
        &self,
        blob_id: &BlobId,
    ) -> So3Result<BlobPayload> {
        for client in self.blob_clients.values() {
            if let Ok(payload) = client.fetch(blob_id).await {
                return Ok(payload);
            }
        }
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available on any peer"
        )))
    }
}

#[async_trait]
impl<CJR, OMR, BR, BPC> InboundConsensusUseCase for InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        self.pre_accept_internal(req).await
    }
    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        self.accept_internal(req).await
    }
    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        self.commit_internal(req).await
    }
    async fn apply(&self, req: ApplyRequest) -> So3Result<ApplyResponse> {
        self.apply_internal(req).await
    }
    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        self.recover_internal(req).await
    }
}
