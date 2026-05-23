use crate::client::interface::BlobPeerClient;
use crate::domain::blob::id::BlobId;
use crate::domain::clock::{HybridLogicalClock, LogicalTimestamp};
use crate::domain::command::ObjectCommand;
use crate::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::inbound_consensus::InboundConsensusUseCase;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

pub struct InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub node_id: NodeId,
    pub epoch: AtomicU64,
    pub hlc: Mutex<HybridLogicalClock>,
    pub journal: Arc<CJR>,
    pub(super) coordinator: Arc<CCS>,
    pub blob_repository: Arc<BR>,
    pub blob_clients: HashMap<NodeId, Arc<BPC>>,
}

impl<CJR, CCS, BR, BPC> InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub fn new(
        node_id: NodeId,
        epoch: u64,
        journal: Arc<CJR>,
        coordinator: Arc<CCS>,
        blob_repo: Arc<BR>,
        blob_clients: HashMap<NodeId, Arc<BPC>>,
        _startup_recovery_done: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            hlc: Mutex::new(HybridLogicalClock::new(node_id.clone())),
            node_id,
            epoch: AtomicU64::new(epoch),
            journal,
            coordinator,
            blob_repository: blob_repo,
            blob_clients,
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

    pub(super) async fn accept_or_observe(&self, remote: &LogicalTimestamp) -> LogicalTimestamp {
        self.hlc
            .lock()
            .await
            .accept_or_observe(self.epoch.load(Ordering::Acquire), remote)
    }

    pub(super) fn command_operation(command: &ObjectCommand) -> &'static str {
        match command {
            ObjectCommand::Read { .. } => "read",
            ObjectCommand::Write { .. } => "write",
            ObjectCommand::Cas { .. } => "cas",
            ObjectCommand::Delete { .. } => "delete",
        }
    }

    pub(super) fn command_object_key(command: &ObjectCommand) -> &ObjectKey {
        match command {
            ObjectCommand::Read { key }
            | ObjectCommand::Write { key, .. }
            | ObjectCommand::Cas { key, .. }
            | ObjectCommand::Delete { key } => key,
        }
    }

    pub(super) async fn fetch_blob_from_any_peer(&self, blob_id: &BlobId) -> So3Result<()> {
        for client in self.blob_clients.values() {
            if let Ok(mut stream) = client.fetch(blob_id).await {
                let temp_blob_id = BlobId::new();
                let mut failed = false;
                while let Some(chunk) = stream.next().await {
                    if let Ok(c) = chunk {
                        if self
                            .blob_repository
                            .append_chunk(&temp_blob_id, c)
                            .await
                            .is_err()
                        {
                            failed = true;
                            break;
                        }
                    } else {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    let _ = self.blob_repository.abort(&temp_blob_id).await;
                    continue;
                }
                return self.blob_repository.commit_as(&temp_blob_id, blob_id).await;
            }
        }
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available on any peer"
        )))
    }
}

#[async_trait]
impl<CJR, CCS, BR, BPC> InboundConsensusUseCase for InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
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
