use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use so3_core::domain::error::{So3Error, So3Result};
use so3_core::repository::blob::fs::FileSystemBlobRepository;
use so3_core::repository::consensus_journal::sqlite::SqliteConsensusJournal;
use so3_core::repository::metadata::sqlite::SqliteObjectMetadataRepository;
use so3_core::service::consensus_coordinator::service::AccordConsensusCoordinatorService;
use so3_core::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use so3_core::use_case::object::use_case::ObjectUseCaseImpl;

use crate::protocol::{Message, ResponseBody};
use crate::runtime::peer::{MaelstromBlobPeerClient, MaelstromConsensusPeerClient};
use crate::service::MaelstromService;

pub(super) type Journal = SqliteConsensusJournal;
pub(super) type MetaRepo = SqliteObjectMetadataRepository;
pub(super) type BlobRepo = FileSystemBlobRepository;

pub(super) type Coordinator = AccordConsensusCoordinatorService<
    Journal,
    MaelstromConsensusPeerClient,
    MetaRepo,
    BlobRepo,
    MaelstromBlobPeerClient,
>;
pub(super) type Handler =
    InboundConsensusUseCaseImpl<Journal, MetaRepo, BlobRepo, MaelstromBlobPeerClient>;
pub(super) type ObjectUC =
    ObjectUseCaseImpl<Coordinator, Journal, MetaRepo, BlobRepo, MaelstromBlobPeerClient>;
pub(super) type Service = MaelstromService<ObjectUC>;

pub(super) struct SharedState {
    pub node_id: String,
    pub output: mpsc::UnboundedSender<Vec<u8>>,
    pub pending_consensus: Mutex<HashMap<u64, oneshot::Sender<So3Result<Vec<u8>>>>>,
    pub pending_blobs: Mutex<HashMap<u64, oneshot::Sender<So3Result<BlobResponse>>>>,
    pub next_msg_id: AtomicU64,
}

impl SharedState {
    pub(super) fn next_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }
}

pub(super) enum BlobResponse {
    Pushed,
    Fetched(Vec<u8>),
}

pub(super) struct SharedRuntime {
    pub service: Service,
    pub local_handler: Arc<Handler>,
    pub local_blobs: Arc<BlobRepo>,
    pub shared: Arc<SharedState>,
    pub pending_forwards: Mutex<HashMap<u64, oneshot::Sender<So3Result<ResponseBody>>>>,
}

impl SharedRuntime {
    pub(super) fn send_message(&self, message: &Message<impl Serialize>) -> So3Result<()> {
        let encoded =
            serde_json::to_vec(message).map_err(|e| So3Error::Serialization(e.to_string()))?;
        self.shared
            .output
            .send(encoded)
            .map_err(|_| So3Error::InvalidRequest("output channel closed".into()))
    }
}

pub(super) struct RuntimeComponents {
    pub service: Service,
    pub local_handler: Arc<Handler>,
    pub local_blobs: Arc<BlobRepo>,
}
