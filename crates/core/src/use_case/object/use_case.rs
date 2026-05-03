use crate::client::interface::BlobPeerClient;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::command::{CasResult, CommandResult};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::{ObjectMetadata, StoredObject};
use crate::domain::object::version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::ObjectUseCase;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ObjectUseCaseImpl<
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
> {
    pub consensus_coordinator_service: CCS,
    pub consensus_journal_repository: Arc<CJR>,
    pub object_metadata_repository: Arc<OMR>,
    pub blob_repository: Arc<BR>,
    pub blob_client_map: HashMap<NodeId, Arc<BC>>,
}

impl<CCS, CJR, OMR, BR, BC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub fn new(
        consensus_coordinator_service: CCS,
        consensus_journal_repository: Arc<CJR>,
        object_metadata_repository: Arc<OMR>,
        blob_repository: Arc<BR>,
        blob_client_map: HashMap<NodeId, Arc<BC>>,
    ) -> Self {
        Self {
            consensus_coordinator_service,
            consensus_journal_repository,
            object_metadata_repository,
            blob_repository,
            blob_client_map,
        }
    }

    pub fn unexpected_result<T>(operation: &str, result: &CommandResult) -> So3Result<T> {
        Err(So3Error::InvalidRequest(format!(
            "unexpected state machine result for {operation}: {result:?}"
        )))
    }
}

#[async_trait]
impl<CCS, CJR, OMR, BR, BC> ObjectUseCase for ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    async fn head(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        self.head_internal(key).await
    }

    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        self.read_internal(key).await
    }

    async fn write(&self, key: ObjectKey, payload: BlobPayload) -> So3Result<ObjectMetadata> {
        self.write_internal(key, payload).await
    }

    async fn delete(&self, key: &ObjectKey) -> So3Result<()> {
        self.delete_internal(key).await
    }

    async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        payload: BlobPayload,
    ) -> So3Result<CasResult> {
        self.cas_internal(key, expected_version, payload).await
    }
}
