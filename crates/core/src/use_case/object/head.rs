use crate::client::interface::{BlobPeerClient, MetadataQueryClient};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CCS, CJR, OMR, BR, BC, MQC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC, MQC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
    MQC: MetadataQueryClient,
{
    pub async fn head_internal(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        self.quorum_read_metadata(key).await
    }
}
