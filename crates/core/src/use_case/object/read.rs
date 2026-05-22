use crate::client::interface::{BlobPeerClient, MetadataQueryClient};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::StoredObject;
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
    pub async fn read_internal(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        let metadata = self
            .quorum_read_metadata(key)
            .await?
            .ok_or_else(|| So3Error::NotFound(format!("object {} not found", key.as_ref())))?;

        if !self.blob_repository.exists(&metadata.blob_id).await? {
            self.fetch_blob_from_any_peer(&metadata.blob_id).await?;
        }

        let blob = self.blob_repository.open_reader(&metadata.blob_id).await?;

        Ok(Some(StoredObject { metadata, blob }))
    }
}
