use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CCS, CJR, OMR, BR, BC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn delete_internal(&self, key: &ObjectKey) -> So3Result<()> {
        let command_id = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Delete { key: key.clone() })
            .await?;

        // TODO: metadata delete and record_applied must be atomic — crash between them causes
        // Accord recovery to re-apply DeleteOp; safe only if delete is idempotent.
        self.object_metadata_repository.delete(key).await?;

        self.consensus_journal_repository
            .record_applied(&command_id, &CommandResult::Delete)
            .await?;

        Ok(())
    }
}
