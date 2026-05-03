use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::{So3Error, So3Result};
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
        let result = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Delete { key: key.clone() })
            .await?;

        match result {
            CommandResult::Delete => Ok(()),
            _ => Err(So3Error::Storage(
                "unexpected result from Delete coordinate".into(),
            )),
        }
    }
}
