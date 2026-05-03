use crate::client::interface::BlobPeerClient;
use crate::domain::command::{CommandResult, ObjectCommand, ReadResult};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
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
    pub async fn head_internal(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        let result = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Read { key: key.clone() })
            .await?;

        match result {
            CommandResult::Read(ReadResult::Found(m)) => Ok(Some(m)),
            CommandResult::Read(ReadResult::NotFound) => Ok(None),
            _ => Err(So3Error::Storage(
                "unexpected result from Head coordinate".into(),
            )),
        }
    }
}
