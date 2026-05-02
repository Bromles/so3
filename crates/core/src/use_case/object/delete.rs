use crate::client::interface::BlobPeerClient;
use crate::domain::command::ObjectCommand;
use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CJ, BR, BC> ObjectUseCaseImpl<CJ, BR, BC>
where
    CJ: ConsensusJournalRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn delete_internal(&self, key: &ObjectKey) -> So3Result<()> {
        self.state_machine
            .execute(ObjectCommand::Delete { key })
            .await?
    }
}
