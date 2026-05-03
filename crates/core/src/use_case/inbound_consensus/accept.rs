use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::command_id::DependencySet;
use crate::domain::consensus::transport::{AcceptRequest, AcceptResponse};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl<CJR, OMR, BR, BPC> InboundConsensusUseCaseImpl<CJR, OMR, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn accept_internal(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        if let Some(entry) = self.journal.load(&req.command_id).await? {
            if let Some(stored_ballot) = entry.ballot {
                if stored_ballot > req.ballot {
                    return Ok(AcceptResponse {
                        dependencies: DependencySet(vec![]),
                        nack: true,
                    });
                }
            }
        }

        self.observe(&req.timestamp).await;

        self.journal
            .record_accepted(&req.command_id, &req.ballot, &req.timestamp)
            .await?;

        Ok(AcceptResponse {
            dependencies: req.dependencies,
            nack: false,
        })
    }
}
