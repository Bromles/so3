use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::command_id::DependencySet;
use crate::domain::consensus::journal::JournalState;
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
        match self.journal.load(&req.command_id).await? {
            Some(entry) => {
                // Already decided — NACK to force recovery so the coordinator learns
                // the existing decision instead of potentially conflicting with it.
                if matches!(entry.state, JournalState::Committed | JournalState::Applied) {
                    return Ok(AcceptResponse {
                        dependencies: entry.dependencies,
                        nack: true,
                    });
                }
                if let Some(stored_ballot) = entry.ballot
                    && stored_ballot > req.ballot
                {
                    return Ok(AcceptResponse {
                        dependencies: DependencySet(vec![]),
                        nack: true,
                    });
                }
            }
            None => {
                // PreAccept was missed; synthesize the row so record_accepted has a row to UPDATE.
                self.journal
                    .check_conflicts_and_record_pre_accepted(
                        &req.command_id,
                        &req.command,
                        &req.timestamp_zero,
                    )
                    .await?;
            }
        }

        self.observe(&req.timestamp).await;

        self.journal
            .record_accepted(
                &req.command_id,
                &req.ballot,
                &req.timestamp,
                &req.dependencies,
            )
            .await?;

        Ok(AcceptResponse {
            dependencies: req.dependencies,
            nack: false,
        })
    }
}
