use std::collections::HashSet;

use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{AcceptRequest, AcceptResponse};
use crate::domain::error::So3Result;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;

impl<CJR, CCS, BR, BPC> InboundConsensusUseCaseImpl<CJR, CCS, BR, BPC>
where
    CJR: ConsensusJournalRepository,
    CCS: ConsensusCoordinatorService,
    BR: BlobRepository,
    BPC: BlobPeerClient,
{
    pub(super) async fn accept_internal(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        let mut local_deps: Option<DependencySet> = None;

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
                local_deps = Some(
                    self.journal
                        .check_conflicts_and_record_pre_accepted(
                            &req.command_id,
                            &req.command,
                            &req.timestamp_zero,
                        )
                        .await?,
                );
            }
        }

        self.observe(&req.timestamp).await;

        let merged_deps = match local_deps {
            Some(ld) => Self::merge_deps(&req.dependencies, &ld),
            None => req.dependencies.clone(),
        };

        self.journal
            .record_accepted(&req.command_id, &req.ballot, &req.timestamp, &merged_deps)
            .await?;

        Ok(AcceptResponse {
            dependencies: merged_deps,
            nack: false,
        })
    }

    fn merge_deps(a: &DependencySet, b: &DependencySet) -> DependencySet {
        let mut seen: HashSet<CommandId> = HashSet::new();
        let mut merged = Vec::new();
        for id in a.0.iter().chain(b.0.iter()) {
            if seen.insert(id.clone()) {
                merged.push(id.clone());
            }
        }
        DependencySet(merged)
    }
}
