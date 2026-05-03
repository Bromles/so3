use crate::client::interface::BlobPeerClient;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;
use crate::domain::consensus::transport::{
    RecoverNack, RecoverRequest, RecoverResponse, RecoverSuccess,
};
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
    pub(super) async fn recover_internal(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let entry = self.journal.load(&req.command_id).await?;

        // Nack if we have a superseding ballot
        if let Some(ref e) = entry {
            if let Some(ref stored) = e.ballot {
                if *stored > req.ballot {
                    return Ok(RecoverResponse::Nack(RecoverNack {
                        superseding_ballot: stored.clone(),
                    }));
                }
            }
        }

        let timestamp = self.observe(&req.timestamp_zero).await;

        match entry {
            None => Ok(RecoverResponse::Success(RecoverSuccess {
                local_state: JournalState::PreAccepted,
                wait_for: vec![],
                superseding: false,
                dependencies: DependencySet(vec![]),
                timestamp_zero: req.timestamp_zero,
                timestamp,
            })),
            Some(e) => {
                let wait_for = self.unapplied_deps(&e.dependencies).await?;
                Ok(RecoverResponse::Success(RecoverSuccess {
                    local_state: e.state,
                    wait_for,
                    superseding: false,
                    dependencies: e.dependencies,
                    timestamp_zero: e.timestamp_zero,
                    timestamp: e.timestamp.unwrap_or(timestamp),
                }))
            }
        }
    }

    async fn unapplied_deps(&self, deps: &DependencySet) -> So3Result<Vec<CommandId>> {
        let mut unapplied = Vec::new();

        for dep_id in &deps.0 {
            match self.journal.load(dep_id).await? {
                Some(e) if e.state == JournalState::Applied => {}
                _ => unapplied.push(dep_id.clone()),
            }
        }

        Ok(unapplied)
    }
}
