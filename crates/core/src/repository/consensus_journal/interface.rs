use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::journal::{JournalEntry, JournalState};
use crate::domain::error::So3Result;
use crate::domain::node::NodeId;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusJournalRepository: Send + Sync + 'static {
    async fn load(&self, command_id: &CommandId) -> So3Result<Option<JournalEntry>>;
    async fn check_conflicts_and_record_pre_accepted(
        &self,
        command_id: &CommandId,
        command: &ObjectCommand,
        timestamp_zero: &LogicalTimestamp,
    ) -> So3Result<DependencySet>;
    /// Records a recovery ballot on an existing entry without changing its state.
    /// Prevents any coordinator with a lower ballot from overwriting this entry.
    async fn record_ballot(&self, command_id: &CommandId, ballot: &Ballot) -> So3Result<()>;
    async fn record_accepted(
        &self,
        command_id: &CommandId,
        ballot: &Ballot,
        timestamp: &LogicalTimestamp,
        deps: &DependencySet,
    ) -> So3Result<()>;
    async fn record_committed(
        &self,
        command_id: &CommandId,
        timestamp: &LogicalTimestamp,
        deps: &DependencySet,
    ) -> So3Result<()>;
    async fn record_applied(&self, command_id: &CommandId, result: &CommandResult)
    -> So3Result<()>;
    async fn list_by_state(&self, state: JournalState) -> So3Result<Vec<JournalEntry>>;
    /// Returns the highest sequence number recorded for the given node, or 0 if none.
    async fn max_sequence(&self, node_id: &NodeId) -> So3Result<u64>;
    /// Removes an entry that was recorded locally but never reached quorum.
    /// Used to clean up after a failed coordinate() attempt so that the stalled
    /// PreAccepted entry does not poison future writes to the same key.
    async fn delete(&self, command_id: &CommandId) -> So3Result<()>;
}
