use crate::domain::consensus::clock::LogicalTimestamp;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::consensus::transport::{Ballot, RecoveryState};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JournalMetadata {
    pub timestamp_zero: Option<LogicalTimestamp>,
    pub timestamp: Option<LogicalTimestamp>,
    pub dependencies: DependencySet,
    pub ballot: Option<Ballot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JournalEntry {
    pub command_id: CommandId,
    pub state: RecoveryState,
    pub metadata: JournalMetadata,
}
