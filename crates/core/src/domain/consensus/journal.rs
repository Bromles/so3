use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};

pub struct JournalEntry {
    pub command_id: CommandId,
    pub command: ObjectCommand,
    pub state: JournalState,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: Option<LogicalTimestamp>,
    pub dependencies: DependencySet,
    pub ballot: Option<Ballot>,
    pub result: Option<CommandResult>,
}

pub enum JournalState {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}