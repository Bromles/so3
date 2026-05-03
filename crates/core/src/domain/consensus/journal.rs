use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::error::So3Error;

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

impl TryFrom<i32> for JournalState {
    type Error = So3Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(JournalState::PreAccepted),
            2 => Ok(JournalState::Accepted),
            3 => Ok(JournalState::Committed),
            4 => Ok(JournalState::Applied),
            val => Err(So3Error::InvalidRequest(format!(
                "invalid journal state: {}",
                val
            ))),
        }
    }
}

impl JournalState {
    pub fn as_i32(&self) -> i32 {
        match self {
            JournalState::PreAccepted => 1,
            JournalState::Accepted => 2,
            JournalState::Committed => 3,
            JournalState::Applied => 4,
        }
    }
}
