use crate::domain::clock::LogicalTimestamp;
use crate::domain::command::{CommandResult, ObjectCommand};
pub(crate) use crate::domain::consensus::ballot::Ballot;
use crate::domain::consensus::command_id::{AppliedSet, CommandId, DependencySet};
use crate::domain::consensus::journal::JournalState;

pub struct PreAcceptRequest {
    pub command_id: CommandId,
    pub command: ObjectCommand,
    pub timestamp_zero: LogicalTimestamp,
    pub last_applied: AppliedSet,
}

pub struct PreAcceptResponse {
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub nack: bool,
}

pub struct AcceptRequest {
    pub command_id: CommandId,
    pub ballot: Ballot,
    pub command: ObjectCommand,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub last_applied: AppliedSet,
}

pub struct AcceptResponse {
    pub dependencies: DependencySet,
    pub nack: bool,
}

#[derive(Clone)]
pub struct CommitRequest {
    pub command_id: CommandId,
    pub command: ObjectCommand,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
}

pub struct CommitResponse;

#[derive(Clone)]
pub struct ApplyRequest {
    pub command_id: CommandId,
    pub command: ObjectCommand,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
}

pub struct ApplyResponse {
    pub result: CommandResult,
}

#[derive(Clone)]
pub struct RecoverRequest {
    pub command_id: CommandId,
    pub ballot: Ballot,
    pub command: ObjectCommand,
    pub timestamp_zero: LogicalTimestamp,
}

pub enum RecoverResponse {
    Success(RecoverSuccess),
    Nack(RecoverNack),
}

pub struct RecoverSuccess {
    pub local_state: JournalState,
    pub wait_for: Vec<CommandId>,
    pub superseding: bool,
    pub dependencies: DependencySet,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
}

pub struct RecoverNack {
    pub superseding_ballot: Ballot,
}
