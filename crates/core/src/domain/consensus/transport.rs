use crate::domain::consensus::clock::LogicalTimestamp;
use crate::domain::consensus::command_id::{CommandId, DependencySet};
use crate::domain::node::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ballot {
    pub round: u64,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastApplied {
    pub commands: Vec<CommandId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommandPayload {
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreAcceptRequest {
    pub command_id: CommandId,
    pub timestamp_zero: LogicalTimestamp,
    pub last_applied: LastApplied,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreAcceptResponse {
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub nack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptRequest {
    pub command_id: CommandId,
    pub ballot: Ballot,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub last_applied: LastApplied,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptResponse {
    pub dependencies: DependencySet,
    pub nack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub command_id: CommandId,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResponse {
    pub result: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyHashes {
    pub transaction_hash: Vec<u8>,
    pub execution_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRequest {
    pub command_id: CommandId,
    pub timestamp_zero: LogicalTimestamp,
    pub timestamp: LogicalTimestamp,
    pub dependencies: DependencySet,
    pub hashes: ApplyHashes,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResponse {
    pub result: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryState {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

impl RecoveryState {
    pub fn to_i32(self) -> i32 {
        match self {
            RecoveryState::PreAccepted => 1,
            RecoveryState::Accepted => 2,
            RecoveryState::Committed => 3,
            RecoveryState::Applied => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverRequest {
    pub command_id: CommandId,
    pub ballot: Ballot,
    pub timestamp_zero: LogicalTimestamp,
    pub payload: CommandPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverResponse {
    pub local_state: RecoveryState,
    pub wait_for: Vec<CommandId>,
    pub superseding: bool,
    pub dependencies: DependencySet,
    pub timestamp: LogicalTimestamp,
    pub nack: Ballot,
}
