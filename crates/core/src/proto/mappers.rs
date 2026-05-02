use crate::domain::clock::LogicalTimestamp as DomainLogicalTimestamp;
use crate::domain::consensus::command_id::{
    CommandId as DomainCommandId, DependencySet as DomainDependencySet,
};
use crate::domain::consensus::transport::{
    AcceptRequest as DomainAcceptRequest, AcceptResponse as DomainAcceptResponse,
    ApplyHashes as DomainApplyHashes, ApplyRequest as DomainApplyRequest,
    ApplyResponse as DomainApplyResponse, Ballot as DomainBallot,
    CommandPayload as DomainCommandPayload, CommitRequest as DomainCommitRequest,
    CommitResponse as DomainCommitResponse, LastApplied as DomainLastApplied,
    PreAcceptRequest as DomainPreAcceptRequest, PreAcceptResponse as DomainPreAcceptResponse,
    RecoverRequest as DomainRecoverRequest, RecoverResponse as DomainRecoverResponse,
    RecoveryState as DomainRecoveryState,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::proto::{
    AcceptRequest as ProtoAcceptRequest, AcceptResponse as ProtoAcceptResponse, ApplyRequest as ProtoApplyRequest,
    ApplyResponse as ProtoApplyResponse, Ballot as ProtoBallot, CommandId as ProtoCommandId,
    CommitRequest as ProtoCommitRequest, CommitResponse as ProtoCommitResponse,
    DependencySet as ProtoDependencySet,
    LogicalTimestamp as ProtoLogicalTimestamp, PreAcceptRequest as ProtoPreAcceptRequest,
    PreAcceptResponse as ProtoPreAcceptResponse, RecoverRequest as ProtoRecoverRequest,
    RecoverResponse as ProtoRecoverResponse,
    State as ProtoState,
};

pub fn map_tonic_status(status: &tonic::Status) -> So3Error {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => So3Error::PeerUnavailable(
            format!("peer returned {}: {}", status.code(), status.message()),
        ),
        _ => So3Error::InvalidRequest(format!(
            "consensus peer returned {}: {}",
            status.code(),
            status.message()
        )),
    }
}

pub fn node_id_to_proto(node_id: NodeId) -> String {
    node_id.to_string()
}

pub fn command_id_to_proto(command_id: DomainCommandId) -> ProtoCommandId {
    ProtoCommandId {
        origin_node_id: node_id_to_proto(command_id.origin_node_id),
        sequence: command_id.sequence,
    }
}

pub fn command_id_to_domain(command_id: ProtoCommandId) -> DomainCommandId {
    DomainCommandId {
        origin_node_id: command_id.origin_node_id.into(),
        sequence: command_id.sequence,
    }
}

pub fn logical_timestamp_to_proto(timestamp: DomainLogicalTimestamp) -> ProtoLogicalTimestamp {
    ProtoLogicalTimestamp {
        epoch: timestamp.physical_time_ms,
        counter: timestamp.counter,
        node_id: node_id_to_proto(timestamp.node_id),
    }
}

pub fn logical_timestamp_to_domain(timestamp: ProtoLogicalTimestamp) -> DomainLogicalTimestamp {
    DomainLogicalTimestamp {
        physical_time_ms: timestamp.epoch,
        counter: timestamp.counter,
        node_id: timestamp.node_id.into(),
    }
}

pub fn last_applied_to_proto(last_applied: DomainLastApplied) -> ProtoLastApplied {
    ProtoLastApplied {
        commands: last_applied
            .commands
            .iter()
            .map(command_id_to_proto)
            .collect(),
    }
}

pub fn last_applied_to_domain(last_applied: ProtoLastApplied) -> DomainLastApplied {
    DomainLastApplied {
        commands: last_applied
            .commands
            .iter()
            .map(command_id_to_domain)
            .collect(),
    }
}

pub fn command_payload_to_proto(command_payload: DomainCommandPayload) -> ProtoEventPayload {
    ProtoEventPayload {
        command: command_payload.command,
    }
}

pub fn command_payload_to_domain(command_payload: ProtoEventPayload) -> DomainCommandPayload {
    DomainCommandPayload {
        command: command_payload.command,
    }
}

pub fn dependency_set_to_proto(dependency_set: DomainDependencySet) -> ProtoDependencySet {
    ProtoDependencySet {
        commands: dependency_set
            .commands
            .iter()
            .map(command_id_to_proto)
            .collect(),
    }
}

pub fn dependency_set_to_domain(dependency_set: ProtoDependencySet) -> DomainDependencySet {
    DomainDependencySet {
        commands: dependency_set
            .commands
            .iter()
            .map(command_id_to_domain)
            .collect(),
    }
}

pub fn ballot_to_proto(ballot: DomainBallot) -> ProtoBallot {
    ProtoBallot {
        round: ballot.round,
        node_id: node_id_to_proto(ballot.node_id),
    }
}

pub fn ballot_to_domain(ballot: ProtoBallot) -> DomainBallot {
    DomainBallot {
        round: ballot.round,
        node_id: ballot.node_id.into(),
    }
}

pub fn apply_hashes_to_proto(hashes: DomainApplyHashes) -> ProtoApplyHashes {
    ProtoApplyHashes {
        transaction_hash: hashes.transaction_hash,
        execution_hash: hashes.execution_hash,
    }
}

pub fn apply_hashes_to_domain(hashes: ProtoApplyHashes) -> DomainApplyHashes {
    DomainApplyHashes {
        transaction_hash: hashes.transaction_hash,
        execution_hash: hashes.execution_hash,
    }
}

pub fn recovery_state_to_domain(state: ProtoState) -> So3Result<DomainRecoveryState> {
    match state {
        ProtoState::PreAccepted => Ok(DomainRecoveryState::PreAccepted),
        ProtoState::Accepted => Ok(DomainRecoveryState::Accepted),
        ProtoState::Committed => Ok(DomainRecoveryState::Committed),
        ProtoState::Applied => Ok(DomainRecoveryState::Applied),
        _ => Err(So3Error::InvalidRequest(format!("invalid state {}", state))),
    }
}

pub fn pre_accept_req_to_proto(req: DomainPreAcceptRequest) -> ProtoPreAcceptRequest {
    ProtoPreAcceptRequest {
        command_id: Some(command_id_to_proto(req.command_id)),
        event: Some(command_payload_to_proto(req.payload)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        last_applied: Some(last_applied_to_proto(req.last_applied)),
    }
}

pub fn pre_accept_req_to_domain(req: ProtoPreAcceptRequest) -> So3Result<DomainPreAcceptRequest> {
    Ok(DomainPreAcceptRequest {
        command_id: command_id_to_domain(req.command_id.ok_or(So3Error::InvalidRequest)?),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero.ok_or(So3Error::InvalidRequest)?,
        ),
        last_applied: last_applied_to_domain(req.last_applied.ok_or(So3Error::InvalidRequest)?),
        payload: command_payload_to_domain(req.event.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn pre_accept_res_to_domain(res: ProtoPreAcceptResponse) -> So3Result<DomainPreAcceptResponse> {
    Ok(DomainPreAcceptResponse {
        timestamp: logical_timestamp_to_domain(res.timestamp.ok_or(So3Error::InvalidRequest)?),
        dependencies: dependency_set_to_domain(res.dependencies.ok_or(So3Error::InvalidRequest)?),
        nack: res.nack,
    })
}

pub fn pre_accept_res_to_proto(res: DomainPreAcceptResponse) -> ProtoPreAcceptResponse {
    ProtoPreAcceptResponse {
        timestamp: Some(logical_timestamp_to_proto(res.timestamp)),
        dependencies: Some(dependency_set_to_proto(res.dependencies)),
        nack: res.nack,
    }
}

pub fn accept_req_to_proto(req: DomainAcceptRequest) -> ProtoAcceptRequest {
    ProtoAcceptRequest {
        command_id: Some(command_id_to_proto(req.command_id)),
        ballot: Some(ballot_to_proto(req.ballot)),
        event: Some(command_payload_to_proto(req.payload)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
        last_applied: Some(last_applied_to_proto(req.last_applied)),
    }
}

pub fn accept_req_to_domain(req: ProtoAcceptRequest) -> So3Result<DomainAcceptRequest> {
    Ok(DomainAcceptRequest {
        command_id: command_id_to_domain(req.command_id.ok_or(So3Error::InvalidRequest)?),
        ballot: ballot_to_domain(req.ballot.ok_or(So3Error::InvalidRequest)?),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero.ok_or(So3Error::InvalidRequest)?,
        ),
        timestamp: logical_timestamp_to_domain(req.timestamp.ok_or(So3Error::InvalidRequest)?),
        dependencies: dependency_set_to_domain(req.dependencies.ok_or(So3Error::InvalidRequest)?),
        last_applied: last_applied_to_domain(req.last_applied.ok_or(So3Error::InvalidRequest)?),
        payload: command_payload_to_domain(req.event.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn accept_res_to_domain(res: ProtoAcceptResponse) -> So3Result<DomainAcceptResponse> {
    Ok(DomainAcceptResponse {
        dependencies: dependency_set_to_domain(res.dependencies.ok_or(So3Error::InvalidRequest)?),
        nack: res.nack,
    })
}

pub fn accept_res_to_proto(res: DomainAcceptResponse) -> ProtoAcceptResponse {
    ProtoAcceptResponse {
        dependencies: Some(dependency_set_to_proto(res.dependencies)),
        nack: res.nack,
    }
}

pub fn commit_req_to_proto(req: DomainCommitRequest) -> ProtoCommitRequest {
    ProtoCommitRequest {
        command_id: Some(command_id_to_proto(req.command_id)),
        event: Some(command_payload_to_proto(req.payload)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
    }
}

pub fn commit_req_to_domain(req: ProtoCommitRequest) -> So3Result<DomainCommitRequest> {
    Ok(DomainCommitRequest {
        command_id: command_id_to_domain(req.command_id.ok_or(So3Error::InvalidRequest)?),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero.ok_or(So3Error::InvalidRequest)?,
        ),
        timestamp: logical_timestamp_to_domain(req.timestamp.ok_or(So3Error::InvalidRequest)?),
        dependencies: dependency_set_to_domain(req.dependencies.ok_or(So3Error::InvalidRequest)?),
        payload: command_payload_to_domain(req.event.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn commit_res_to_domain(res: ProtoCommitResponse) -> So3Result<DomainCommitResponse> {
    Ok(DomainCommitResponse { result: res.result })
}

pub fn commit_res_to_proto(res: DomainCommitResponse) -> ProtoCommitResponse {
    ProtoCommitResponse { result: res.result }
}

pub fn apply_req_to_proto(req: DomainApplyRequest) -> ProtoApplyRequest {
    ProtoApplyRequest {
        command_id: Some(command_id_to_proto(req.command_id)),
        event: Some(command_payload_to_proto(req.payload)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
        hashes: Some(apply_hashes_to_proto(req.hashes)),
    }
}

pub fn apply_req_to_domain(req: ProtoApplyRequest) -> So3Result<DomainApplyRequest> {
    Ok(DomainApplyRequest {
        command_id: command_id_to_domain(req.command_id.ok_or(So3Error::InvalidRequest)?),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero.ok_or(So3Error::InvalidRequest)?,
        ),
        timestamp: logical_timestamp_to_domain(req.timestamp.ok_or(So3Error::InvalidRequest)?),
        dependencies: dependency_set_to_domain(req.dependencies.ok_or(So3Error::InvalidRequest)?),
        hashes: apply_hashes_to_domain(req.hashes.ok_or(So3Error::InvalidRequest)?),
        payload: command_payload_to_domain(req.event.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn apply_res_to_domain(res: ProtoApplyResponse) -> So3Result<DomainApplyResponse> {
    Ok(DomainApplyResponse { result: res.result })
}

pub fn apply_res_to_proto(res: DomainApplyResponse) -> ProtoApplyResponse {
    ProtoApplyResponse { result: res.result }
}

pub fn recover_req_to_proto(req: DomainRecoverRequest) -> ProtoRecoverRequest {
    ProtoRecoverRequest {
        command_id: Some(command_id_to_proto(req.command_id)),
        ballot: Some(ballot_to_proto(req.ballot)),
        event: Some(command_payload_to_proto(req.payload)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
    }
}

pub fn recover_req_to_domain(req: ProtoRecoverRequest) -> So3Result<DomainRecoverRequest> {
    Ok(DomainRecoverRequest {
        command_id: command_id_to_domain(req.command_id.ok_or(So3Error::InvalidRequest)?),
        ballot: ballot_to_domain(req.ballot.ok_or(So3Error::InvalidRequest)?),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero.ok_or(So3Error::InvalidRequest)?,
        ),
        payload: command_payload_to_domain(req.event.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn recover_res_to_domain(res: ProtoRecoverResponse) -> So3Result<DomainRecoverResponse> {
    Ok(DomainRecoverResponse {
        local_state: ProtoState::try_from(res.local_state)
            .map_err(So3Error::InvalidRequest)?
            .map(recovery_state_to_domain)?,
        wait_for: res.wait_for.iter().map(command_id_to_domain).collect(),
        superseding: res.superseding,
        dependencies: dependency_set_to_domain(res.dependencies.ok_or(So3Error::InvalidRequest)?),
        timestamp: logical_timestamp_to_domain(res.timestamp.ok_or(So3Error::InvalidRequest)?),
        nack: ballot_to_domain(res.nack.ok_or(So3Error::InvalidRequest)?),
    })
}

pub fn recover_res_to_proto(res: DomainRecoverResponse) -> ProtoRecoverResponse {
    ProtoRecoverResponse {
        local_state: res.local_state.to_i32(),
        wait_for: res.wait_for.iter().map(command_id_to_proto).collect(),
        superseding: res.superseding,
        dependencies: Some(dependency_set_to_proto(res.dependencies)),
        timestamp: Some(logical_timestamp_to_proto(res.timestamp)),
        nack: Some(ballot_to_proto(res.nack)),
    }
}
