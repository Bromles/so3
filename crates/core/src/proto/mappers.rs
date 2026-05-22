use crate::domain::clock::LogicalTimestamp as DomainLogicalTimestamp;
use crate::domain::command::{
    CasResult as DomainCasResult, CommandResult as DomainCommandResult,
    ObjectCommand as DomainObjectCommand, ReadResult as DomainReadResult,
    WriteResult as DomainWriteResult,
};
use crate::domain::consensus::command_id::{
    AppliedSet as DomainAppliedSet, CommandId as DomainCommandId,
    DependencySet as DomainDependencySet,
};
use crate::domain::consensus::journal::JournalState as DomainJournalState;
use crate::domain::consensus::transport::{
    AcceptRequest as DomainAcceptRequest, AcceptResponse as DomainAcceptResponse,
    ApplyRequest as DomainApplyRequest, ApplyResponse as DomainApplyResponse,
    Ballot as DomainBallot, CommitRequest as DomainCommitRequest,
    CommitResponse as DomainCommitResponse, PreAcceptRequest as DomainPreAcceptRequest,
    PreAcceptResponse as DomainPreAcceptResponse, RecoverNack as DomainRecoverNack,
    RecoverRequest as DomainRecoverRequest, RecoverResponse as DomainRecoverResponse,
    RecoverSuccess as DomainRecoverSuccess,
};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata as DomainObjectMetadata;
use crate::proto::base::cas_result::Outcome as ProtoCasOutcome;
use crate::proto::base::command_result::Result as ProtoResult;
use crate::proto::base::object_command::Op as ProtoOp;
use crate::proto::base::read_result::Outcome as ProtoReadOutcome;
use crate::proto::base::{
    CasConflict as ProtoCasConflict, CasOp as ProtoCasOp, CasResult as ProtoCasResult,
    CasSuccess as ProtoCasSuccess, CommandResult as ProtoCommandResult, DeleteOp as ProtoDeleteOp,
    DeleteResult as ProtoDeleteResult, LogicalTimestamp as ProtoLogicalTimestamp,
    NotFound as ProtoNotFound, ObjectCommand as ProtoObjectCommand,
    ObjectMetadata as ProtoObjectMetadata, ReadOp as ProtoReadOp, ReadResult as ProtoReadResult,
    WriteOp as ProtoWriteOp, WriteResult as ProtoWriteResult,
};
use crate::proto::consensus::recover_response::Outcome as ProtoRecoverOutcome;
use crate::proto::consensus::{
    AcceptRequest as ProtoAcceptRequest, AcceptResponse as ProtoAcceptResponse,
    AppliedSet as ProtoAppliedSet, ApplyRequest as ProtoApplyRequest,
    ApplyResponse as ProtoApplyResponse, Ballot as ProtoBallot, CommandId as ProtoCommandId,
    CommitRequest as ProtoCommitRequest, CommitResponse as ProtoCommitResponse,
    DependencySet as ProtoDependencySet, PreAcceptRequest as ProtoPreAcceptRequest,
    PreAcceptResponse as ProtoPreAcceptResponse, RecoverNack as ProtoRecoverNack,
    RecoverRequest as ProtoRecoverRequest, RecoverResponse as ProtoRecoverResponse,
    RecoverSuccess as ProtoRecoverSuccess, State as ProtoState,
};

pub fn map_tonic_status(status: tonic::Status) -> So3Error {
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
    node_id.as_ref().to_string()
}

pub fn object_metadata_to_proto(metadata: DomainObjectMetadata) -> ProtoObjectMetadata {
    ProtoObjectMetadata {
        key: metadata.key.as_ref().to_string(),
        version: metadata.version.get(),
        blob_id: metadata.blob_id.to_string(),
        sha256: metadata.sha256.as_bytes().to_vec().into(),
        size: metadata.size,
        last_modified_ms: metadata.last_modified_ms,
        deleted: metadata.deleted,
    }
}

pub fn object_metadata_to_domain(metadata: ProtoObjectMetadata) -> So3Result<DomainObjectMetadata> {
    Ok(DomainObjectMetadata {
        key: ObjectKey::new(metadata.key)?,
        version: metadata.version.try_into()?,
        blob_id: metadata.blob_id.as_str().try_into()?,
        sha256: metadata.sha256.try_into()?,
        size: metadata.size,
        last_modified_ms: metadata.last_modified_ms,
        deleted: metadata.deleted,
    })
}

pub fn command_id_to_proto(command_id: &DomainCommandId) -> ProtoCommandId {
    ProtoCommandId {
        origin_node_id: node_id_to_proto(command_id.origin_node_id.clone()),
        sequence: command_id.sequence,
    }
}

pub fn command_id_to_domain(command_id: &ProtoCommandId) -> DomainCommandId {
    DomainCommandId {
        origin_node_id: NodeId::new(command_id.origin_node_id.clone()),
        sequence: command_id.sequence,
    }
}

pub fn logical_timestamp_to_proto(timestamp: DomainLogicalTimestamp) -> ProtoLogicalTimestamp {
    ProtoLogicalTimestamp {
        epoch: timestamp.epoch,
        physical_millis: timestamp.physical_millis,
        logical: timestamp.logical,
        node_id: node_id_to_proto(timestamp.node_id),
    }
}

pub fn logical_timestamp_to_domain(timestamp: ProtoLogicalTimestamp) -> DomainLogicalTimestamp {
    DomainLogicalTimestamp {
        epoch: timestamp.epoch,
        physical_millis: timestamp.physical_millis,
        logical: timestamp.logical,
        node_id: NodeId::new(timestamp.node_id),
    }
}

pub fn dependency_set_to_proto(dependency_set: DomainDependencySet) -> ProtoDependencySet {
    ProtoDependencySet {
        command_ids: dependency_set.0.iter().map(command_id_to_proto).collect(),
    }
}

pub fn dependency_set_to_domain(dependency_set: ProtoDependencySet) -> DomainDependencySet {
    DomainDependencySet(
        dependency_set
            .command_ids
            .iter()
            .map(command_id_to_domain)
            .collect(),
    )
}

pub fn applied_set_to_proto(applied_set: DomainAppliedSet) -> ProtoAppliedSet {
    ProtoAppliedSet {
        command_ids: applied_set.0.iter().map(command_id_to_proto).collect(),
    }
}

pub fn applied_set_to_domain(applied_set: ProtoAppliedSet) -> DomainAppliedSet {
    DomainAppliedSet(
        applied_set
            .command_ids
            .iter()
            .map(command_id_to_domain)
            .collect(),
    )
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
        node_id: NodeId::new(ballot.node_id),
    }
}

pub fn object_command_to_proto(command: DomainObjectCommand) -> ProtoObjectCommand {
    match command {
        DomainObjectCommand::Read { key } => ProtoObjectCommand {
            op: Some(ProtoOp::Read(ProtoReadOp {
                key: key.as_ref().to_string(),
            })),
        },
        DomainObjectCommand::Write {
            key,
            blob_id,
            sha256,
            size,
        } => ProtoObjectCommand {
            op: Some(ProtoOp::Write(ProtoWriteOp {
                key: key.as_ref().to_string(),
                blob_id: blob_id.to_string(),
                sha256: sha256.as_bytes().to_vec().into(),
                size,
            })),
        },
        DomainObjectCommand::Cas {
            key,
            expected_version,
            blob_id,
            sha256,
            size,
        } => ProtoObjectCommand {
            op: Some(ProtoOp::Cas(ProtoCasOp {
                key: key.as_ref().to_string(),
                expected_version: expected_version.get(),
                blob_id: blob_id.to_string(),
                sha256: sha256.as_bytes().to_vec().into(),
                size,
            })),
        },
        DomainObjectCommand::Delete { key } => ProtoObjectCommand {
            op: Some(ProtoOp::Delete(ProtoDeleteOp {
                key: key.as_ref().to_string(),
            })),
        },
    }
}

pub fn object_command_to_domain(command: ProtoObjectCommand) -> So3Result<DomainObjectCommand> {
    match command
        .op
        .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?
    {
        ProtoOp::Read(ProtoReadOp { key }) => Ok(DomainObjectCommand::Read {
            key: ObjectKey::new(key)?,
        }),
        ProtoOp::Write(ProtoWriteOp {
            key,
            blob_id,
            sha256,
            size,
        }) => Ok(DomainObjectCommand::Write {
            key: ObjectKey::new(key)?,
            blob_id: blob_id.as_str().try_into()?,
            sha256: sha256.try_into()?,
            size,
        }),
        ProtoOp::Cas(ProtoCasOp {
            key,
            expected_version,
            blob_id,
            sha256,
            size,
        }) => Ok(DomainObjectCommand::Cas {
            key: ObjectKey::new(key)?,
            expected_version: expected_version.try_into()?,
            blob_id: blob_id.as_str().try_into()?,
            sha256: sha256.try_into()?,
            size,
        }),
        ProtoOp::Delete(ProtoDeleteOp { key }) => Ok(DomainObjectCommand::Delete {
            key: ObjectKey::new(key)?,
        }),
    }
}

pub fn command_result_to_proto(res: DomainCommandResult) -> ProtoCommandResult {
    match res {
        DomainCommandResult::Read(result) => match result {
            DomainReadResult::Found(metadata) => ProtoCommandResult {
                result: Some(ProtoResult::Read(ProtoReadResult {
                    outcome: Some(ProtoReadOutcome::Metadata(object_metadata_to_proto(
                        metadata,
                    ))),
                })),
            },
            DomainReadResult::NotFound => ProtoCommandResult {
                result: Some(ProtoResult::Read(ProtoReadResult {
                    outcome: Some(ProtoReadOutcome::NotFound(ProtoNotFound {})),
                })),
            },
        },
        DomainCommandResult::Write(DomainWriteResult { metadata }) => ProtoCommandResult {
            result: Some(ProtoResult::Write(ProtoWriteResult {
                metadata: Some(object_metadata_to_proto(metadata)),
            })),
        },
        DomainCommandResult::Cas(result) => match result {
            DomainCasResult::Updated(metadata) => ProtoCommandResult {
                result: Some(ProtoResult::Cas(ProtoCasResult {
                    outcome: Some(ProtoCasOutcome::Success(ProtoCasSuccess {
                        metadata: Some(object_metadata_to_proto(metadata)),
                    })),
                })),
            },
            DomainCasResult::Conflict { current_version } => ProtoCommandResult {
                result: Some(ProtoResult::Cas(ProtoCasResult {
                    outcome: Some(ProtoCasOutcome::Conflict(ProtoCasConflict {
                        current_version: current_version.get(),
                    })),
                })),
            },
        },
        DomainCommandResult::Delete => ProtoCommandResult {
            result: Some(ProtoResult::Delete(ProtoDeleteResult {})),
        },
    }
}

pub fn command_result_to_domain(res: ProtoCommandResult) -> So3Result<DomainCommandResult> {
    match res
        .result
        .ok_or_else(|| So3Error::InvalidRequest("empty command result".to_string()))?
    {
        ProtoResult::Read(ProtoReadResult { outcome }) => {
            match outcome
                .ok_or_else(|| So3Error::InvalidRequest("empty read command outcome".to_string()))?
            {
                ProtoReadOutcome::Metadata(metadata) => Ok(DomainCommandResult::Read(
                    DomainReadResult::Found(object_metadata_to_domain(metadata)?),
                )),
                ProtoReadOutcome::NotFound(_) => {
                    Ok(DomainCommandResult::Read(DomainReadResult::NotFound))
                }
            }
        }
        ProtoResult::Write(ProtoWriteResult { metadata }) => {
            Ok(DomainCommandResult::Write(DomainWriteResult {
                metadata: object_metadata_to_domain(metadata.ok_or_else(|| {
                    So3Error::InvalidRequest("empty write command metadata".to_string())
                })?)?,
            }))
        }
        ProtoResult::Cas(ProtoCasResult { outcome }) => {
            match outcome
                .ok_or_else(|| So3Error::InvalidRequest("empty cas command outcome".to_string()))?
            {
                ProtoCasOutcome::Success(ProtoCasSuccess { metadata }) => {
                    Ok(DomainCommandResult::Cas(DomainCasResult::Updated(
                        object_metadata_to_domain(metadata.ok_or_else(|| {
                            So3Error::InvalidRequest("empty cas command metadata".to_string())
                        })?)?,
                    )))
                }
                ProtoCasOutcome::Conflict(ProtoCasConflict { current_version }) => {
                    Ok(DomainCommandResult::Cas(DomainCasResult::Conflict {
                        current_version: current_version.try_into()?,
                    }))
                }
            }
        }
        ProtoResult::Delete(ProtoDeleteResult {}) => Ok(DomainCommandResult::Delete),
    }
}

pub fn recovery_state_to_domain(state: ProtoState) -> So3Result<DomainJournalState> {
    match state {
        ProtoState::PreAccepted => Ok(DomainJournalState::PreAccepted),
        ProtoState::Accepted => Ok(DomainJournalState::Accepted),
        ProtoState::Committed => Ok(DomainJournalState::Committed),
        ProtoState::Applied => Ok(DomainJournalState::Applied),
        _ => Err(So3Error::InvalidRequest(format!(
            "invalid state {}",
            state.as_str_name()
        ))),
    }
}

pub fn pre_accept_req_to_proto(req: DomainPreAcceptRequest) -> ProtoPreAcceptRequest {
    ProtoPreAcceptRequest {
        command_id: Some(command_id_to_proto(&req.command_id)),
        command: Some(object_command_to_proto(req.command)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        last_applied: Some(applied_set_to_proto(req.last_applied)),
    }
}

pub fn pre_accept_req_to_domain(req: ProtoPreAcceptRequest) -> So3Result<DomainPreAcceptRequest> {
    Ok(DomainPreAcceptRequest {
        command_id: command_id_to_domain(
            req.command_id
                .as_ref()
                .ok_or_else(|| So3Error::InvalidRequest("empty command id".to_string()))?,
        ),
        command: object_command_to_domain(
            req.command
                .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?,
        )?,
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
        ),
        last_applied: applied_set_to_domain(
            req.last_applied
                .ok_or_else(|| So3Error::InvalidRequest("empty last_applied".to_string()))?,
        ),
    })
}

pub fn pre_accept_res_to_domain(res: ProtoPreAcceptResponse) -> So3Result<DomainPreAcceptResponse> {
    Ok(DomainPreAcceptResponse {
        timestamp: logical_timestamp_to_domain(
            res.timestamp
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp".to_string()))?,
        ),
        dependencies: dependency_set_to_domain(
            res.dependencies
                .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
        ),
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
        command_id: Some(command_id_to_proto(&req.command_id)),
        command: Some(object_command_to_proto(req.command)),
        ballot: Some(ballot_to_proto(req.ballot)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
        last_applied: Some(applied_set_to_proto(req.last_applied)),
    }
}

pub fn accept_req_to_domain(req: ProtoAcceptRequest) -> So3Result<DomainAcceptRequest> {
    Ok(DomainAcceptRequest {
        command_id: command_id_to_domain(
            req.command_id
                .as_ref()
                .ok_or_else(|| So3Error::InvalidRequest("empty command id".to_string()))?,
        ),
        command: object_command_to_domain(
            req.command
                .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?,
        )?,
        ballot: ballot_to_domain(
            req.ballot
                .ok_or_else(|| So3Error::InvalidRequest("empty ballot".to_string()))?,
        ),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
        ),
        timestamp: logical_timestamp_to_domain(
            req.timestamp
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp".to_string()))?,
        ),
        dependencies: dependency_set_to_domain(
            req.dependencies
                .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
        ),
        last_applied: applied_set_to_domain(
            req.last_applied
                .ok_or_else(|| So3Error::InvalidRequest("empty last_applied".to_string()))?,
        ),
    })
}

pub fn accept_res_to_domain(res: ProtoAcceptResponse) -> So3Result<DomainAcceptResponse> {
    Ok(DomainAcceptResponse {
        dependencies: dependency_set_to_domain(
            res.dependencies
                .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
        ),
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
        command_id: Some(command_id_to_proto(&req.command_id)),
        command: Some(object_command_to_proto(req.command)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
    }
}

pub fn commit_req_to_domain(req: ProtoCommitRequest) -> So3Result<DomainCommitRequest> {
    Ok(DomainCommitRequest {
        command_id: command_id_to_domain(
            req.command_id
                .as_ref()
                .ok_or_else(|| So3Error::InvalidRequest("empty command id".to_string()))?,
        ),
        command: object_command_to_domain(
            req.command
                .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?,
        )?,
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
        ),
        timestamp: logical_timestamp_to_domain(
            req.timestamp
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp".to_string()))?,
        ),
        dependencies: dependency_set_to_domain(
            req.dependencies
                .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
        ),
    })
}

pub fn commit_res_to_domain(_res: ProtoCommitResponse) -> So3Result<DomainCommitResponse> {
    Ok(DomainCommitResponse {})
}

pub fn commit_res_to_proto(_res: DomainCommitResponse) -> ProtoCommitResponse {
    ProtoCommitResponse {}
}

pub fn apply_req_to_proto(req: DomainApplyRequest) -> ProtoApplyRequest {
    ProtoApplyRequest {
        command_id: Some(command_id_to_proto(&req.command_id)),
        command: Some(object_command_to_proto(req.command)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
        timestamp: Some(logical_timestamp_to_proto(req.timestamp)),
        dependencies: Some(dependency_set_to_proto(req.dependencies)),
    }
}

pub fn apply_req_to_domain(req: ProtoApplyRequest) -> So3Result<DomainApplyRequest> {
    Ok(DomainApplyRequest {
        command_id: command_id_to_domain(
            req.command_id
                .as_ref()
                .ok_or_else(|| So3Error::InvalidRequest("empty command id".to_string()))?,
        ),
        command: object_command_to_domain(
            req.command
                .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?,
        )?,
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
        ),
        timestamp: logical_timestamp_to_domain(
            req.timestamp
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp".to_string()))?,
        ),
        dependencies: dependency_set_to_domain(
            req.dependencies
                .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
        ),
    })
}

pub fn apply_res_to_domain(res: ProtoApplyResponse) -> So3Result<DomainApplyResponse> {
    Ok(DomainApplyResponse {
        result: command_result_to_domain(
            res.result
                .ok_or_else(|| So3Error::InvalidRequest("empty result".to_string()))?,
        )?,
    })
}

pub fn apply_res_to_proto(res: DomainApplyResponse) -> ProtoApplyResponse {
    ProtoApplyResponse {
        result: Some(command_result_to_proto(res.result)),
    }
}

pub fn recover_req_to_proto(req: DomainRecoverRequest) -> ProtoRecoverRequest {
    ProtoRecoverRequest {
        command_id: Some(command_id_to_proto(&req.command_id)),
        command: Some(object_command_to_proto(req.command)),
        ballot: Some(ballot_to_proto(req.ballot)),
        timestamp_zero: Some(logical_timestamp_to_proto(req.timestamp_zero)),
    }
}

pub fn recover_req_to_domain(req: ProtoRecoverRequest) -> So3Result<DomainRecoverRequest> {
    Ok(DomainRecoverRequest {
        command_id: command_id_to_domain(
            req.command_id
                .as_ref()
                .ok_or_else(|| So3Error::InvalidRequest("empty command id".to_string()))?,
        ),
        command: object_command_to_domain(
            req.command
                .ok_or_else(|| So3Error::InvalidRequest("empty command".to_string()))?,
        )?,
        ballot: ballot_to_domain(
            req.ballot
                .ok_or_else(|| So3Error::InvalidRequest("empty ballot".to_string()))?,
        ),
        timestamp_zero: logical_timestamp_to_domain(
            req.timestamp_zero
                .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
        ),
    })
}

pub fn recover_res_to_domain(res: ProtoRecoverResponse) -> So3Result<DomainRecoverResponse> {
    match res
        .outcome
        .ok_or_else(|| So3Error::InvalidRequest("empty recover response outcome".to_string()))?
    {
        ProtoRecoverOutcome::Success(ProtoRecoverSuccess {
            local_state,
            wait_for,
            superseding,
            dependencies,
            timestamp_zero,
            timestamp,
            accepted_ballot,
        }) => Ok(DomainRecoverResponse::Success(DomainRecoverSuccess {
            local_state: local_state.try_into()?,
            wait_for: wait_for.iter().map(command_id_to_domain).collect(),
            superseding,
            dependencies: dependency_set_to_domain(
                dependencies
                    .ok_or_else(|| So3Error::InvalidRequest("empty dependencies".to_string()))?,
            ),
            timestamp_zero: logical_timestamp_to_domain(
                timestamp_zero
                    .ok_or_else(|| So3Error::InvalidRequest("empty timestamp zero".to_string()))?,
            ),
            timestamp: logical_timestamp_to_domain(
                timestamp.ok_or_else(|| So3Error::InvalidRequest("empty timestamp".to_string()))?,
            ),
            accepted_ballot: accepted_ballot.map(ballot_to_domain),
        })),
        ProtoRecoverOutcome::Nack(ProtoRecoverNack { superseding_ballot }) => {
            Ok(DomainRecoverResponse::Nack(DomainRecoverNack {
                superseding_ballot: ballot_to_domain(
                    superseding_ballot
                        .ok_or_else(|| So3Error::InvalidRequest("empty ballot".to_string()))?,
                ),
            }))
        }
    }
}

pub fn recover_res_to_proto(res: DomainRecoverResponse) -> ProtoRecoverResponse {
    match res {
        DomainRecoverResponse::Success(DomainRecoverSuccess {
            local_state,
            wait_for,
            superseding,
            dependencies,
            timestamp_zero,
            timestamp,
            accepted_ballot,
        }) => ProtoRecoverResponse {
            outcome: Some(ProtoRecoverOutcome::Success(ProtoRecoverSuccess {
                local_state: local_state.as_i32(),
                wait_for: wait_for.iter().map(command_id_to_proto).collect(),
                superseding,
                dependencies: Some(dependency_set_to_proto(dependencies)),
                timestamp_zero: Some(logical_timestamp_to_proto(timestamp_zero)),
                timestamp: Some(logical_timestamp_to_proto(timestamp)),
                accepted_ballot: accepted_ballot.map(ballot_to_proto),
            })),
        },
        DomainRecoverResponse::Nack(DomainRecoverNack { superseding_ballot }) => {
            ProtoRecoverResponse {
                outcome: Some(ProtoRecoverOutcome::Nack(ProtoRecoverNack {
                    superseding_ballot: Some(ballot_to_proto(superseding_ballot)),
                })),
            }
        }
    }
}
