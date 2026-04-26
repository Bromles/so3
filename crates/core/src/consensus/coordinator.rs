use async_trait::async_trait;
use tonic::Status;
use tracing::warn;

use crate::consensus::ConsensusCommandId;
use crate::consensus::clock::{HybridLogicalClock, timestamp_is_after};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectCommand, ObjectResult};
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, Ballot, CommitRequest, CommitResponse, DependencySet,
    EventPayload, LastApplied, LogicalTimestamp, PreAcceptRequest, PreAcceptResponse,
    RecoverRequest, RecoverResponse, State,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

#[async_trait]
pub trait ConsensusPeerTransport: Send {
    async fn pre_accept_peer(
        &mut self,
        peer_id: &str,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse>;
    async fn accept_peer(
        &mut self,
        peer_id: &str,
        request: AcceptRequest,
    ) -> So3Result<AcceptResponse>;
    async fn commit_peer(
        &mut self,
        peer_id: &str,
        request: CommitRequest,
    ) -> So3Result<CommitResponse>;
    async fn recover_peer(
        &mut self,
        peer_id: &str,
        request: RecoverRequest,
    ) -> So3Result<RecoverResponse>;
}

#[derive(Clone, Debug)]
pub struct AccordCoordinatorConfig {
    pub node_id: String,
    pub peer_ids: Vec<String>,
}

pub struct AccordCoordinator<'a, L, P> {
    config: AccordCoordinatorConfig,
    local_transport: &'a L,
    peer_transport: &'a mut P,
    clock: HybridLogicalClock,
}

const INITIAL_BALLOT_ROUND: u64 = 0;
const MAX_BALLOT_RECOVERY_RETRIES: usize = 3;

impl<'a, L, P> AccordCoordinator<'a, L, P>
where
    L: ConsensusTransportHandler,
    P: ConsensusPeerTransport,
{
    fn quorum_size(&self) -> usize {
        let total = 1 + self.config.peer_ids.len();
        total / 2 + 1
    }

    fn total_replicas(&self) -> usize {
        1 + self.config.peer_ids.len()
    }
    #[must_use]
    pub fn new(
        config: AccordCoordinatorConfig,
        local_transport: &'a L,
        peer_transport: &'a mut P,
    ) -> Self {
        Self::with_clock(
            HybridLogicalClock::new(config.node_id.clone()),
            config,
            local_transport,
            peer_transport,
        )
    }

    #[must_use]
    pub fn with_clock(
        clock: HybridLogicalClock,
        config: AccordCoordinatorConfig,
        local_transport: &'a L,
        peer_transport: &'a mut P,
    ) -> Self {
        Self {
            config,
            local_transport,
            peer_transport,
            clock,
        }
    }

    /// # Errors
    ///
    /// Returns an error if the command cannot be serialized, accepted by every
    /// configured replica, committed, applied, or decoded from the applied result.
    pub async fn execute(
        &mut self,
        command_id: &ConsensusCommandId,
        command: ObjectCommand,
    ) -> So3Result<ObjectResult> {
        let command_bytes = command.to_bytes()?;
        let timestamp_zero = self.clock.tick().await;

        let pre_accepted = self
            .pre_accept_all(command_id, &command_bytes, &timestamp_zero)
            .await?;

        // Fast path: all replicas agreed on timestamp_zero; Accept can be skipped.
        // Slow path: at least one replica proposed a later timestamp; Accept is required to
        // reach agreement on the final timestamp before committing.
        let (final_timestamp, final_dependencies) = if pre_accepted.fast_path {
            (timestamp_zero.clone(), pre_accepted.dependencies)
        } else {
            let timestamp = self
                .clock
                .observe(&pre_accepted.timestamp.unwrap_or(timestamp_zero.clone()))
                .await;
            let mut dependencies = empty_dependencies();
            merge_dependencies(&mut dependencies, Some(pre_accepted.dependencies));

            let accept_decision = self
                .accept_with_retry(
                    command_id,
                    &command_bytes,
                    &timestamp_zero,
                    &timestamp,
                    &dependencies,
                )
                .await?;
            (accept_decision.timestamp, accept_decision.dependencies)
        };

        let result = self
            .commit_all(
                command_id,
                &command_bytes,
                &timestamp_zero,
                &final_timestamp,
                &final_dependencies,
            )
            .await?;

        ObjectResult::from_bytes(&result)
    }

    /// # Errors
    ///
    /// Returns an error if the recovery request is rejected locally or by any configured replica.
    pub async fn recover(
        &mut self,
        command_id: &ConsensusCommandId,
        ballot: Option<Ballot>,
    ) -> So3Result<RecoveryDecision> {
        let timestamp_zero = self.clock.tick().await;
        let request = RecoverRequest {
            command_id: Some(command_id_proto(command_id)),
            ballot,
            event: None,
            timestamp_zero: Some(timestamp_zero.clone()),
        };

        let local_response = self
            .local_transport
            .recover(request.clone())
            .await
            .map_err(|status| map_status(&status))?;
        let mut decision = RecoveryDecision::from_local_timestamp(timestamp_zero);
        apply_recover_response(&mut decision, &self.config.node_id, local_response)?;

        for peer_id in &self.config.peer_ids {
            let response = self
                .peer_transport
                .recover_peer(peer_id, request.clone())
                .await?;
            apply_recover_response(&mut decision, peer_id, response)?;
        }

        Ok(decision)
    }

    async fn pre_accept_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
    ) -> So3Result<PreAcceptDecision> {
        let request = PreAcceptRequest {
            command_id: Some(command_id_proto(command_id)),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            last_applied: Some(LastApplied {
                commands: Vec::new(),
            }),
        };

        let quorum = self.quorum_size();
        let max_errors = self.total_replicas().saturating_sub(quorum);
        let mut decision = PreAcceptDecision {
            timestamp: Some(timestamp_zero.clone()),
            dependencies: empty_dependencies(),
            // Starts true; flipped if any replica proposes a different timestamp or is unreachable.
            // Requires all replicas to have responded with timestamp_zero for unanimity.
            fast_path: true,
        };
        let mut ack_count: usize = 0;
        let mut error_count: usize = 0;

        let local_response = self
            .local_transport
            .pre_accept(request.clone())
            .await
            .map_err(|status| map_status(&status))?;
        if local_response.nack {
            return Err(So3Error::InvalidRequest(format!(
                "pre_accept rejected by local replica {}",
                self.config.node_id
            )));
        }
        if timestamp_advanced(&local_response.timestamp, timestamp_zero) {
            decision.fast_path = false;
        }
        apply_pre_accept_response(&mut decision, &self.config.node_id, local_response)?;
        ack_count += 1;

        for peer_id in &self.config.peer_ids.clone() {
            match self
                .peer_transport
                .pre_accept_peer(peer_id, request.clone())
                .await
            {
                Ok(response) => {
                    if response.nack {
                        return Err(So3Error::InvalidRequest(format!(
                            "pre_accept rejected by replica {peer_id}"
                        )));
                    }
                    if timestamp_advanced(&response.timestamp, timestamp_zero) {
                        decision.fast_path = false;
                    }
                    apply_pre_accept_response(&mut decision, peer_id, response)?;
                    ack_count += 1;
                }
                Err(_) => {
                    error_count += 1;
                    if error_count > max_errors {
                        return Err(So3Error::InvalidRequest(
                            "pre_accept failed to reach quorum: too many peer failures".to_owned(),
                        ));
                    }
                    // An unreachable peer cannot confirm unanimity for the fast path.
                    decision.fast_path = false;
                }
            }
        }

        if ack_count < quorum {
            return Err(So3Error::InvalidRequest(
                "pre_accept failed to reach quorum".to_owned(),
            ));
        }

        Ok(decision)
    }

    async fn accept_with_retry(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        timestamp: &LogicalTimestamp,
        dependencies: &DependencySet,
    ) -> So3Result<AcceptDecision> {
        let mut ballot = ballot(INITIAL_BALLOT_ROUND, &self.config.node_id);
        let mut accepted_timestamp = timestamp.clone();
        let mut accepted_dependencies = dependencies.clone();

        for attempt in 0..=MAX_BALLOT_RECOVERY_RETRIES {
            match self
                .accept_all(
                    command_id,
                    command,
                    timestamp_zero,
                    &accepted_timestamp,
                    &accepted_dependencies,
                    &ballot,
                )
                .await
            {
                Ok(accepted) => {
                    merge_dependencies(&mut accepted_dependencies, Some(accepted));
                    return Ok(AcceptDecision {
                        timestamp: accepted_timestamp,
                        dependencies: accepted_dependencies,
                    });
                }
                Err(AcceptPhaseError::Rejected { replica_id }) => {
                    let recovery = self.recover(command_id, Some(ballot.clone())).await?;
                    accepted_timestamp =
                        max_timestamp(Some(accepted_timestamp), recovery.timestamp.clone())
                            .expect("accept phase always tracks a timestamp");
                    merge_dependencies(&mut accepted_dependencies, Some(recovery.dependencies));

                    if !recovery.wait_for.is_empty()
                        && matches!(recovery.local_state, State::Committed | State::Applied)
                    {
                        return Err(So3Error::InvalidRequest(format!(
                            "accept rejected by replica {replica_id}; recovery observed durable {:?} state waiting for dependencies {:?}",
                            recovery.local_state, recovery.wait_for
                        )));
                    }
                    if matches!(recovery.local_state, State::Committed | State::Applied) {
                        return Ok(AcceptDecision {
                            timestamp: accepted_timestamp,
                            dependencies: accepted_dependencies,
                        });
                    }
                    let Some(highest_nack) = recovery.highest_nack else {
                        return Err(So3Error::InvalidRequest(format!(
                            "accept rejected by replica {replica_id} without durable ballot information"
                        )));
                    };
                    if attempt == MAX_BALLOT_RECOVERY_RETRIES {
                        return Err(So3Error::InvalidRequest(format!(
                            "accept retry budget exhausted after rejection from replica {replica_id}"
                        )));
                    }
                    ballot = next_ballot_after(&highest_nack, &self.config.node_id);
                }
                Err(AcceptPhaseError::Other(error)) => return Err(error),
            }
        }

        Err(So3Error::InvalidRequest(
            "accept retry loop terminated unexpectedly".to_owned(),
        ))
    }

    async fn accept_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        timestamp: &LogicalTimestamp,
        dependencies: &DependencySet,
        ballot: &Ballot,
    ) -> Result<DependencySet, AcceptPhaseError> {
        let request = AcceptRequest {
            command_id: Some(command_id_proto(command_id)),
            ballot: Some(ballot.clone()),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            timestamp: Some(timestamp.clone()),
            dependencies: Some(dependencies.clone()),
            last_applied: Some(LastApplied {
                commands: Vec::new(),
            }),
        };

        let quorum = self.quorum_size();
        let max_errors = self.total_replicas().saturating_sub(quorum);
        let mut accepted_dependencies = empty_dependencies();
        let mut ack_count: usize = 0;
        let mut error_count: usize = 0;

        let local_response = self
            .local_transport
            .accept(request.clone())
            .await
            .map_err(|status| AcceptPhaseError::Other(map_status(&status)))?;
        apply_accept_response(
            &mut accepted_dependencies,
            &self.config.node_id,
            local_response,
        )?;
        ack_count += 1;

        for peer_id in &self.config.peer_ids {
            match self
                .peer_transport
                .accept_peer(peer_id, request.clone())
                .await
            {
                Ok(response) => {
                    // A nack means a higher ballot has been seen; propagate immediately
                    // rather than treating it as a soft failure — recovery is required.
                    apply_accept_response(&mut accepted_dependencies, peer_id, response)?;
                    ack_count += 1;
                }
                Err(error) => {
                    error_count += 1;
                    if error_count > max_errors {
                        return Err(AcceptPhaseError::Other(error));
                    }
                }
            }
        }

        if ack_count < quorum {
            return Err(AcceptPhaseError::Other(So3Error::InvalidRequest(
                "accept failed to reach quorum".to_owned(),
            )));
        }

        Ok(accepted_dependencies)
    }

    async fn commit_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        timestamp: &LogicalTimestamp,
        dependencies: &DependencySet,
    ) -> So3Result<Vec<u8>> {
        let request = CommitRequest {
            command_id: Some(command_id_proto(command_id)),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            timestamp: Some(timestamp.clone()),
            dependencies: Some(dependencies.clone()),
        };

        // Commit is stable once Accept reached quorum; broadcast to peers best-effort.
        // Peers that miss this message will learn the decision via recovery.
        for peer_id in &self.config.peer_ids {
            if let Err(e) = self
                .peer_transport
                .commit_peer(peer_id, request.clone())
                .await
            {
                warn!(
                    peer_id,
                    error = %e,
                    "commit broadcast to peer failed; peer will learn via recovery"
                );
            }
        }

        let response = self
            .local_transport
            .commit(request)
            .await
            .map_err(|status| map_status(&status))?;

        Ok(response.result)
    }
}

#[derive(Debug)]
struct PreAcceptDecision {
    timestamp: Option<LogicalTimestamp>,
    dependencies: DependencySet,
    /// True when every replica that responded agreed on `timestamp_zero` with no conflicts,
    /// allowing the Accept phase to be skipped (Accord fast path).
    fast_path: bool,
}

struct AcceptDecision {
    timestamp: LogicalTimestamp,
    dependencies: DependencySet,
}

enum AcceptPhaseError {
    Rejected { replica_id: String },
    Other(So3Error),
}

impl From<AcceptPhaseError> for So3Error {
    fn from(value: AcceptPhaseError) -> Self {
        match value {
            AcceptPhaseError::Rejected { replica_id } => {
                So3Error::InvalidRequest(format!("accept rejected by replica {replica_id}"))
            }
            AcceptPhaseError::Other(error) => error,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RecoveryDecision {
    pub local_state: State,
    pub wait_for: Vec<crate::rpc_server::proto::CommandId>,
    pub superseding: bool,
    pub dependencies: DependencySet,
    pub timestamp: Option<LogicalTimestamp>,
    pub highest_nack: Option<Ballot>,
}

impl RecoveryDecision {
    fn from_local_timestamp(timestamp: LogicalTimestamp) -> Self {
        Self {
            local_state: State::Undefined,
            wait_for: Vec::new(),
            superseding: false,
            dependencies: empty_dependencies(),
            timestamp: Some(timestamp),
            highest_nack: None,
        }
    }
}

fn timestamp_advanced(
    proposed: &Option<LogicalTimestamp>,
    timestamp_zero: &LogicalTimestamp,
) -> bool {
    proposed
        .as_ref()
        .map_or(false, |ts| timestamp_is_after(ts, timestamp_zero))
}

fn apply_pre_accept_response(
    decision: &mut PreAcceptDecision,
    node_id: &str,
    response: PreAcceptResponse,
) -> So3Result<()> {
    if response.nack {
        return Err(So3Error::InvalidRequest(format!(
            "pre_accept rejected by replica {node_id}"
        )));
    }

    decision.timestamp = max_timestamp(decision.timestamp.take(), response.timestamp);
    merge_dependencies(&mut decision.dependencies, response.dependencies);

    Ok(())
}

fn apply_accept_response(
    dependencies: &mut DependencySet,
    node_id: &str,
    response: AcceptResponse,
) -> Result<(), AcceptPhaseError> {
    if response.nack {
        return Err(AcceptPhaseError::Rejected {
            replica_id: node_id.to_owned(),
        });
    }

    merge_dependencies(dependencies, response.dependencies);

    Ok(())
}

fn apply_recover_response(
    decision: &mut RecoveryDecision,
    node_id: &str,
    response: RecoverResponse,
) -> So3Result<()> {
    if response.superseding {
        decision.superseding = true;
    }

    decision.local_state = max_state(
        decision.local_state,
        State::try_from(response.local_state).map_err(|_| {
            So3Error::InvalidRequest(format!(
                "recover returned unknown state from replica {node_id}"
            ))
        })?,
    );
    merge_command_ids(&mut decision.wait_for, response.wait_for);
    merge_dependencies(&mut decision.dependencies, response.dependencies);
    decision.timestamp = max_timestamp(decision.timestamp.take(), response.timestamp);
    decision.highest_nack = max_ballot_option(decision.highest_nack.take(), response.nack);

    Ok(())
}

fn command_id_proto(command_id: &ConsensusCommandId) -> crate::rpc_server::proto::CommandId {
    crate::rpc_server::proto::CommandId {
        origin_node_id: command_id.origin_node_id().to_owned(),
        sequence: command_id.sequence(),
    }
}

fn event_payload(command: &[u8]) -> EventPayload {
    EventPayload {
        command: command.to_vec(),
    }
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
    }
}

fn merge_dependencies(target: &mut DependencySet, source: Option<DependencySet>) {
    let Some(source) = source else {
        return;
    };

    for command in source.commands {
        if !target.commands.iter().any(|existing| {
            existing.origin_node_id == command.origin_node_id
                && existing.sequence == command.sequence
        }) {
            target.commands.push(command);
        }
    }
}

fn merge_command_ids(
    target: &mut Vec<crate::rpc_server::proto::CommandId>,
    source: Vec<crate::rpc_server::proto::CommandId>,
) {
    for command in source {
        if !target.iter().any(|existing| {
            existing.origin_node_id == command.origin_node_id
                && existing.sequence == command.sequence
        }) {
            target.push(command);
        }
    }
}

fn max_timestamp(
    current: Option<LogicalTimestamp>,
    candidate: Option<LogicalTimestamp>,
) -> Option<LogicalTimestamp> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(if timestamp_is_after(&candidate, &current) {
            candidate
        } else {
            current
        }),
        (None, candidate) => candidate,
        (current, None) => current,
    }
}

fn ballot(round: u64, node_id: &str) -> Ballot {
    Ballot {
        round,
        node_id: node_id.to_owned(),
    }
}

fn next_ballot_after(current: &Ballot, node_id: &str) -> Ballot {
    Ballot {
        round: current.round.saturating_add(1),
        node_id: node_id.to_owned(),
    }
}

fn max_ballot_option(current: Option<Ballot>, candidate: Option<Ballot>) -> Option<Ballot> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(if ballot_is_after(&candidate, &current) {
            candidate
        } else {
            current
        }),
        (None, candidate) => candidate,
        (current, None) => current,
    }
}

fn ballot_is_after(candidate: &Ballot, current: &Ballot) -> bool {
    candidate.round > current.round
        || (candidate.round == current.round && candidate.node_id > current.node_id)
}

fn max_state(current: State, candidate: State) -> State {
    if state_rank(candidate) > state_rank(current) {
        candidate
    } else {
        current
    }
}

fn state_rank(state: State) -> u8 {
    match state {
        State::Undefined => 0,
        State::PreAccepted => 1,
        State::Accepted => 2,
        State::Committed => 3,
        State::Applied => 4,
    }
}

fn map_status(status: &Status) -> So3Error {
    match status.code() {
        tonic::Code::Internal => So3Error::Storage(status.message().to_owned()),
        _ => So3Error::InvalidRequest(status.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tonic::Status;

    use super::{
        AccordCoordinator, AccordCoordinatorConfig, ConsensusPeerTransport, RecoveryDecision,
        empty_dependencies,
    };
    use crate::consensus::ConsensusCommandId;
    use crate::domain::error::So3Error;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectResult, ReadCommand, ReadResult, WriteCommand,
    };
    use crate::rpc_server::proto::{
        AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, Ballot, CommandId,
        CommitRequest, CommitResponse, DependencySet, LogicalTimestamp, PreAcceptRequest,
        PreAcceptResponse, RecoverRequest, RecoverResponse, State,
    };
    use crate::rpc_server::transport::ConsensusTransportHandler;

    const LOCAL_NODE_ID: &str = "n0";
    const PEER_A: &str = "n1";
    const PEER_B: &str = "n2";
    const KEY_ALPHA: &str = "alpha";

    #[tokio::test]
    async fn execute_drives_all_consensus_phases_through_core() {
        let result = ObjectResult::Read(ReadResult { object: None });
        let local = FakeLocalTransport::new(result.to_bytes().unwrap());
        let mut peers = FakePeerTransport::with_pre_accepts([
            pre_accept_response(
                LogicalTimestamp {
                    epoch: u64::MAX - 1,
                    counter: 9,
                    node_id: PEER_A.to_owned(),
                },
                dependency(PEER_A, 11),
            ),
            pre_accept_response(
                LogicalTimestamp {
                    epoch: u64::MAX - 2,
                    counter: 3,
                    node_id: PEER_B.to_owned(),
                },
                dependency(PEER_B, 12),
            ),
        ]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let command = ObjectCommand::Read(ReadCommand {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
        });
        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 7),
                command,
            )
            .await
            .unwrap();

        assert_eq!(actual, result);
        assert_eq!(peers.pre_accept_peer_ids, vec![PEER_A, PEER_B]);
        assert_eq!(peers.accept_peer_ids, vec![PEER_A, PEER_B]);
        assert_eq!(peers.commit_peer_ids, vec![PEER_A, PEER_B]);
        assert!(peers.accept_timestamps.iter().all(|timestamp| {
            timestamp.epoch == u64::MAX - 1
                && timestamp.counter == 10
                && timestamp.node_id == LOCAL_NODE_ID
        }));
        assert!(peers.commit_dependencies.iter().all(|dependencies| {
            dependencies.commands.len() == 2
                && dependencies
                    .commands
                    .iter()
                    .any(|command| command.origin_node_id == PEER_A && command.sequence == 11)
                && dependencies
                    .commands
                    .iter()
                    .any(|command| command.origin_node_id == PEER_B && command.sequence == 12)
        }));
    }

    #[tokio::test]
    async fn execute_rejects_pre_accept_nack_before_accepting() {
        let local = FakeLocalTransport::new(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
        );
        let mut peers = FakePeerTransport::with_pre_accepts([PreAcceptResponse {
            timestamp: None,
            dependencies: Some(empty_dependencies()),
            nack: true,
        }]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned()],
            },
            &local,
            &mut peers,
        );

        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            value: b"value".to_vec(),
        });
        let error = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 8),
                command,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("pre_accept rejected"));
        assert!(peers.accept_peer_ids.is_empty());
        assert!(peers.commit_peer_ids.is_empty());
    }

    #[tokio::test]
    async fn recover_merges_peer_state_dependencies_waits_and_highest_nack() {
        let local = FakeLocalTransport::new(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
        );
        let mut peers = FakePeerTransport::with_recoveries([
            RecoverResponse {
                local_state: State::Committed.into(),
                wait_for: vec![dependency(PEER_A, 10)],
                superseding: false,
                dependencies: Some(DependencySet {
                    commands: vec![dependency(PEER_A, 11)],
                }),
                timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX - 1,
                    counter: 1,
                    node_id: PEER_A.to_owned(),
                }),
                nack: Some(Ballot {
                    round: 1,
                    node_id: PEER_A.to_owned(),
                }),
            },
            RecoverResponse {
                local_state: State::Applied.into(),
                wait_for: vec![dependency(PEER_B, 12), dependency(PEER_A, 10)],
                superseding: true,
                dependencies: Some(DependencySet {
                    commands: vec![dependency(PEER_B, 13)],
                }),
                timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX,
                    counter: 0,
                    node_id: PEER_B.to_owned(),
                }),
                nack: Some(Ballot {
                    round: 2,
                    node_id: PEER_B.to_owned(),
                }),
            },
        ]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let decision = coordinator
            .recover(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 9),
                Some(Ballot {
                    round: 0,
                    node_id: LOCAL_NODE_ID.to_owned(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            decision,
            RecoveryDecision {
                local_state: State::Applied,
                wait_for: vec![dependency(PEER_A, 10), dependency(PEER_B, 12)],
                superseding: true,
                dependencies: DependencySet {
                    commands: vec![dependency(PEER_A, 11), dependency(PEER_B, 13)],
                },
                timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX,
                    counter: 0,
                    node_id: PEER_B.to_owned(),
                }),
                highest_nack: Some(Ballot {
                    round: 2,
                    node_id: PEER_B.to_owned(),
                }),
            }
        );
        assert_eq!(peers.recover_peer_ids, vec![PEER_A, PEER_B]);
    }

    #[tokio::test]
    async fn execute_retries_accept_with_higher_ballot_after_stale_rejection() {
        let local = FakeLocalTransport::with_accept_and_recover(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
            [
                AcceptResponse {
                    dependencies: Some(empty_dependencies()),
                    nack: true,
                },
                AcceptResponse {
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                },
            ],
            [RecoverResponse {
                local_state: State::Accepted.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(empty_dependencies()),
                timestamp: None,
                nack: Some(Ballot {
                    round: 4,
                    node_id: PEER_A.to_owned(),
                }),
            }],
        );
        let mut peers = FakePeerTransport::new();
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: Vec::new(),
            },
            &local,
            &mut peers,
        );

        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 10),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, ObjectResult::Read(ReadResult { object: None }));
        assert_eq!(
            local.accept_ballots(),
            vec![
                Ballot {
                    round: 0,
                    node_id: LOCAL_NODE_ID.to_owned(),
                },
                Ballot {
                    round: 5,
                    node_id: LOCAL_NODE_ID.to_owned(),
                }
            ]
        );
        assert_eq!(
            local.recover_ballots(),
            vec![Ballot {
                round: 0,
                node_id: LOCAL_NODE_ID.to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn execute_rebroadcasts_commit_when_recovery_observes_committed_state() {
        let local = FakeLocalTransport::with_accept_and_recover(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
            [AcceptResponse {
                dependencies: Some(empty_dependencies()),
                nack: true,
            }],
            [RecoverResponse {
                local_state: State::Committed.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(DependencySet {
                    commands: vec![dependency(PEER_A, 21)],
                }),
                timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX,
                    counter: 7,
                    node_id: PEER_A.to_owned(),
                }),
                nack: Some(Ballot {
                    round: 2,
                    node_id: PEER_A.to_owned(),
                }),
            }],
        );
        let mut peers = FakePeerTransport::new();
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: Vec::new(),
            },
            &local,
            &mut peers,
        );

        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 11),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, ObjectResult::Read(ReadResult { object: None }));
        assert_eq!(
            local.commit_dependencies(),
            vec![DependencySet {
                commands: vec![dependency(PEER_A, 21)],
            }]
        );
        assert_eq!(
            local.commit_timestamps(),
            vec![LogicalTimestamp {
                epoch: u64::MAX,
                counter: 7,
                node_id: PEER_A.to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn execute_fails_when_recovery_observes_committed_state_waiting_for_dependencies() {
        let local = FakeLocalTransport::with_accept_and_recover(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
            [AcceptResponse {
                dependencies: Some(empty_dependencies()),
                nack: true,
            }],
            [RecoverResponse {
                local_state: State::Committed.into(),
                wait_for: vec![dependency(PEER_A, 31)],
                superseding: false,
                dependencies: Some(DependencySet {
                    commands: vec![dependency(PEER_A, 31)],
                }),
                timestamp: None,
                nack: Some(Ballot {
                    round: 2,
                    node_id: PEER_A.to_owned(),
                }),
            }],
        );
        let mut peers = FakePeerTransport::new();
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: Vec::new(),
            },
            &local,
            &mut peers,
        );

        let error = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 12),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("waiting for dependencies"));
    }

    #[tokio::test]
    async fn execute_merges_recovered_metadata_before_retrying_accept() {
        let local = FakeLocalTransport::with_accept_and_recover(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
            [
                AcceptResponse {
                    dependencies: Some(empty_dependencies()),
                    nack: true,
                },
                AcceptResponse {
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                },
            ],
            [RecoverResponse {
                local_state: State::Accepted.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(DependencySet {
                    commands: vec![dependency(PEER_A, 41)],
                }),
                timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX,
                    counter: 9,
                    node_id: PEER_A.to_owned(),
                }),
                nack: Some(Ballot {
                    round: 4,
                    node_id: PEER_A.to_owned(),
                }),
            }],
        );
        let mut peers = FakePeerTransport::new();
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: Vec::new(),
            },
            &local,
            &mut peers,
        );

        let _ = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 13),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            local.accept_dependencies(),
            vec![
                empty_dependencies(),
                DependencySet {
                    commands: vec![dependency(PEER_A, 41)],
                }
            ]
        );
        let accept_timestamps = local.accept_timestamps();
        assert_eq!(accept_timestamps.len(), 2);
        assert_eq!(
            accept_timestamps[1],
            LogicalTimestamp {
                epoch: u64::MAX,
                counter: 9,
                node_id: PEER_A.to_owned(),
            }
        );
    }

    struct FakeLocalTransport {
        result: Vec<u8>,
        accepts: Mutex<VecDeque<AcceptResponse>>,
        recovers: Mutex<VecDeque<RecoverResponse>>,
        accept_ballots: Mutex<Vec<Ballot>>,
        accept_timestamps: Mutex<Vec<LogicalTimestamp>>,
        accept_dependencies: Mutex<Vec<DependencySet>>,
        recover_ballots: Mutex<Vec<Ballot>>,
        commit_timestamps: Mutex<Vec<LogicalTimestamp>>,
        commit_dependencies: Mutex<Vec<DependencySet>>,
        /// When set, pre_accept returns this timestamp to force the slow path (Accept required).
        pre_accept_timestamp: Option<LogicalTimestamp>,
    }

    impl FakeLocalTransport {
        fn new(result: Vec<u8>) -> Self {
            Self {
                result,
                accepts: Mutex::new(VecDeque::new()),
                recovers: Mutex::new(VecDeque::new()),
                accept_ballots: Mutex::new(Vec::new()),
                accept_timestamps: Mutex::new(Vec::new()),
                accept_dependencies: Mutex::new(Vec::new()),
                recover_ballots: Mutex::new(Vec::new()),
                commit_timestamps: Mutex::new(Vec::new()),
                commit_dependencies: Mutex::new(Vec::new()),
                pre_accept_timestamp: None,
            }
        }

        fn with_accept_and_recover<const A: usize, const R: usize>(
            result: Vec<u8>,
            accepts: [AcceptResponse; A],
            recovers: [RecoverResponse; R],
        ) -> Self {
            Self {
                result,
                accepts: Mutex::new(VecDeque::from(accepts)),
                recovers: Mutex::new(VecDeque::from(recovers)),
                accept_ballots: Mutex::new(Vec::new()),
                accept_timestamps: Mutex::new(Vec::new()),
                accept_dependencies: Mutex::new(Vec::new()),
                recover_ballots: Mutex::new(Vec::new()),
                commit_timestamps: Mutex::new(Vec::new()),
                commit_dependencies: Mutex::new(Vec::new()),
                // Force slow path so the queued Accept/Recover responses are exercised.
                pre_accept_timestamp: Some(LogicalTimestamp {
                    epoch: u64::MAX,
                    counter: 0,
                    node_id: LOCAL_NODE_ID.to_owned(),
                }),
            }
        }

        fn accept_ballots(&self) -> Vec<Ballot> {
            self.accept_ballots.lock().unwrap().clone()
        }

        fn accept_timestamps(&self) -> Vec<LogicalTimestamp> {
            self.accept_timestamps.lock().unwrap().clone()
        }

        fn accept_dependencies(&self) -> Vec<DependencySet> {
            self.accept_dependencies.lock().unwrap().clone()
        }

        fn recover_ballots(&self) -> Vec<Ballot> {
            self.recover_ballots.lock().unwrap().clone()
        }

        fn commit_timestamps(&self) -> Vec<LogicalTimestamp> {
            self.commit_timestamps.lock().unwrap().clone()
        }

        fn commit_dependencies(&self) -> Vec<DependencySet> {
            self.commit_dependencies.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ConsensusTransportHandler for FakeLocalTransport {
        async fn pre_accept(
            &self,
            _request: PreAcceptRequest,
        ) -> Result<PreAcceptResponse, Status> {
            Ok(PreAcceptResponse {
                timestamp: self.pre_accept_timestamp.clone(),
                dependencies: Some(empty_dependencies()),
                nack: false,
            })
        }

        async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status> {
            if let Some(ballot) = request.ballot.clone() {
                self.accept_ballots.lock().unwrap().push(ballot);
            }
            if let Some(timestamp) = request.timestamp.clone() {
                self.accept_timestamps.lock().unwrap().push(timestamp);
            }
            self.accept_dependencies.lock().unwrap().push(
                request
                    .dependencies
                    .clone()
                    .unwrap_or_else(empty_dependencies),
            );
            if let Some(response) = self.accepts.lock().unwrap().pop_front() {
                return Ok(response);
            }

            Ok(AcceptResponse {
                dependencies: request.dependencies,
                nack: false,
            })
        }

        async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status> {
            if let Some(timestamp) = request.timestamp.clone() {
                self.commit_timestamps.lock().unwrap().push(timestamp);
            }
            self.commit_dependencies
                .lock()
                .unwrap()
                .push(request.dependencies.unwrap_or_else(empty_dependencies));
            Ok(CommitResponse {
                result: self.result.clone(),
            })
        }

        async fn apply(&self, _request: ApplyRequest) -> Result<ApplyResponse, Status> {
            unimplemented!("coordinator does not call apply directly")
        }

        async fn recover(&self, _request: RecoverRequest) -> Result<RecoverResponse, Status> {
            if let Some(ballot) = _request.ballot.clone() {
                self.recover_ballots.lock().unwrap().push(ballot);
            }
            if let Some(response) = self.recovers.lock().unwrap().pop_front() {
                return Ok(response);
            }

            Ok(RecoverResponse {
                local_state: State::Undefined.into(),
                wait_for: Vec::new(),
                superseding: false,
                dependencies: Some(empty_dependencies()),
                timestamp: None,
                nack: None,
            })
        }
    }

    type PeerResult<T> = crate::domain::error::So3Result<T>;

    struct FakePeerTransport {
        pre_accepts: VecDeque<PeerResult<PreAcceptResponse>>,
        accepts: VecDeque<PeerResult<AcceptResponse>>,
        recoveries: VecDeque<RecoverResponse>,
        pre_accept_peer_ids: Vec<String>,
        accept_peer_ids: Vec<String>,
        commit_peer_ids: Vec<String>,
        recover_peer_ids: Vec<String>,
        accept_ballots: Vec<Ballot>,
        accept_timestamps: Vec<LogicalTimestamp>,
        commit_dependencies: Vec<DependencySet>,
        commit_errors: VecDeque<So3Error>,
    }

    impl FakePeerTransport {
        fn new() -> Self {
            Self {
                pre_accepts: VecDeque::new(),
                accepts: VecDeque::new(),
                recoveries: VecDeque::new(),
                pre_accept_peer_ids: Vec::new(),
                accept_peer_ids: Vec::new(),
                commit_peer_ids: Vec::new(),
                recover_peer_ids: Vec::new(),
                accept_ballots: Vec::new(),
                accept_timestamps: Vec::new(),
                commit_dependencies: Vec::new(),
                commit_errors: VecDeque::new(),
            }
        }

        fn with_pre_accepts<const N: usize>(responses: [PreAcceptResponse; N]) -> Self {
            Self {
                pre_accepts: VecDeque::from(responses.map(Ok)),
                ..Self::new()
            }
        }

        fn with_pre_accept_results<const N: usize>(
            results: [PeerResult<PreAcceptResponse>; N],
        ) -> Self {
            Self {
                pre_accepts: VecDeque::from(results),
                ..Self::new()
            }
        }

        fn with_recoveries<const N: usize>(responses: [RecoverResponse; N]) -> Self {
            Self {
                recoveries: VecDeque::from(responses),
                ..Self::new()
            }
        }

        fn with_accept_results<const N: usize>(results: [PeerResult<AcceptResponse>; N]) -> Self {
            Self {
                accepts: VecDeque::from(results),
                ..Self::new()
            }
        }
    }

    #[async_trait]
    impl ConsensusPeerTransport for FakePeerTransport {
        async fn pre_accept_peer(
            &mut self,
            peer_id: &str,
            _request: PreAcceptRequest,
        ) -> crate::domain::error::So3Result<PreAcceptResponse> {
            self.pre_accept_peer_ids.push(peer_id.to_owned());
            self.pre_accepts.pop_front().unwrap_or(Ok(PreAcceptResponse {
                timestamp: None,
                dependencies: Some(empty_dependencies()),
                nack: false,
            }))
        }

        async fn accept_peer(
            &mut self,
            peer_id: &str,
            request: AcceptRequest,
        ) -> crate::domain::error::So3Result<AcceptResponse> {
            self.accept_peer_ids.push(peer_id.to_owned());
            if let Some(ballot) = request.ballot.clone() {
                self.accept_ballots.push(ballot);
            }
            self.accept_timestamps.push(request.timestamp.unwrap());
            if let Some(result) = self.accepts.pop_front() {
                return result;
            }
            Ok(AcceptResponse {
                dependencies: request.dependencies,
                nack: false,
            })
        }

        async fn commit_peer(
            &mut self,
            peer_id: &str,
            request: CommitRequest,
        ) -> crate::domain::error::So3Result<CommitResponse> {
            self.commit_peer_ids.push(peer_id.to_owned());
            if let Some(e) = self.commit_errors.pop_front() {
                return Err(e);
            }
            self.commit_dependencies.push(request.dependencies.unwrap());
            Ok(CommitResponse { result: Vec::new() })
        }

        async fn recover_peer(
            &mut self,
            peer_id: &str,
            _request: RecoverRequest,
        ) -> crate::domain::error::So3Result<RecoverResponse> {
            self.recover_peer_ids.push(peer_id.to_owned());
            Ok(self.recoveries.pop_front().unwrap())
        }
    }

    #[tokio::test]
    async fn execute_takes_fast_path_when_all_replicas_agree_on_timestamp_zero() {
        let result = ObjectResult::Read(ReadResult { object: None });
        let local = FakeLocalTransport::new(result.to_bytes().unwrap());
        // Both peers respond without bumping the timestamp → unanimous agreement.
        let mut peers = FakePeerTransport::with_pre_accepts([
            PreAcceptResponse {
                timestamp: None,
                dependencies: Some(empty_dependencies()),
                nack: false,
            },
            PreAcceptResponse {
                timestamp: None,
                dependencies: Some(empty_dependencies()),
                nack: false,
            },
        ]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 20),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, result);
        assert_eq!(peers.pre_accept_peer_ids, vec![PEER_A, PEER_B]);
        // Fast path: Accept is skipped entirely.
        assert!(peers.accept_peer_ids.is_empty(), "accept must not be called on fast path");
        // Commit still broadcasts to all peers.
        assert_eq!(peers.commit_peer_ids, vec![PEER_A, PEER_B]);
    }

    #[tokio::test]
    async fn pre_accept_succeeds_when_minority_of_peers_are_unreachable() {
        let result = ObjectResult::Read(ReadResult { object: None });
        let local = FakeLocalTransport::new(result.to_bytes().unwrap());
        // 3-node cluster (local + PEER_A + PEER_B). PEER_B is unreachable.
        // Quorum = 2; local + PEER_A = 2 → quorum met.
        let mut peers = FakePeerTransport::with_pre_accept_results([
            Ok(PreAcceptResponse {
                timestamp: None,
                dependencies: Some(empty_dependencies()),
                nack: false,
            }),
            Err(So3Error::InvalidRequest("peer unreachable".to_owned())),
        ]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 21),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, result);
        // Unreachable peer prevents unanimity → must go through Accept.
        assert!(!peers.accept_peer_ids.is_empty(), "slow path required when peer unreachable");
    }

    #[tokio::test]
    async fn pre_accept_fails_when_majority_of_peers_are_unreachable() {
        let local = FakeLocalTransport::new(
            ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
        );
        // 3-node cluster; both peers unreachable → quorum impossible.
        let mut peers = FakePeerTransport::with_pre_accept_results([
            Err(So3Error::InvalidRequest("peer unreachable".to_owned())),
            Err(So3Error::InvalidRequest("peer unreachable".to_owned())),
        ]);
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let error = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 22),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("quorum"), "expected quorum failure");
        assert!(peers.accept_peer_ids.is_empty());
        assert!(peers.commit_peer_ids.is_empty());
    }

    #[tokio::test]
    async fn accept_succeeds_when_minority_of_peers_are_unreachable() {
        let result = ObjectResult::Read(ReadResult { object: None });
        // Force slow path; accept from PEER_A succeeds, PEER_B unreachable.
        // local + PEER_A = 2 = quorum for 3-node cluster.
        let local = FakeLocalTransport::new(result.to_bytes().unwrap());
        let mut peers = FakePeerTransport {
            pre_accepts: VecDeque::from([
                Ok(PreAcceptResponse {
                    timestamp: Some(LogicalTimestamp {
                        epoch: u64::MAX - 1,
                        counter: 1,
                        node_id: PEER_A.to_owned(),
                    }),
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                }),
                Ok(PreAcceptResponse {
                    timestamp: None,
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                }),
            ]),
            accepts: VecDeque::from([Err(So3Error::InvalidRequest(
                "peer unreachable".to_owned(),
            ))]),
            ..FakePeerTransport::new()
        };
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 23),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, result);
    }

    #[tokio::test]
    async fn commit_succeeds_even_when_a_peer_commit_fails() {
        let result = ObjectResult::Read(ReadResult { object: None });
        let local = FakeLocalTransport::new(result.to_bytes().unwrap());
        let mut peers = FakePeerTransport {
            pre_accepts: VecDeque::from([
                Ok(PreAcceptResponse {
                    timestamp: None,
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                }),
                Ok(PreAcceptResponse {
                    timestamp: None,
                    dependencies: Some(empty_dependencies()),
                    nack: false,
                }),
            ]),
            commit_errors: VecDeque::from([So3Error::InvalidRequest(
                "peer unreachable".to_owned(),
            )]),
            ..FakePeerTransport::new()
        };
        let mut coordinator = AccordCoordinator::new(
            AccordCoordinatorConfig {
                node_id: LOCAL_NODE_ID.to_owned(),
                peer_ids: vec![PEER_A.to_owned(), PEER_B.to_owned()],
            },
            &local,
            &mut peers,
        );

        // Fast path: PEER_A commit fails but local commit still succeeds.
        let actual = coordinator
            .execute(
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 24),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap();

        assert_eq!(actual, result);
        assert_eq!(peers.commit_peer_ids, vec![PEER_A, PEER_B]);
    }

    fn pre_accept_response(
        timestamp: LogicalTimestamp,
        dependency: CommandId,
    ) -> PreAcceptResponse {
        PreAcceptResponse {
            timestamp: Some(timestamp),
            dependencies: Some(DependencySet {
                commands: vec![dependency],
            }),
            nack: false,
        }
    }

    fn dependency(origin_node_id: &str, sequence: u64) -> CommandId {
        CommandId {
            origin_node_id: origin_node_id.to_owned(),
            sequence,
        }
    }
}
