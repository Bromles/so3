use async_trait::async_trait;
use tonic::Status;

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
        let mut dependencies = empty_dependencies();

        let pre_accepted = self
            .pre_accept_all(command_id, &command_bytes, &timestamp_zero)
            .await?;
        let timestamp = self
            .clock
            .observe(&pre_accepted.timestamp.unwrap_or(timestamp_zero.clone()))
            .await;
        merge_dependencies(&mut dependencies, Some(pre_accepted.dependencies));

        let accepted = self
            .accept_with_retry(
                command_id,
                &command_bytes,
                &timestamp_zero,
                &timestamp,
                &dependencies,
            )
            .await?;
        merge_dependencies(&mut dependencies, Some(accepted));

        let result = self
            .commit_all(
                command_id,
                &command_bytes,
                &timestamp_zero,
                &timestamp,
                &dependencies,
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

        let local_response = self
            .local_transport
            .pre_accept(request.clone())
            .await
            .map_err(|status| map_status(&status))?;
        let mut decision = PreAcceptDecision {
            timestamp: Some(timestamp_zero.clone()),
            dependencies: empty_dependencies(),
        };
        apply_pre_accept_response(&mut decision, &self.config.node_id, local_response)?;
        for peer_id in &self.config.peer_ids {
            let response = self
                .peer_transport
                .pre_accept_peer(peer_id, request.clone())
                .await?;
            apply_pre_accept_response(&mut decision, peer_id, response)?;
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
    ) -> So3Result<DependencySet> {
        let mut ballot = ballot(INITIAL_BALLOT_ROUND, &self.config.node_id);

        for attempt in 0..=MAX_BALLOT_RECOVERY_RETRIES {
            match self
                .accept_all(
                    command_id,
                    command,
                    timestamp_zero,
                    timestamp,
                    dependencies,
                    &ballot,
                )
                .await
            {
                Ok(accepted) => return Ok(accepted),
                Err(AcceptPhaseError::Rejected { replica_id }) => {
                    let recovery = self.recover(command_id, Some(ballot.clone())).await?;
                    if matches!(recovery.local_state, State::Committed | State::Applied) {
                        return Err(So3Error::InvalidRequest(format!(
                            "accept rejected by replica {replica_id}; recovery observed durable {:?} state and automatic takeover is not implemented",
                            recovery.local_state
                        )));
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

        let local_response = self
            .local_transport
            .accept(request.clone())
            .await
            .map_err(|status| AcceptPhaseError::Other(map_status(&status)))?;
        let mut accepted_dependencies = empty_dependencies();
        apply_accept_response(
            &mut accepted_dependencies,
            &self.config.node_id,
            local_response,
        )?;
        for peer_id in &self.config.peer_ids {
            let response = self
                .peer_transport
                .accept_peer(peer_id, request.clone())
                .await
                .map_err(AcceptPhaseError::Other)?;
            apply_accept_response(&mut accepted_dependencies, peer_id, response)?;
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

        for peer_id in &self.config.peer_ids {
            self.peer_transport
                .commit_peer(peer_id, request.clone())
                .await?;
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
    So3Error::InvalidRequest(status.to_string())
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
    async fn execute_fails_closed_when_recovery_observes_committed_state() {
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
                dependencies: Some(empty_dependencies()),
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
                &ConsensusCommandId::new(LOCAL_NODE_ID.to_owned(), 11),
                ObjectCommand::Read(ReadCommand {
                    key: ObjectKey::new(KEY_ALPHA).unwrap(),
                }),
            )
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("automatic takeover is not implemented")
        );
    }

    struct FakeLocalTransport {
        result: Vec<u8>,
        accepts: Mutex<VecDeque<AcceptResponse>>,
        recovers: Mutex<VecDeque<RecoverResponse>>,
        accept_ballots: Mutex<Vec<Ballot>>,
        recover_ballots: Mutex<Vec<Ballot>>,
    }

    impl FakeLocalTransport {
        fn new(result: Vec<u8>) -> Self {
            Self {
                result,
                accepts: Mutex::new(VecDeque::new()),
                recovers: Mutex::new(VecDeque::new()),
                accept_ballots: Mutex::new(Vec::new()),
                recover_ballots: Mutex::new(Vec::new()),
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
                recover_ballots: Mutex::new(Vec::new()),
            }
        }

        fn accept_ballots(&self) -> Vec<Ballot> {
            self.accept_ballots.lock().unwrap().clone()
        }

        fn recover_ballots(&self) -> Vec<Ballot> {
            self.recover_ballots.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ConsensusTransportHandler for FakeLocalTransport {
        async fn pre_accept(
            &self,
            _request: PreAcceptRequest,
        ) -> Result<PreAcceptResponse, Status> {
            Ok(PreAcceptResponse {
                timestamp: None,
                dependencies: Some(empty_dependencies()),
                nack: false,
            })
        }

        async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status> {
            if let Some(ballot) = request.ballot.clone() {
                self.accept_ballots.lock().unwrap().push(ballot);
            }
            if let Some(response) = self.accepts.lock().unwrap().pop_front() {
                return Ok(response);
            }

            Ok(AcceptResponse {
                dependencies: request.dependencies,
                nack: false,
            })
        }

        async fn commit(&self, _request: CommitRequest) -> Result<CommitResponse, Status> {
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

    struct FakePeerTransport {
        pre_accepts: VecDeque<PreAcceptResponse>,
        accepts: VecDeque<AcceptResponse>,
        recoveries: VecDeque<RecoverResponse>,
        pre_accept_peer_ids: Vec<String>,
        accept_peer_ids: Vec<String>,
        commit_peer_ids: Vec<String>,
        recover_peer_ids: Vec<String>,
        accept_ballots: Vec<Ballot>,
        accept_timestamps: Vec<LogicalTimestamp>,
        commit_dependencies: Vec<DependencySet>,
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
            }
        }

        fn with_pre_accepts<const N: usize>(responses: [PreAcceptResponse; N]) -> Self {
            Self {
                pre_accepts: VecDeque::from(responses),
                accepts: VecDeque::new(),
                recoveries: VecDeque::new(),
                pre_accept_peer_ids: Vec::new(),
                accept_peer_ids: Vec::new(),
                commit_peer_ids: Vec::new(),
                recover_peer_ids: Vec::new(),
                accept_ballots: Vec::new(),
                accept_timestamps: Vec::new(),
                commit_dependencies: Vec::new(),
            }
        }

        fn with_recoveries<const N: usize>(responses: [RecoverResponse; N]) -> Self {
            Self {
                pre_accepts: VecDeque::new(),
                accepts: VecDeque::new(),
                recoveries: VecDeque::from(responses),
                pre_accept_peer_ids: Vec::new(),
                accept_peer_ids: Vec::new(),
                commit_peer_ids: Vec::new(),
                recover_peer_ids: Vec::new(),
                accept_ballots: Vec::new(),
                accept_timestamps: Vec::new(),
                commit_dependencies: Vec::new(),
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
            Ok(self.pre_accepts.pop_front().unwrap())
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
            if let Some(response) = self.accepts.pop_front() {
                return Ok(response);
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
