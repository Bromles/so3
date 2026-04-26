use async_trait::async_trait;
use tonic::Status;

use crate::consensus::ConsensusCommandId;
use crate::consensus::clock::{HybridLogicalClock, timestamp_is_after};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectCommand, ObjectResult};
use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, Ballot, CommitRequest, CommitResponse, DependencySet,
    EventPayload, LastApplied, LogicalTimestamp, PreAcceptRequest, PreAcceptResponse,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

#[async_trait(?Send)]
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
        Self {
            clock: HybridLogicalClock::new(config.node_id.clone()),
            config,
            local_transport,
            peer_transport,
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
            .accept_all(
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

    async fn accept_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        timestamp: &LogicalTimestamp,
        dependencies: &DependencySet,
    ) -> So3Result<DependencySet> {
        let request = AcceptRequest {
            command_id: Some(command_id_proto(command_id)),
            ballot: Some(ballot(&self.config.node_id)),
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
            .map_err(|status| map_status(&status))?;
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
                .await?;
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
) -> So3Result<()> {
    if response.nack {
        return Err(So3Error::InvalidRequest(format!(
            "accept rejected by replica {node_id}"
        )));
    }

    merge_dependencies(dependencies, response.dependencies);

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

fn ballot(node_id: &str) -> Ballot {
    Ballot {
        round: 0,
        node_id: node_id.to_owned(),
    }
}

fn map_status(status: &Status) -> So3Error {
    So3Error::InvalidRequest(status.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;
    use tonic::Status;

    use super::{
        AccordCoordinator, AccordCoordinatorConfig, ConsensusPeerTransport, empty_dependencies,
    };
    use crate::consensus::ConsensusCommandId;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectResult, ReadCommand, ReadResult, WriteCommand,
    };
    use crate::rpc_server::proto::{
        AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommandId, CommitRequest,
        CommitResponse, DependencySet, LogicalTimestamp, PreAcceptRequest, PreAcceptResponse,
        RecoverRequest, RecoverResponse,
    };
    use crate::rpc_server::transport::ConsensusTransportHandler;

    const LOCAL_NODE_ID: &str = "n0";
    const PEER_A: &str = "n1";
    const PEER_B: &str = "n2";
    const KEY_ALPHA: &str = "alpha";

    #[tokio::test]
    async fn execute_drives_all_consensus_phases_through_core() {
        let result = ObjectResult::Read(ReadResult { object: None });
        let local = FakeLocalTransport {
            result: result.to_bytes().unwrap(),
        };
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
        let local = FakeLocalTransport {
            result: ObjectResult::Read(ReadResult { object: None })
                .to_bytes()
                .unwrap(),
        };
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

    struct FakeLocalTransport {
        result: Vec<u8>,
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
            unimplemented!("coordinator does not call recover directly")
        }
    }

    struct FakePeerTransport {
        pre_accepts: VecDeque<PreAcceptResponse>,
        pre_accept_peer_ids: Vec<String>,
        accept_peer_ids: Vec<String>,
        commit_peer_ids: Vec<String>,
        accept_timestamps: Vec<LogicalTimestamp>,
        commit_dependencies: Vec<DependencySet>,
    }

    impl FakePeerTransport {
        fn with_pre_accepts<const N: usize>(responses: [PreAcceptResponse; N]) -> Self {
            Self {
                pre_accepts: VecDeque::from(responses),
                pre_accept_peer_ids: Vec::new(),
                accept_peer_ids: Vec::new(),
                commit_peer_ids: Vec::new(),
                accept_timestamps: Vec::new(),
                commit_dependencies: Vec::new(),
            }
        }
    }

    #[async_trait(?Send)]
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
            self.accept_timestamps.push(request.timestamp.unwrap());
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
