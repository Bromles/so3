use async_trait::async_trait;
use tonic::Status;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::rpc_server::proto::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    DependencySet, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse, State,
};

#[async_trait]
pub trait ConsensusTransportHandler: Send + Sync {
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status>;
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status>;
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status>;
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status>;
    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status>;
}

#[derive(Clone)]
pub struct RejectingConsensusTransport {
    node_id: String,
}

impl RejectingConsensusTransport {
    #[must_use]
    pub fn new(node_id: Uuid) -> Self {
        Self {
            node_id: node_id.to_string(),
        }
    }
}

#[async_trait]
impl ConsensusTransportHandler for RejectingConsensusTransport {
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            event_size = request.event.as_ref().map_or(0, |event| event.command.len()),
            "rejecting pre_accept request before accord engine is wired"
        );

        Ok(PreAcceptResponse {
            timestamp: request.timestamp_zero,
            dependencies: Some(empty_dependencies()),
            nack: true,
        })
    }

    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            dependency_count = request
                .dependencies
                .as_ref()
                .map_or(0, |dependencies| dependencies.commands.len()),
            "rejecting accept request before accord engine is wired"
        );

        Ok(AcceptResponse {
            dependencies: Some(request.dependencies.unwrap_or_else(empty_dependencies)),
            nack: true,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse, Status> {
        warn!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            "rejecting commit request because accord engine is not configured"
        );

        Err(Status::failed_precondition(
            "accord commit handling is not configured",
        ))
    }

    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse, Status> {
        warn!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            "rejecting apply request because accord engine is not configured"
        );

        Err(Status::failed_precondition(
            "accord apply handling is not configured",
        ))
    }

    async fn recover(&self, request: RecoverRequest) -> Result<RecoverResponse, Status> {
        debug!(
            node_id = %self.node_id,
            command_origin = request
                .command_id
                .as_ref()
                .map_or("<missing>", |command_id| command_id.origin_node_id.as_str()),
            ballot_round = request.ballot.as_ref().map_or(0, |ballot| ballot.round),
            "rejecting recover request before accord engine is wired"
        );

        Ok(RecoverResponse {
            local_state: State::Undefined.into(),
            wait_for: Vec::new(),
            superseding: false,
            dependencies: Some(empty_dependencies()),
            timestamp: request.timestamp_zero,
            nack: request.ballot,
        })
    }
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;
    use uuid::Uuid;

    use super::{ConsensusTransportHandler, RejectingConsensusTransport};
    use crate::rpc_server::proto::{
        AcceptRequest, ApplyRequest, Ballot, CommitRequest, CommandId, EventPayload,
        LogicalTimestamp, PreAcceptRequest, RecoverRequest, State,
    };

    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const RECOVERY_NODE_ID: &str = "node-b";
    const COMMAND_SEQUENCE: u64 = 7;
    const TEST_EPOCH: u64 = 1;
    const TEST_COUNTER: u64 = 2;
    const TEST_BALLOT_ROUND: u64 = 3;
    const TEST_COMMAND_BYTES: &[u8] = b"cmd";

    fn command_id() -> CommandId {
        CommandId {
            origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            sequence: COMMAND_SEQUENCE,
        }
    }

    #[tokio::test]
    async fn pre_accept_returns_nack_response() {
        let handler = RejectingConsensusTransport::new(Uuid::nil());

        let response = handler
            .pre_accept(PreAcceptRequest {
                command_id: Some(command_id()),
                event: Some(EventPayload {
                    command: TEST_COMMAND_BYTES.to_vec(),
                }),
                timestamp_zero: Some(LogicalTimestamp {
                    epoch: TEST_EPOCH,
                    counter: TEST_COUNTER,
                    node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                }),
                last_applied: None,
            })
            .await
            .unwrap();

        assert!(response.nack);
        assert_eq!(response.timestamp.unwrap().counter, TEST_COUNTER);
        assert!(response.dependencies.unwrap().commands.is_empty());
    }

    #[tokio::test]
    async fn accept_returns_nack_response() {
        let handler = RejectingConsensusTransport::new(Uuid::nil());

        let response = handler.accept(AcceptRequest::default()).await.unwrap();

        assert!(response.nack);
        assert!(response.dependencies.unwrap().commands.is_empty());
    }

    #[tokio::test]
    async fn commit_returns_failed_precondition() {
        let handler = RejectingConsensusTransport::new(Uuid::nil());

        let error = handler.commit(CommitRequest::default()).await.unwrap_err();

        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn apply_returns_failed_precondition() {
        let handler = RejectingConsensusTransport::new(Uuid::nil());

        let error = handler.apply(ApplyRequest::default()).await.unwrap_err();

        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn recover_returns_nack_ballot_and_undefined_state() {
        let handler = RejectingConsensusTransport::new(Uuid::nil());

        let response = handler
            .recover(RecoverRequest {
                command_id: Some(command_id()),
                ballot: Some(Ballot {
                    round: TEST_BALLOT_ROUND,
                    node_id: RECOVERY_NODE_ID.to_owned(),
                }),
                event: None,
                timestamp_zero: None,
            })
            .await
            .unwrap();

        assert_eq!(response.local_state, State::Undefined as i32);
        assert_eq!(response.nack.unwrap().round, TEST_BALLOT_ROUND);
    }
}
