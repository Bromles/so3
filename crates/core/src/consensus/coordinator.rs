use async_trait::async_trait;
use tonic::Status;

use crate::consensus::ConsensusCommandId;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectCommand, ObjectResult};
use crate::rpc_server::proto::{
    AcceptRequest, Ballot, CommitRequest, DependencySet, EventPayload, LastApplied,
    LogicalTimestamp, PreAcceptRequest,
};
use crate::rpc_server::transport::ConsensusTransportHandler;

#[async_trait(?Send)]
pub trait ConsensusPeerTransport: Send {
    async fn pre_accept_peer(&mut self, peer_id: &str, request: PreAcceptRequest)
    -> So3Result<()>;
    async fn accept_peer(&mut self, peer_id: &str, request: AcceptRequest) -> So3Result<()>;
    async fn commit_peer(&mut self, peer_id: &str, request: CommitRequest) -> So3Result<()>;
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
        let timestamp_zero = logical_timestamp(&self.config.node_id, command_id.sequence());
        let dependencies = empty_dependencies();

        self.pre_accept_all(command_id, &command_bytes, &timestamp_zero)
            .await?;
        self.accept_all(command_id, &command_bytes, &timestamp_zero, &dependencies)
            .await?;
        let result = self
            .commit_all(command_id, &command_bytes, &timestamp_zero, &dependencies)
            .await?;

        ObjectResult::from_bytes(&result)
    }

    async fn pre_accept_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
    ) -> So3Result<()> {
        let request = PreAcceptRequest {
            command_id: Some(command_id_proto(command_id)),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            last_applied: Some(LastApplied {
                commands: Vec::new(),
            }),
        };

        self.local_transport
            .pre_accept(request.clone())
            .await
            .map_err(|status| map_status(&status))?;
        for peer_id in &self.config.peer_ids {
            self.peer_transport
                .pre_accept_peer(peer_id, request.clone())
                .await?;
        }

        Ok(())
    }

    async fn accept_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        dependencies: &DependencySet,
    ) -> So3Result<()> {
        let request = AcceptRequest {
            command_id: Some(command_id_proto(command_id)),
            ballot: Some(ballot(&self.config.node_id)),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            timestamp: Some(timestamp_zero.clone()),
            dependencies: Some(dependencies.clone()),
            last_applied: Some(LastApplied {
                commands: Vec::new(),
            }),
        };

        self.local_transport
            .accept(request.clone())
            .await
            .map_err(|status| map_status(&status))?;
        for peer_id in &self.config.peer_ids {
            self.peer_transport
                .accept_peer(peer_id, request.clone())
                .await?;
        }

        Ok(())
    }

    async fn commit_all(
        &mut self,
        command_id: &ConsensusCommandId,
        command: &[u8],
        timestamp_zero: &LogicalTimestamp,
        dependencies: &DependencySet,
    ) -> So3Result<Vec<u8>> {
        let request = CommitRequest {
            command_id: Some(command_id_proto(command_id)),
            event: Some(event_payload(command)),
            timestamp_zero: Some(timestamp_zero.clone()),
            timestamp: Some(timestamp_zero.clone()),
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

fn logical_timestamp(node_id: &str, counter: u64) -> LogicalTimestamp {
    LogicalTimestamp {
        epoch: 0,
        counter,
        node_id: node_id.to_owned(),
    }
}

fn empty_dependencies() -> DependencySet {
    DependencySet {
        commands: Vec::new(),
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
