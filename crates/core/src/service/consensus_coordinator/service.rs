use crate::domain::command::ObjectCommand;
use crate::domain::consensus::command_id::CommandId;
use crate::domain::error::So3Result;
use crate::domain::node::NodeId;
use crate::service::consensus_coordinator::interface::ConsensusCoordinatorService;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct AccordConsensusCoordinatorService {
    node_id: NodeId,
    sequence: AtomicU64,
}

impl AccordConsensusCoordinatorService {
    fn next_command_id(&self) -> CommandId {
        CommandId {
            origin_node_id: self.node_id.clone(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl ConsensusCoordinatorService for AccordConsensusCoordinatorService {
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<CommandId> {
        todo!()
    }
}
