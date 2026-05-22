use crate::domain::node::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandId {
    pub origin_node_id: NodeId,
    pub sequence: u64,
}

#[derive(Clone)]
pub struct DependencySet(pub Vec<CommandId>);

#[derive(Clone)]
pub struct AppliedSet(pub Vec<CommandId>);
