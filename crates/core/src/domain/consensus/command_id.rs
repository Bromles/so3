use crate::domain::node::NodeId;

pub struct CommandId {
    pub origin_node_id: NodeId,
    pub sequence:       u64,
}

pub struct DependencySet(pub Vec<CommandId>);

pub struct AppliedSet(pub Vec<CommandId>);
