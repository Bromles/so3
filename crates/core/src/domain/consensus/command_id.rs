use crate::domain::node::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandId {
    pub origin_node_id: NodeId,
    pub sequence: u64,
}

impl CommandId {
    #[must_use]
    pub fn new(origin_node_id: NodeId, sequence: u64) -> Self {
        Self {
            origin_node_id,
            sequence,
        }
    }

    #[must_use]
    pub fn origin_node_id(&self) -> &str {
        &self.origin_node_id
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DependencySet {
    pub commands: Vec<CommandId>,
}

impl DependencySet {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }
}
