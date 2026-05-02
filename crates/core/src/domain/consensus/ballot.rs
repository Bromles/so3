use crate::domain::node::NodeId;
use std::cmp::Ordering;

#[derive(Clone)]
pub struct Ballot {
    pub round: u64,
    pub node_id: NodeId,
}

impl Ballot {
    pub fn initial(node_id: NodeId) -> Self {
        Self { round: 0, node_id }
    }
    pub fn next(&self, node_id: NodeId) -> Self {
        Self {
            round: self.round + 1,
            node_id,
        }
    }
}

impl PartialEq for Ballot {
    fn eq(&self, other: &Self) -> bool {
        self.round == other.round && self.node_id.as_ref() == other.node_id.as_ref()
    }
}

impl Eq for Ballot {}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.round
            .cmp(&other.round)
            .then_with(|| self.node_id.as_ref().cmp(other.node_id.as_ref()))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
