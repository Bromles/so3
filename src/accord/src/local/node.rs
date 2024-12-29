#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct NodeId {
    pub id: i32,
}

impl NodeId {
    pub const MAX: Self = NodeId { id: i32::MAX };
    pub const NONE: Self = NodeId { id: 0 };
}

pub struct Node {}
