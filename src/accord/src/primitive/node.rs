#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct NodeId {
    pub id: i32,
}

impl NodeId {
    const MAX: Self = NodeId { id: i32::MAX };
    const NONE: Self = NodeId { id: 0 };
}
