use crate::reorder_buffer::ReorderBuffer;
use uuid::Uuid;

pub trait Node {}

pub struct ClusterNode {
    pub id: Uuid,
}

impl Node for ClusterNode {}

pub struct LocalClusterNode {
    pub id: Uuid,
    pub reorder_buffer: ReorderBuffer,
}

impl Node for LocalClusterNode {}
