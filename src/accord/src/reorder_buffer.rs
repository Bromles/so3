use crate::timestamp::Timestamp;
use std::collections::BinaryHeap;

pub struct ReorderBuffer {
    buffer: BinaryHeap<Timestamp>,
}

impl ReorderBuffer {}
