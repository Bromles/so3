use std::collections::BinaryHeap;

use crate::timestamp::Timestamp;

pub struct ReorderBuffer {
    buffer: BinaryHeap<Timestamp>,
}

impl ReorderBuffer {}
