use std::sync::Arc;
use crate::impl_timestamp_base;
use crate::local::node::NodeId;

use crate::primitive::timestamp::{Timestamp, TimestampBase};

pub struct Ballot {
    pub timestamp: Timestamp,
}

impl_timestamp_base!(Ballot);

impl Ballot {
    pub const ZERO: Self = Self {
        timestamp: Timestamp::NONE,
    };
    pub const MAX: Self = Self {
        timestamp: Timestamp::MAX,
    };

    pub fn from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self {
        if msb == 0 && lsb == 0 && node.as_ref().eq(&NodeId::NONE) {
            return Self::ZERO;
        }

        Self::new_from_values(msb, lsb, node)
    }

    pub fn from_values(epoch: i64, hlc: i64, node: Arc<NodeId>) -> Self {
        Self::from_values_with_flags(epoch, hlc, 0, node)
    }

    pub fn from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
        if epoch == 0 && hlc == 0 && flags == 0 && node.as_ref().eq(&NodeId::NONE) {
            return Self::ZERO;
        }

        Self::new_from_values_with_flags(epoch, hlc, flags, node)
    }

    fn new_from_values(epoch: i64, hlc: i64, node: Arc<NodeId>) -> Self {
        Self {
            timestamp: Timestamp::from_values(epoch, hlc, node),
        }
    }

    fn new_from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
        Self {
            timestamp: Timestamp::from_values_with_flags(epoch, hlc, flags, node),
        }
    }

    pub fn merge(&self, that: Timestamp) -> Self {
        Timestamp::merge_with_fn(self, that, Self::from_bits)
    }
}
