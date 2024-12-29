use std::sync::Arc;
use crate::local::node::NodeId;
use crate::primitive::timestamp::{Timestamp, TimestampBase};

pub struct TxnId {
    pub timestamp: Timestamp
}

impl_timestamp_base!(TxnId);

pub trait TxnIdBase: Sized {
    
}

impl<T: TxnIdBase> TxnIdBase for &T{}

impl TxnId {
    pub const NO_TXNIDS: [Self; 0] = [];
    pub const NONE: Self = TxnId{timestamp: Timestamp{msb: 0, lsb: 0, node: Arc::new(NodeId::NONE)}};
    pub const MAX: Self = TxnId{timestamp: Timestamp{msb: i64::MAX, lsb: i64::MAX, node: Arc::new(NodeId::MAX)}};
    
    
}

impl TxnIdBase for TxnId {
    
}
