use crate::primitive::node::NodeId;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct Timestamp {
    pub msb: i64,
    pub lsb: i64,
    pub node: NodeId,
}

impl Timestamp {
    const MAX: Self = Timestamp { msb: i64::MAX, lsb: i64::MAX, node: NodeId::MAX };
    const NONE: Self = Timestamp { msb: 0, lsb: 0, node: NodeId::NONE };

    const REJECTED_FLAG: i32 = 0x8000;

    /**
     * The set of flags we want to retain as we merge timestamps (e.g. when taking mergeMax).
     * Today this is only the REJECTED_FLAG, but we may include additional flags in future (such as Committed, Applied...)
     * which we may also want to retain when merging in other contexts (such as in Deps).
     */
    const MERGE_FLAGS: i32 = 0x8000;
    const IDENTITY_LSB: u64 = 0xFFFFFFFF_FFFF001F;
    pub const IDENTITY_FLAGS: i32 = 0x00000000_0000001F;
    pub const MAX_EPOCH: i64 = (1 << 40) - 1;
    const MLC_INCR: i64 = 1 << 10;
    const MAX_FLAGS: i64 = Self::MLC_INCR - 1;

    pub fn from_bits(msb: i64, lsb: i64, node: NodeId) -> Self {
        Self {
            msb,
            lsb,
            node,
        }
    }

    pub fn from_values(epoch: i64, hlc: i64, node: NodeId) -> Self {}

    pub fn from_values_
}