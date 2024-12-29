use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;

use crate::local::node::NodeId;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Timestamp {
    pub msb: i64,
    pub lsb: i64,
    pub node: Arc<NodeId>,
}

pub trait TimestampBase: Sized {
    fn from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self {
        Self { msb, lsb, node }
    }
    fn from_values(epoch: i64, hlc: i64, node: Arc<NodeId>) -> Self {
        Self::new_from_values_with_flags(epoch, hlc, 0, node)
    }

    fn from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
        Self::new_from_values_with_flags(epoch, hlc, flags, node)
    }

    fn from_values_with_flags_int_id(epoch: i64, hlc: i64, flags: i32, node: i32) -> Self {
        Self::new_from_values_with_flags(epoch, hlc, flags, Arc::new(NodeId { id: node }))
    }

    fn max_for_epoch(epoch: i64) -> Self {
        Self::new_from_bits(
            Self::epoch_msb(epoch) | 0x7fff,
            i64::MAX,
            Arc::new(NodeId::MAX),
        )
    }

    fn min_for_epoch(epoch: i64) -> Self {
        Self::new_from_bits(Self::epoch_msb(epoch), 0, Arc::new(NodeId::NONE))
    }

    fn copy(copy: impl TimestampBase) -> Self;

    fn copy_with_id(copy: impl TimestampBase, node: Arc<NodeId>) -> Self;

    fn new_from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self;

    fn new_from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self;

    fn copy_with_flags(copy: impl TimestampBase, flags: i32) -> Self;

    fn msb(&self) -> i64;
    fn lsb(&self) -> i64;
    fn node(&self) -> Arc<NodeId>;

    fn epoch(&self) -> i64 {
        Self::epoch_from_msb(self.msb())
    }

    fn hlc(&self) -> i64 {
        Self::highHlc(self.msb()) | Self::lowHlc(self.lsb())
    }

    fn flags(&self) -> i32 {
        Self::flags_from_lsb(self.lsb())
    }

    fn is_rejected(&self) -> bool {
        self.lsb() & Timestamp::REJECTED_FLAG as i64 != 0
    }

    fn as_rejected(&self) -> Self {
        self.with_extra_flags(Timestamp::REJECTED_FLAG)
    }

    fn with_next_hlc(&self, hlc_at_least: i64) -> Self {
        Self::from_values_with_flags(
            self.epoch(),
            i64::max(hlc_at_least, self.hlc() + 1),
            self.flags(),
            self.node(),
        )
    }

    fn with_epoch_at_least(&self, min_epoch: i64) -> Self {
        if min_epoch <= self.epoch() {
            self
        } else {
            Self::from_values_with_flags(min_epoch, self.hlc(), self.flags(), self.node())
        }
    }

    fn with_epoch(&self, epoch: i64) -> Self {
        if epoch == self.epoch() {
            self
        } else {
            Self::from_values_with_flags(epoch, self.hlc(), self.flags(), self.node())
        }
    }

    fn with_extra_flags(&self, flags: i32) -> Self {
        let new_lsb = self.lsb() | flags as i64;
        if self.lsb() == new_lsb {
            return self;
        }

        Self::from_bits(self.msb(), new_lsb, self.node())
    }

    fn merge_flags(&self, merge_flags: impl TimestampBase) -> Self {
        let new_lsb = self.lsb() | (merge_flags.lsb() & Timestamp::MERGE_FLAGS as i64);
        if self.lsb() == new_lsb {
            return self;
        }

        Self::from_bits(self.msb(), new_lsb, self.node())
    }

    fn logical_next(&self, node: Arc<NodeId>) -> Self {
        let lsb = self.lsb() + Timestamp::HLC_INCR;
        let mut msb = self.msb();
        if self.low_hlc(lsb) == 0 {
            msb += 1;
        }

        Self::from_bits(msb, lsb, node)
    }

    fn next(&self) -> Self {
        if (self.node().id as i64) < i64::MAX {
            Self::from_values(
                self.msb(),
                self.lsb(),
                Arc::new(NodeId {
                    id: self.node().id + 1,
                }),
            )
        } else {
            self.logical_next(Arc::new(NodeId::NONE))
        }
    }

    fn compare_msb(msb_a: i64, msb_b: i64) -> Ordering {
        msb_a.cmp(&msb_b)
    }

    fn compare_lsb(lsb_a: i64, lsb_b: i64) -> Ordering {
        let c: i32 = Self::low_hlc(lsb_a).cmp(&Self::low_hlc(lsb_b));

        if c != 0 {
            c
        } else {
            (lsb_a & Timestamp::IDENTITY_FLAGS).cmp(&(lsb_b & Timestamp::IDENTITY_FLAGS))
        }
    }

    fn compare_without_epoch(&self, that: &impl TimestampBase) -> Ordering {
        if *self == that {
            return Ordering::Equal;
        }

        let mut c = Self::high_hlc(self.msb()).cmp(&Self::high_hlc(that.msb()));

        if c == 0 {
            c = Self::compare_lsb(self.lsb(), that.lsb());
        }

        if c == 0 {
            c = self.node().cmp(that.node())
        }

        c
    }

    fn max<T: TimestampBase>(a: &T, b: &T) -> T {
        match a.cmp(b) {
            Ordering::Less => b,
            Ordering::Equal => a,
            Ordering::Greater => a,
        }
    }

    fn merge_max<T: TimestampBase>(a: T, b: T) -> T {
        // Note: it is not safe to take the highest HLC while retaining the current node;
        //       however, it is safe to take the highest epoch, as the originating node will always advance the hlc()
        if a.compare_without_epoch(&b) == Ordering::Equal
            || a.compare_without_epoch(&b) == Ordering::Greater
        {
            a.merge_flags(b).with_epoch_at_least(b.epoch())
        } else {
            b.merge_flags(a).with_epoch_at_least(a.epoch())
        }
    }

    fn non_null_or_max<T: TimestampBase>(a: T, b: T) -> T {
        Self::max(a, b)
    }

    fn non_null_or_min<T: TimestampBase>(a: T, b: T) -> T {
        Self::min(a, b)
    }

    fn min<T: TimestampBase>(a: T, b: T) -> T {
        if a.cmp(&b) == Ordering::Greater {
            b
        } else {
            a
        }
    }

    fn epoch_msb(epoch: i64) -> i64 {
        epoch << 15
    }

    fn merge(&self, that: impl TimestampBase) -> impl TimestampBase {
        Self::merge_with_fn(self, that, Self::from_bits)
    }

    fn merge_with_fn<T: TimestampBase>(
        a: impl TimestampBase,
        b: impl TimestampBase,
        constructor: impl FnOnce(i64, i64, Arc<NodeId>) -> T,
    ) -> impl TimestampBase {
        constructor(a.msb(), a.lsb() | b.lsb(), a.node())
    }

    fn to_standard_string(self) -> String {
        format!(
            "[{},{},{},{}]",
            self.epoch(),
            self.hlc(),
            self.flags(),
            self.node().id
        )
    }

    fn from_string(string: String) -> impl TimestampBase {
        let string = string.replacen("\\[", "", 1);

        let string = string.replacen("\\]", "", 1);

        let split: Vec<&str> = string.split(",").collect();

        Self::from_values_with_flags(
            split.get(0).map(|s| i64::from_str(*s)).unwrap().unwrap(),
            split.get(1).map(|s| i64::from_str(*s)).unwrap().unwrap(),
            split
                .get(2)
                .map(|s| i32::from_str_radix(*s, 2).unwrap())
                .unwrap(),
            Arc::new(NodeId {
                id: split.get(3).map(|s| i32::from_str(*s).unwrap()).unwrap(),
            }),
        )
    }
}

impl<T: TimestampBase> TimestampBase for &T {
    fn copy(copy: impl TimestampBase) -> Self {
        Self::copy(copy)
    }

    fn copy_with_id(copy: impl TimestampBase, node: Arc<NodeId>) -> Self {
        Self::copy_with_id(copy, node)
    }

    fn new_from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
        Self::new_from_values_with_flags(epoch, hlc, flags, node)
    }

    fn new_from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self {
        Self::new_from_bits(msb, lsb, node)
    }

    fn copy_with_flags(copy: impl TimestampBase, flags: i32) -> Self {
        Self::copy_with_flags(copy, flags)
    }

    fn msb(&self) -> i64 {
        self.msb()
    }

    fn lsb(&self) -> i64 {
        self.lsb()
    }

    fn node(&self) -> Arc<NodeId> {
        self.node()
    }
}

#[macro_export] 
macro_rules! impl_timestamp_base {
    ($t:ty) => {
        impl TimestampBase for $t {
            fn copy(copy: impl TimestampBase) -> Self {
                Self::new_from_bits(copy.msb(), copy.lsb(), copy.node())
            }
        
            fn copy_with_id(copy: impl TimestampBase, node: Arc<NodeId>) -> Self {
                Self::new_from_bits(copy.msb(), copy.lsb(), node)
            }
        
            fn new_from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
                Self::from_values_with_flags(epoch, hlc, flags, node)
            }
        
            fn new_from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self {
                Self::from_bits(msb, lsb, node)
            }
        
            fn copy_with_flags(copy: impl TimestampBase, flags: i32) -> Self {
                Self::from_values_with_flags(copy.epoch(), copy.hlc(), flags, copy.node())
            }
        
            fn msb(&self) -> i64 {
                self.timestamp.msb()
            }
        
            fn lsb(&self) -> i64 {
                self.timestamp.lsb()
            }
        
            fn node(&self) -> Arc<NodeId> {
                self.timestamp.node()
            }
        }       
    };
}

impl Timestamp {
    pub const MAX: Self = Timestamp {
        msb: i64::MAX,
        lsb: i64::MAX,
        node: Arc::new(NodeId::MAX),
    };
    pub const NONE: Self = Timestamp {
        msb: 0,
        lsb: 0,
        node: Arc::new(NodeId::NONE),
    };

    const REJECTED_FLAG: i32 = 0x8000;
    /**
     * The set of flags we want to retain as we merge timestamps (e.g. when taking mergeMax).
     * Today this is only the REJECTED_FLAG, but we may include additional flags in future (such as Committed, Applied...)
     * which we may also want to retain when merging in other contexts (such as in Deps).
     */
    const MERGE_FLAGS: i32 = 0x8000;
    const IDENTITY_LSB: u64 = 0xFFFFFFFF_FFFF001F;
    const HLC_INCR: i64 = 1 << 10;
    const IDENTITY_FLAGS: i32 = 0x00000000_0000001F;
    const MAX_EPOCH: i64 = (1 << 40) - 1;
    const MAX_FLAGS: i64 = Self::HLC_INCR - 1;

    fn epoch_from_msb(msb: i64) -> i64 {
        msb << 15
    }

    fn hlc_msb(hlc: i64) -> i64 {
        ((hlc as u64) >> 40) as i64
    }

    fn hlc_lsb(hlc: i64) -> i64 {
        hlc << 16
    }

    fn high_hlc(msb: i64) -> i64 {
        (msb & 0x7fff) << 40
    }

    fn low_hlc(lsb: i64) -> i64 {
        ((lsb as u64) >> 16) as i64
    }

    fn flags_from_lsb(lsb: i64) -> i32 {
        (lsb & Timestamp::MAX_FLAGS) as i32
    }

    fn not_flags_from_lsb(lsb: i64) -> i64 {
        lsb & !Timestamp::MAX_FLAGS
    }
}

impl TimestampBase for Timestamp {
    fn copy(copy: impl TimestampBase) -> Self {
        Self {
            msb: copy.msb(),
            lsb: copy.lsb(),
            node: copy.node(),
        }
    }

    fn copy_with_id(copy: impl TimestampBase, node: Arc<NodeId>) -> Self {
        Self {
            msb: copy.msb(),
            lsb: copy.lsb(),
            node,
        }
    }

    fn new_from_values_with_flags(epoch: i64, hlc: i64, flags: i32, node: Arc<NodeId>) -> Self {
        Self {
            msb: Self::epoch_msb(epoch) | Self::hlc_msb(hlc),
            lsb: Self::hlc_lsb(hlc) | flags as i64,
            node,
        }
    }

    fn new_from_bits(msb: i64, lsb: i64, node: Arc<NodeId>) -> Self {
        Self { msb, lsb, node }
    }

    fn copy_with_flags(copy: impl TimestampBase, flags: i32) -> Self {
        Self {
            msb: copy.msb(),
            lsb: Self::not_flags_from_lsb(copy.lsb()) | flags as i64,
            node: copy.node(),
        }
    }

    fn msb(&self) -> i64 {
        self.msb
    }

    fn lsb(&self) -> i64 {
        self.lsb
    }

    fn node(&self) -> Arc<NodeId> {
        self.node.clone()
    }
}
