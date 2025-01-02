use std::time::Instant;

use uuid::Uuid;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct Timestamp {
    pub epoch: u64,
    pub time: Instant,
    pub seq: u64,
    pub id: Uuid,
}
