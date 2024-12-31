use std::time::Instant;

use uuid::Uuid;

pub struct Timestamp {
    pub epoch: u64,
    pub time: Instant,
    pub seq: u64,
    pub id: Uuid,
}
