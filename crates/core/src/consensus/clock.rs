use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use crate::rpc_server::proto::LogicalTimestamp;

#[derive(Clone, Debug)]
pub struct HybridLogicalClock {
    node_id: String,
    state: Arc<Mutex<HybridLogicalClockState>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct HybridLogicalClockState {
    physical_millis: u64,
    logical_counter: u64,
}

impl HybridLogicalClock {
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self::with_state(node_id, HybridLogicalClockState::default())
    }

    fn with_state(node_id: String, state: HybridLogicalClockState) -> Self {
        Self {
            node_id,
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub async fn tick(&self) -> LogicalTimestamp {
        let mut state = self.state.lock().await;
        let now = physical_millis_now();

        if now > state.physical_millis {
            state.physical_millis = now;
            state.logical_counter = 0;
        } else {
            state.logical_counter = state.logical_counter.saturating_add(1);
        }

        self.timestamp_from_state(*state)
    }

    pub async fn observe(&self, remote: &LogicalTimestamp) -> LogicalTimestamp {
        let mut state = self.state.lock().await;
        let now = physical_millis_now();
        let local_physical = state.physical_millis;
        let local_logical = state.logical_counter;
        let remote_physical = remote.epoch;
        let remote_logical = remote.counter;
        let physical_millis = now.max(local_physical).max(remote_physical);

        let logical_counter =
            if physical_millis == local_physical && physical_millis == remote_physical {
                local_logical.max(remote_logical).saturating_add(1)
            } else if physical_millis == local_physical {
                local_logical.saturating_add(1)
            } else if physical_millis == remote_physical {
                remote_logical.saturating_add(1)
            } else {
                0
            };

        state.physical_millis = physical_millis;
        state.logical_counter = logical_counter;

        self.timestamp_from_state(*state)
    }

    fn timestamp_from_state(&self, state: HybridLogicalClockState) -> LogicalTimestamp {
        LogicalTimestamp {
            epoch: state.physical_millis,
            counter: state.logical_counter,
            node_id: self.node_id.clone(),
        }
    }
}

#[must_use]
pub fn timestamp_is_after(left: &LogicalTimestamp, right: &LogicalTimestamp) -> bool {
    (left.epoch, left.counter, left.node_id.as_str())
        > (right.epoch, right.counter, right.node_id.as_str())
}

fn physical_millis_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();

    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{timestamp_is_after, HybridLogicalClock, HybridLogicalClockState};
    use crate::rpc_server::proto::LogicalTimestamp;

    const NODE_A: &str = "n0";
    const NODE_B: &str = "n1";

    #[tokio::test]
    async fn tick_returns_monotonic_timestamps_for_node() {
        let clock = HybridLogicalClock::new(NODE_A.to_owned());

        let first = clock.tick().await;
        let second = clock.tick().await;

        assert!(timestamp_is_after(&second, &first));
        assert_eq!(first.node_id, NODE_A);
        assert_eq!(second.node_id, NODE_A);
    }

    #[tokio::test]
    async fn observe_advances_past_remote_timestamp() {
        let clock = HybridLogicalClock::with_state(
            NODE_A.to_owned(),
            HybridLogicalClockState {
                physical_millis: 10,
                logical_counter: 2,
            },
        );
        let remote = LogicalTimestamp {
            epoch: u64::MAX - 10,
            counter: 7,
            node_id: NODE_B.to_owned(),
        };

        let observed = clock.observe(&remote).await;

        assert!(timestamp_is_after(&observed, &remote));
        assert_eq!(observed.epoch, remote.epoch);
        assert_eq!(observed.counter, remote.counter + 1);
        assert_eq!(observed.node_id, NODE_A);
    }
}
