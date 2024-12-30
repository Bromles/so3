use std::any::Any;
use crate::timestamp::Timestamp;

pub enum ConsensusMessageType {
    PreAccept {
        txn_id: u64,
        proposed_timestamp: Timestamp
    },
    PreAcceptOk {
        accepted_timestamp: Timestamp,
        deps: Vec<Box<dyn Any>>
    },
    Accept {
        ballot: u64,
        txn_id: u64,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Box<dyn Any>>
    },
    Nack,
    AcceptOk {
        deps: Vec<Box<dyn Any>>
    },
}

pub enum ExecutionMessageType {
    Commit {
        txn_id: u64,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Box<dyn Any>>
    },
    Read {
        txn_id: u64,
        accepted_timestamp: Timestamp,
        deps: Vec<Box<dyn Any>>
    },
    ReadOk {
        result: Vec<Box<dyn Any>>
    },
    Apply {
        txn_id: u64,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        result: Vec<Box<dyn Any>>
    },
}

pub enum ReconfigurationMessageType {
    JoinElectorate {
        txn_ids: Vec<u64>,
        epoch: u64
    },
    JoinShard {
        txn_ids: Vec<u64>,
        epoch: u64
    },
}

pub enum RecoveryMessageType {
    Recover {
        ballot: u64,
        txn_id: u64,
        proposed_timestamp: Timestamp
    },
    Nack {
        ballot: u64
    },
    RecoverOk {
        txn_id: u64,
        superseding_txn_ids: Vec<u64>,
        wait_txn_ids: Vec<u64>,
    },
}
