use crate::primitives::{Ballot, Dependency, ObjectKey, TxnId};
use crate::timestamp::Timestamp;

pub enum ConsensusMessageType<T: ObjectKey> {
    PreAccept {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
    },
    PreAcceptOk {
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency<T>>,
    },
    Accept {
        ballot: Ballot,
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency<T>>,
    },
    Nack,
    AcceptOk {
        deps: Vec<Dependency<T>>,
    },
}

pub enum ExecutionMessageType<T: ObjectKey> {
    Commit {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency<T>>,
    },
    Read {
        txn_id: TxnId,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency<T>>,
    },
    ReadOk {
        result: Vec<Dependency<T>>,
    },
    Apply {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        result: Vec<Dependency<T>>,
    },
}

pub enum ReconfigurationMessageType {
    JoinElectorate { txn_ids: Vec<TxnId>, epoch: u64 },
    JoinShard { txn_ids: Vec<TxnId>, epoch: u64 },
}

pub enum RecoveryMessageType {
    Recover {
        ballot: Ballot,
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
    },
    Nack {
        ballot: Ballot,
    },
    RecoverOk {
        txn_id: TxnId,
        superseding_txn_ids: Vec<TxnId>,
        wait_txn_ids: Vec<TxnId>,
    },
}
