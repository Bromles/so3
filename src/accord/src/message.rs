use crate::primitives::{Ballot, Dependency, TxnId};
use crate::timestamp::Timestamp;

pub enum ConsensusMessageType {
    PreAccept {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
    },
    PreAcceptOk {
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency>,
    },
    Accept {
        ballot: Ballot,
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency>,
    },
    Nack,
    AcceptOk {
        deps: Vec<Dependency>
    },
}

pub enum ExecutionMessageType {
    Commit {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency>,
    },
    Read {
        txn_id: TxnId,
        accepted_timestamp: Timestamp,
        deps: Vec<Dependency>,
    },
    ReadOk {
        result: Vec<Dependency>
    },
    Apply {
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
        accepted_timestamp: Timestamp,
        result: Vec<Dependency>,
    },
}

pub enum ReconfigurationMessageType {
    JoinElectorate {
        txn_ids: Vec<TxnId>,
        epoch: u64,
    },
    JoinShard {
        txn_ids: Vec<TxnId>,
        epoch: u64,
    },
}

pub enum RecoveryMessageType {
    Recover {
        ballot: Ballot,
        txn_id: TxnId,
        proposed_timestamp: Timestamp,
    },
    Nack {
        ballot: Ballot
    },
    RecoverOk {
        txn_id: TxnId,
        superseding_txn_ids: Vec<TxnId>,
        wait_txn_ids: Vec<TxnId>,
    },
}
