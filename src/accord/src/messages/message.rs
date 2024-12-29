pub enum ConsensusMessageType {
    PreAccept,
    PreAcceptOk,
    Accept,
    AcceptOk,
}

pub enum ExecutionMessageType {
    Commit,
    Read,
    ReadOk,
    Apply,
}

pub enum ReconfigurationMessageType {
    JoinElectorate,
    JoinShard,
}

pub enum RecoveryMessageType {
    Recover,
    Nack,
    RecoverOk,
}
