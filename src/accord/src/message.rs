pub enum ConsensusMessageType {
    PreAccept,
    PreAcceptOk,
    Accept,
    Nack,
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
