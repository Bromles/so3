pub struct TxnId(pub u64);

pub struct Ballot(pub u64);

pub struct ObjectKey(pub String);

pub struct Dependency(pub ObjectKey, pub TxnId);
