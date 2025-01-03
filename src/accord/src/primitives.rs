use std::hash::Hash;

#[derive(Debug)]
pub struct TxnId(pub u64);

pub struct Ballot(pub u64);

#[derive(Eq, PartialEq, Hash)]
pub struct SimpleObjectKey(pub String);

#[derive(Eq, PartialEq, Hash)]
pub struct RangedObjectKey {
    pub from: SimpleObjectKey,
    pub to: SimpleObjectKey,
}

pub trait ObjectKey: Hash {
    fn match_objects(&self) -> Vec<&SimpleObjectKey>;
}

impl ObjectKey for SimpleObjectKey {
    fn match_objects(&self) -> Vec<&SimpleObjectKey> {
        vec![self]
    }
}

impl ObjectKey for RangedObjectKey {
    fn match_objects(&self) -> Vec<&SimpleObjectKey> {
        vec![&self.from, &self.to]
    }
}

pub struct Dependency<T: ObjectKey> {
    pub key: T,
    pub txn_id: TxnId,
}
