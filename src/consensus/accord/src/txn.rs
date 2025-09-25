use crate::primitives::TxnId;

pub struct Txn {
    pub local_state: LocalTxnState,
    pub id: TxnId,
}

#[derive(Default)]
pub enum LocalTxnState {
    #[default]
    Unknown,
    PreAccepted,
    Accepted,
    Commited,
    Applied,
}
