use std::fmt::Debug;
use bytes::Bytes;
use serde::Serialize;
use crate::types::error::So3Error;

pub trait Txn: Debug + Clone + Send {
    type TxnErr: Send + Serialize;
    type TxnOk: Send + Serialize;

    fn as_bytes(&self) -> Bytes;
    fn from_bytes(bytes: Bytes) -> Result<Self, So3Error> where Self: Sized;
}


