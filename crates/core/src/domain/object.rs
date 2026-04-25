use serde::{Deserialize, Serialize};

use crate::domain::{ObjectKey, ObjectVersion};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub blob_id: String,
    pub content_length: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject {
    pub record: ObjectRecord,
    pub value: Vec<u8>,
}
