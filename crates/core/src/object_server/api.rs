use serde::{Deserialize, Serialize};

use crate::domain::StoredObject;

pub const OBJECT_ROUTE_PATH: &str = "/objects/{key}";
pub const OBJECT_METADATA_ROUTE_PATH: &str = "/objects/{key}/metadata";
pub const VERSION_HEADER: &str = "x-so3-version";
pub const ETAG_HEADER: &str = "etag";
pub const DEFAULT_ERROR_LABEL: &str = "error";

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    pub expected_version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectMetadataResponse {
    pub key: String,
    pub version: i64,
    pub checksum: String,
    pub content_length: u64,
}

impl From<StoredObject> for ObjectMetadataResponse {
    fn from(object: StoredObject) -> Self {
        Self::from(&object)
    }
}

impl From<&StoredObject> for ObjectMetadataResponse {
    fn from(object: &StoredObject) -> Self {
        Self {
            key: object.record.key.as_str().to_owned(),
            version: object.record.version.get(),
            checksum: object.record.checksum.clone(),
            content_length: object.record.content_length,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: String,
}
