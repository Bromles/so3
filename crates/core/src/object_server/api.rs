use serde::{Deserialize, Serialize};

use crate::domain::StoredObject;

pub const OBJECT_ROUTE_PATH: &str = "/objects/{key}";
pub const OBJECT_METADATA_ROUTE_PATH: &str = "/objects/{key}/metadata";
pub const S3_OBJECT_ROUTE_PATH: &str = "/{bucket}/{*key}";
pub const VERSION_HEADER: &str = "x-so3-version";
pub const S3_VERSION_ID_HEADER: &str = "x-amz-version-id";
pub const S3_OBJECT_SIZE_HEADER: &str = "x-amz-object-size";
pub const S3_STORAGE_CLASS_HEADER: &str = "x-amz-storage-class";
pub const ETAG_HEADER: &str = "etag";
pub const LAST_MODIFIED_HEADER: &str = "last-modified";
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
    pub last_modified_unix_millis: i64,
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
            last_modified_unix_millis: object.record.last_modified.unix_millis(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: String,
}
