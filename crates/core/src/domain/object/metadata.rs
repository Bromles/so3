use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::version::ObjectVersion;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectMetadata {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub blob_id: BlobId,
    pub sha256: Sha256Digest,
    pub size: u64,
    pub last_modified_ms: u64,
    pub deleted: bool,
}

pub struct StoredObject {
    pub metadata: ObjectMetadata,
    pub blob: BlobStream,
}
