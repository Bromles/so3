use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::clock::LogicalTimestamp;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::version::ObjectVersion;

#[derive(Debug)]
pub struct ObjectMetadata {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub blob_id: BlobId,
    pub sha256: Sha256Digest,
    pub size: u64,
    pub last_modified: LogicalTimestamp,
}

pub struct StoredObject {
    pub metadata: ObjectMetadata,
    pub blob: BlobPayload,
}