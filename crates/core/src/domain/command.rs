use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::domain::object::version::ObjectVersion;

pub enum ObjectCommand {
    Read {
        key: ObjectKey,
    },
    Write {
        key: ObjectKey,
        blob_id: BlobId,
        sha256: Sha256Digest,
        size: u64,
    },
    Cas {
        key: ObjectKey,
        expected_version: ObjectVersion,
        blob_id: BlobId,
        sha256: Sha256Digest,
        size: u64,
    },
    Delete {
        key: ObjectKey,
    },
}

pub enum CommandResult {
    Read(ReadResult),
    Write(WriteResult),
    Cas(CasResult),
    Delete,
}

pub enum ReadResult {
    Found(ObjectMetadata),
    NotFound,
}

pub struct WriteResult {
    pub metadata: ObjectMetadata,
}

pub enum CasResult {
    Updated(ObjectMetadata),
    Conflict { current_version: ObjectVersion },
}
