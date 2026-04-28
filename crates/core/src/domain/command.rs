use crate::domain::blob::BlobMetadata;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::{ObjectLastModified, ObjectMetadata};
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectCommand {
    Read(ReadCommand),
    Write(WriteCommand),
    Cas(CasCommand),
    Delete(DeleteCommand),
}

impl ObjectCommand {
    /// # Errors
    ///
    /// Returns [`So3Error::Serialization`] when postcard cannot encode the command.
    pub fn to_bytes(&self) -> So3Result<Vec<u8>> {
        postcard_to_allocvec(self).map_err(So3Error::from)
    }

    /// # Errors
    ///
    /// Returns [`So3Error::Serialization`] when postcard cannot decode the command payload.
    pub fn from_bytes(bytes: &[u8]) -> So3Result<Self> {
        postcard_from_bytes(bytes).map_err(So3Error::from)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadCommand {
    pub key: ObjectKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteCommand {
    pub key: ObjectKey,
    pub metadata: BlobMetadata,
    pub last_modified: ObjectLastModified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasCommand {
    pub key: ObjectKey,
    pub expected_version: ObjectVersion,
    pub metadata: BlobMetadata,
    pub last_modified: ObjectLastModified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteCommand {
    pub key: ObjectKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    Read(ReadResult),
    Write(WriteResult),
    Cas(CasResult),
    Delete(DeleteResult),
}

impl CommandResult {
    /// # Errors
    ///
    /// Returns [`So3Error::Serialization`] when postcard cannot encode the result.
    pub fn to_bytes(&self) -> So3Result<Vec<u8>> {
        postcard_to_allocvec(self).map_err(So3Error::from)
    }

    /// # Errors
    ///
    /// Returns [`So3Error::Serialization`] when postcard cannot decode the result payload.
    pub fn from_bytes(bytes: &[u8]) -> So3Result<Self> {
        postcard_from_bytes(bytes).map_err(So3Error::from)
    }
}

/// Result of a replicated `Read` command. Contains only metadata; blob bytes are loaded
/// on demand by the caller via [`crate::repository::blob::BlobRepository`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResult {
    pub record: Option<ObjectMetadata>,
}

/// Result of a replicated `Write` command. Contains only metadata; blob bytes are never
/// stored in the consensus or applied-command tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResult {
    pub record: ObjectMetadata,
}

/// Result of a replicated `CAS` command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasResult {
    /// CAS succeeded; the new record is returned (no blob bytes).
    Applied(ObjectMetadata),
    NotFound,
    Mismatch {
        current_version: ObjectVersion,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResult;

#[cfg(test)]
mod tests {
    use crate::domain::blob::BlobId;
    use crate::domain::checksum::Sha256Digest;
    use crate::domain::command::{
        BlobMetadata, CasCommand, CommandResult, ObjectCommand, ReadCommand, ReadResult,
        WriteCommand,
    };
    use crate::domain::object::ObjectLastModified;
    use crate::domain::object_key::ObjectKey;
    use crate::domain::object_version::ObjectVersion;

    const KEY_ALPHA: &str = "alpha";
    const READ_KEY: &str = "r";
    const WRITE_KEY: &str = "w";
    const BLOB_ID: BlobId = "abc123def456.blob".into();
    const CHECKSUM: Sha256Digest = "abc123def456".into();
    const CONTENT_LENGTH: u64 = 7;
    const EXPECTED_VERSION: i64 = 7;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;

    fn test_payload() -> BlobMetadata {
        BlobMetadata {
            blob_id: BLOB_ID.to_owned(),
            content_length: CONTENT_LENGTH,
            checksum_sha256: CHECKSUM.to_owned(),
        }
    }

    #[test]
    fn object_command_roundtrip_is_stable() {
        let command = ObjectCommand::Cas(CasCommand {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            expected_version: ObjectVersion::try_from(EXPECTED_VERSION).unwrap(),
            metadata: test_payload(),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        });

        let encoded = command.to_bytes().unwrap();
        let decoded = ObjectCommand::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, command);
    }

    #[test]
    fn stored_payload_roundtrip_is_stable() {
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            metadata: test_payload(),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        });

        let encoded = command.to_bytes().unwrap();
        let decoded = ObjectCommand::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, command);
    }

    #[test]
    fn read_and_write_commands_construct_cleanly() {
        let read = ObjectCommand::Read(ReadCommand {
            key: ObjectKey::new(READ_KEY).unwrap(),
        });
        let write = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(WRITE_KEY).unwrap(),
            metadata: test_payload(),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        });

        assert!(matches!(read, ObjectCommand::Read(_)));
        assert!(matches!(write, ObjectCommand::Write(_)));
    }

    #[test]
    fn object_result_roundtrip_is_stable() {
        let result = CommandResult::Read(ReadResult { record: None });

        let encoded = result.to_bytes().unwrap();
        let decoded = CommandResult::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, result);
    }
}
