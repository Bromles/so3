use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};
use serde::{Deserialize, Serialize};

use crate::domain::error::{So3Error, So3Result};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> So3Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(So3Error::InvalidKey);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = So3Error;

    fn try_from(value: String) -> So3Result<Self> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = So3Error;

    fn try_from(value: &str) -> So3Result<Self> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectVersion(i64);

impl ObjectVersion {
    pub fn initial() -> Self {
        Self(1)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for ObjectVersion {
    type Error = So3Error;

    fn try_from(value: i64) -> So3Result<Self> {
        if value < 1 {
            return Err(So3Error::InvalidVersion(value));
        }

        Ok(Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub blob_id: String,
    pub content_length: u64,
    pub checksum: String,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject {
    pub record: ObjectRecord,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectCommand {
    Read(ReadCommand),
    Write(WriteCommand),
    Cas(CasCommand),
}

impl ObjectCommand {
    pub fn to_bytes(&self) -> So3Result<Vec<u8>> {
        postcard_to_allocvec(self).map_err(So3Error::from)
    }

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
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasCommand {
    pub key: ObjectKey,
    pub expected_version: ObjectVersion,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectResult {
    Read(ReadResult),
    Write(WriteResult),
    Cas(CasResult),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadResult {
    pub object: Option<StoredObject>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteResult {
    pub object: StoredObject,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasResult {
    Applied(StoredObject),
    NotFound,
    Mismatch { current_version: ObjectVersion },
}

#[cfg(test)]
mod tests {
    use super::{CasCommand, ObjectCommand, ObjectKey, ObjectVersion, ReadCommand, WriteCommand};
    use crate::domain::error::So3Error;

    #[test]
    fn object_key_rejects_blank_values() {
        let error = ObjectKey::new("   ").unwrap_err();
        assert!(matches!(error, So3Error::InvalidKey));
    }

    #[test]
    fn object_version_rejects_non_positive_numbers() {
        let error = ObjectVersion::try_from(0).unwrap_err();
        assert!(matches!(error, So3Error::InvalidVersion(0)));
    }

    #[test]
    fn object_command_roundtrip_is_stable() {
        let command = ObjectCommand::Cas(CasCommand {
            key: ObjectKey::new("alpha").unwrap(),
            expected_version: ObjectVersion::try_from(7).unwrap(),
            value: b"payload".to_vec(),
        });

        let encoded = command.to_bytes().unwrap();
        let decoded = ObjectCommand::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, command);
    }

    #[test]
    fn read_and_write_commands_construct_cleanly() {
        let read = ObjectCommand::Read(ReadCommand {
            key: ObjectKey::new("r").unwrap(),
        });
        let write = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new("w").unwrap(),
            value: b"v".to_vec(),
        });

        assert!(matches!(read, ObjectCommand::Read(_)));
        assert!(matches!(write, ObjectCommand::Write(_)));
    }
}
