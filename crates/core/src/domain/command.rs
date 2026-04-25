use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};
use serde::{Deserialize, Serialize};

use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectKey, ObjectVersion, StoredObject};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectCommand {
    Read(ReadCommand),
    Write(WriteCommand),
    Cas(CasCommand),
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

impl ObjectResult {
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
    use crate::domain::{
        CasCommand, ObjectCommand, ObjectKey, ObjectResult, ObjectVersion, ReadCommand, ReadResult,
        WriteCommand,
    };

    const KEY_ALPHA: &str = "alpha";
    const READ_KEY: &str = "r";
    const WRITE_KEY: &str = "w";
    const PAYLOAD: &[u8] = b"payload";
    const WRITE_VALUE: &[u8] = b"v";
    const EXPECTED_VERSION: i64 = 7;

    #[test]
    fn object_command_roundtrip_is_stable() {
        let command = ObjectCommand::Cas(CasCommand {
            key: ObjectKey::new(KEY_ALPHA).unwrap(),
            expected_version: ObjectVersion::try_from(EXPECTED_VERSION).unwrap(),
            value: PAYLOAD.to_vec(),
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
            value: WRITE_VALUE.to_vec(),
        });

        assert!(matches!(read, ObjectCommand::Read(_)));
        assert!(matches!(write, ObjectCommand::Write(_)));
    }

    #[test]
    fn object_result_roundtrip_is_stable() {
        let result = ObjectResult::Read(ReadResult { object: None });

        let encoded = result.to_bytes().unwrap();
        let decoded = ObjectResult::from_bytes(&encoded).unwrap();

        assert_eq!(decoded, result);
    }
}
