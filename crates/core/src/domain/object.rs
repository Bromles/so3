use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::domain::error::{So3Error, So3Result};
use crate::domain::{ObjectKey, ObjectVersion};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectLastModified {
    unix_millis: i64,
}

impl ObjectLastModified {
    /// # Errors
    ///
    /// Returns an error if the system clock is before the Unix epoch or exceeds the supported
    /// persisted timestamp range.
    pub fn now() -> So3Result<Self> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| So3Error::Storage(format!("system clock is before epoch: {error}")))?;
        let millis = i64::try_from(duration.as_millis()).map_err(|_| {
            So3Error::Storage("system time exceeds supported timestamp range".to_owned())
        })?;

        Self::try_from(millis)
    }

    #[must_use]
    pub fn unix_millis(self) -> i64 {
        self.unix_millis
    }
}

impl TryFrom<i64> for ObjectLastModified {
    type Error = So3Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(So3Error::Storage(format!(
                "last_modified_unix_millis cannot be negative: {value}"
            )));
        }

        Ok(Self { unix_millis: value })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    pub blob_id: String,
    pub content_length: u64,
    pub checksum: String,
    pub last_modified: ObjectLastModified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject {
    pub record: ObjectRecord,
    pub value: Vec<u8>,
}
