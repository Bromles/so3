use crate::domain::error::So3Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectVersion(i64);

impl ObjectVersion {
    pub fn initial() -> Self {
        Self(1)
    }
    #[must_use]
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for ObjectVersion {
    type Error = So3Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 1 {
            Err(So3Error::InvalidVersion(value))
        } else {
            Ok(Self(value))
        }
    }
}
