use serde::{Deserialize, Serialize};

use crate::domain::error::{So3Error, So3Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectVersion(i64);

impl ObjectVersion {
    #[must_use]
    pub fn initial() -> Self {
        Self(1)
    }

    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[must_use]
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

#[cfg(test)]
mod tests {
    use crate::domain::ObjectVersion;
    use crate::domain::error::So3Error;

    #[test]
    fn object_version_rejects_non_positive_numbers() {
        let error = ObjectVersion::try_from(0).unwrap_err();
        assert!(matches!(error, So3Error::InvalidVersion(0)));
    }
}
