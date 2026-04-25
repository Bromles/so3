use serde::{Deserialize, Serialize};

use crate::domain::error::{So3Error, So3Result};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// # Errors
    ///
    /// Returns [`So3Error::InvalidKey`] when the provided value is blank after trimming.
    pub fn new(value: impl Into<String>) -> So3Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(So3Error::InvalidKey);
        }

        Ok(Self(value))
    }

    #[must_use]
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

#[cfg(test)]
mod tests {
    use crate::domain::ObjectKey;
    use crate::domain::error::So3Error;

    const BLANK_KEY: &str = "   ";

    #[test]
    fn object_key_rejects_blank_values() {
        let error = ObjectKey::new(BLANK_KEY).unwrap_err();
        assert!(matches!(error, So3Error::InvalidKey));
    }
}
