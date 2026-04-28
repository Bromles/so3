use sqlx::Error as SqlxError;
use crate::domain::error::So3Error;

pub mod applied_command;
pub mod blob;
pub mod metadata;
pub mod registry;

impl From<SqlxError> for So3Error {
    fn from(value: SqlxError) -> Self {
        Self::Storage(value.to_string())
    }
}
