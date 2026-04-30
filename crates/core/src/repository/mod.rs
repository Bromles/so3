use crate::domain::error::So3Error;
use sqlx::Error as SqlxError;

pub mod applied_command;
pub mod blob;
pub mod metadata;
pub mod registry;
pub mod consensus_journal;

impl From<SqlxError> for So3Error {
    fn from(value: SqlxError) -> Self {
        Self::Storage(value.to_string())
    }
}
