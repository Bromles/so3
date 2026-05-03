use crate::domain::error::So3Error;
use tonic::transport::Error as TonicError;

impl From<TonicError> for So3Error {
    fn from(value: TonicError) -> Self {
        Self::Io(value.to_string())
    }
}
