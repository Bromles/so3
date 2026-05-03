use tonic::transport::Error as TonicError;
use crate::domain::error::So3Error;

impl From<TonicError> for So3Error {
    fn from(value: TonicError) -> Self {
        Self::Io(value.to_string())
    }
}
