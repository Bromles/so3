use crate::domain::error::So3Error;
use tonic::Status;

impl From<So3Error> for Status {
    fn from(error: So3Error) -> Self {
        match error {
            So3Error::InvalidKey | So3Error::InvalidVersion(_) | So3Error::InvalidRequest(_) => {
                Status::invalid_argument(error.to_string())
            }
            So3Error::NotFound(_) => Status::not_found(error.to_string()),
            So3Error::CasMismatch { .. } => Status::aborted(error.to_string()),
            So3Error::PeerUnavailable(_) => Status::unavailable(error.to_string()),
            So3Error::Storage(_) | So3Error::Io(_) | So3Error::Serialization(_) => {
                Status::internal(error.to_string())
            }
        }
    }
}
