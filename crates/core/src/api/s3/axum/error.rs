use crate::api::s3::axum::controller::DEFAULT_ERROR_LABEL;
use crate::domain::error::So3Error;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

pub struct ApiError(So3Error);

impl From<So3Error> for ApiError {
    fn from(value: So3Error) -> Self {
        Self(value)
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self.0 {
            error @ (So3Error::InvalidKey
            | So3Error::InvalidVersion(_)
            | So3Error::InvalidRequest(_)) => (StatusCode::BAD_REQUEST, error),
            error @ So3Error::NotFound(_) => (StatusCode::NOT_FOUND, error),
            error @ So3Error::CasMismatch { .. } => (StatusCode::CONFLICT, error),
            error => {
                tracing::warn!(error = %error, "request failed with internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, error)
            }
        };

        let body = Json(ErrorResponse {
            error: status
                .canonical_reason()
                .unwrap_or(DEFAULT_ERROR_LABEL)
                .to_lowercase(),
            detail: error.to_string(),
        });

        (status, body).into_response()
    }
}
