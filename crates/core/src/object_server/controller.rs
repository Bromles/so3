use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::consensus::state_machine::LocalStateMachine;
use crate::domain::error::So3Error;
use crate::domain::types::{
    CasCommand, CasResult, ObjectCommand, ObjectKey, ObjectResult, ReadCommand, WriteCommand,
};

#[derive(Clone)]
pub struct ObjectApiState {
    pub state_machine: Arc<LocalStateMachine>,
    pub request_timeout: Duration,
}

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    pub expected_version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    pub key: String,
    pub version: i64,
    pub checksum: String,
    pub content_length: u64,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: String,
}

pub fn object_controller(state: ObjectApiState) -> Router {
    let request_timeout = state.request_timeout;

    Router::new()
        .route("/objects/{key}", get(handle_get).put(handle_put))
        .with_state(state)
        .layer((
            TraceLayer::new_for_http(),
            TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout),
        ))
}

async fn handle_get(
    State(state): State<ObjectApiState>,
    Path(key): Path<String>,
) -> Result<Response, ApiError> {
    let key = ObjectKey::new(key)?;
    let result = state
        .state_machine
        .execute(ObjectCommand::Read(ReadCommand { key: key.clone() }))
        .await?;

    match result {
        ObjectResult::Read(read) => {
            let Some(object) = read.object else {
                return Err(ApiError::from(So3Error::not_found(&key)));
            };

            let mut response = object.value.into_response();
            response.headers_mut().insert(
                "x-so3-version",
                HeaderValue::from_str(&object.record.version.get().to_string())
                    .map_err(|error| ApiError::from(So3Error::InvalidRequest(error.to_string())))?,
            );
            response.headers_mut().insert(
                "etag",
                HeaderValue::from_str(&object.record.checksum)
                    .map_err(|error| ApiError::from(So3Error::InvalidRequest(error.to_string())))?,
            );

            Ok(response)
        }
        _ => Err(ApiError::from(So3Error::InvalidRequest(
            "unexpected read result".to_owned(),
        ))),
    }
}

async fn handle_put(
    State(state): State<ObjectApiState>,
    Path(key): Path<String>,
    Query(query): Query<WriteQuery>,
    body: Bytes,
) -> Result<Json<WriteResponse>, ApiError> {
    let key = ObjectKey::new(key)?;
    let command = match query.expected_version {
        Some(version) => ObjectCommand::Cas(CasCommand {
            key: key.clone(),
            expected_version: version.try_into()?,
            value: body.to_vec(),
        }),
        None => ObjectCommand::Write(WriteCommand {
            key: key.clone(),
            value: body.to_vec(),
        }),
    };

    match state.state_machine.execute(command).await? {
        ObjectResult::Write(result) => Ok(Json(WriteResponse {
            key: result.object.record.key.as_str().to_owned(),
            version: result.object.record.version.get(),
            checksum: result.object.record.checksum,
            content_length: result.object.record.content_length,
        })),
        ObjectResult::Cas(CasResult::Applied(object)) => Ok(Json(WriteResponse {
            key: object.record.key.as_str().to_owned(),
            version: object.record.version.get(),
            checksum: object.record.checksum,
            content_length: object.record.content_length,
        })),
        ObjectResult::Cas(CasResult::NotFound) => Err(ApiError::from(So3Error::not_found(&key))),
        ObjectResult::Cas(CasResult::Mismatch { current_version }) => {
            Err(ApiError::from(So3Error::cas_mismatch(
                &key,
                query.expected_version.unwrap().try_into()?,
                current_version,
            )))
        }
        _ => Err(ApiError::from(So3Error::InvalidRequest(
            "unexpected write result".to_owned(),
        ))),
    }
}

pub struct ApiError(So3Error);

impl From<So3Error> for ApiError {
    fn from(value: So3Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error) = match self.0 {
            error @ So3Error::InvalidKey
            | error @ So3Error::InvalidVersion(_)
            | error @ So3Error::InvalidRequest(_) => (StatusCode::BAD_REQUEST, error),
            error @ So3Error::NotFound(_) => (StatusCode::NOT_FOUND, error),
            error @ So3Error::CasMismatch { .. } => (StatusCode::CONFLICT, error),
            error => (StatusCode::INTERNAL_SERVER_ERROR, error),
        };

        let body = Json(ErrorResponse {
            error: status.canonical_reason().unwrap_or("error").to_lowercase(),
            detail: error.to_string(),
        });

        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    use super::{ObjectApiState, WriteResponse, object_controller};
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::storage::sqlite_fs::PersistentObjectStore;

    #[tokio::test]
    async fn put_then_get_returns_object_and_version_headers() {
        let temp_dir = TempDir::new().unwrap();
        let state_machine = Arc::new(LocalStateMachine::new(Arc::new(
            PersistentObjectStore::open(temp_dir.path()).await.unwrap(),
        )));
        let app = object_controller(ObjectApiState {
            state_machine,
            request_timeout: Duration::from_secs(10),
        });

        let put_response = app
            .clone()
            .oneshot(
                Request::put("/objects/alpha")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(put_response.status(), StatusCode::OK);
        let write_body = to_bytes(put_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let write: WriteResponse = serde_json::from_slice(&write_body).unwrap();
        assert_eq!(write.key, "alpha");
        assert_eq!(write.version, 1);

        let get_response = app
            .oneshot(Request::get("/objects/alpha").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(
            get_response
                .headers()
                .get("x-so3-version")
                .unwrap()
                .to_str()
                .unwrap(),
            "1"
        );
        let get_body = to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&get_body[..], b"hello");
    }

    #[tokio::test]
    async fn get_missing_object_returns_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let state_machine = Arc::new(LocalStateMachine::new(Arc::new(
            PersistentObjectStore::open(temp_dir.path()).await.unwrap(),
        )));
        let app = object_controller(ObjectApiState {
            state_machine,
            request_timeout: Duration::from_secs(10),
        });

        let response = app
            .oneshot(
                Request::get("/objects/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cas_mismatch_returns_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let state_machine = Arc::new(LocalStateMachine::new(Arc::new(
            PersistentObjectStore::open(temp_dir.path()).await.unwrap(),
        )));
        let app = object_controller(ObjectApiState {
            state_machine,
            request_timeout: Duration::from_secs(10),
        });

        let _ = app
            .clone()
            .oneshot(
                Request::put("/objects/alpha")
                    .body(Body::from("first"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::put("/objects/alpha?expected_version=9")
                    .body(Body::from("second"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
