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

use crate::domain::error::So3Error;
use crate::domain::{CasResult, ObjectKey, StoredObject};
use crate::object_server::service::ObjectService;
use crate::storage::object::repository::ObjectRepository;

const OBJECT_ROUTE_PATH: &str = "/objects/{key}";
const VERSION_HEADER: &str = "x-so3-version";
const ETAG_HEADER: &str = "etag";
const DEFAULT_ERROR_LABEL: &str = "error";

#[derive(Clone)]
pub struct ObjectApiState<R: ObjectRepository> {
    pub service: ObjectService<R>,
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

impl WriteResponse {
    fn from_object(object: StoredObject) -> Self {
        Self {
            key: object.record.key.as_str().to_owned(),
            version: object.record.version.get(),
            checksum: object.record.checksum,
            content_length: object.record.content_length,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub detail: String,
}

pub fn object_controller<R>(state: ObjectApiState<R>) -> Router
where
    R: ObjectRepository + Clone + Send + Sync + 'static,
{
    let request_timeout = state.request_timeout;

    Router::new()
        .route(OBJECT_ROUTE_PATH, get(handle_get).put(handle_put))
        .with_state(state)
        .layer((
            TraceLayer::new_for_http(),
            TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout),
        ))
}

async fn handle_get<R>(
    State(state): State<ObjectApiState<R>>,
    Path(key): Path<String>,
) -> Result<Response, ApiError>
where
    R: ObjectRepository + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(key)?;
    let Some(object) = state.service.read(key.clone()).await? else {
        return Err(ApiError::from(So3Error::not_found(&key)));
    };

    let mut response = object.value.into_response();
    response.headers_mut().insert(
        VERSION_HEADER,
        HeaderValue::from_str(&object.record.version.get().to_string())
            .map_err(|error| ApiError::from(So3Error::InvalidRequest(error.to_string())))?,
    );
    response.headers_mut().insert(
        ETAG_HEADER,
        HeaderValue::from_str(&object.record.checksum)
            .map_err(|error| ApiError::from(So3Error::InvalidRequest(error.to_string())))?,
    );

    Ok(response)
}

async fn handle_put<R>(
    State(state): State<ObjectApiState<R>>,
    Path(key): Path<String>,
    Query(query): Query<WriteQuery>,
    body: Bytes,
) -> Result<Json<WriteResponse>, ApiError>
where
    R: ObjectRepository + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(key)?;
    match query.expected_version {
        None => {
            let object = state.service.write(key, body.to_vec()).await?;
            Ok(Json(WriteResponse::from_object(object)))
        }
        Some(expected_version) => {
            match state
                .service
                .cas(key.clone(), expected_version.try_into()?, body.to_vec())
                .await?
            {
                CasResult::Applied(object) => Ok(Json(WriteResponse::from_object(object))),
                CasResult::NotFound => Err(ApiError::from(So3Error::not_found(&key))),
                CasResult::Mismatch { current_version } => Err(ApiError::from(
                    So3Error::cas_mismatch(&key, expected_version.try_into()?, current_version),
                )),
            }
        }
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
            error @ (So3Error::InvalidKey
            | So3Error::InvalidVersion(_)
            | So3Error::InvalidRequest(_)) => (StatusCode::BAD_REQUEST, error),
            error @ So3Error::NotFound(_) => (StatusCode::NOT_FOUND, error),
            error @ So3Error::CasMismatch { .. } => (StatusCode::CONFLICT, error),
            error => (StatusCode::INTERNAL_SERVER_ERROR, error),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    use super::{ObjectApiState, VERSION_HEADER, WriteResponse, object_controller};
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::object_server::service::ObjectService;
    use crate::storage::object::persistent::SqliteFsObjectRepository;

    const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
    const TEST_MAX_RESPONSE_BYTES: usize = usize::MAX;
    const OBJECT_PATH: &str = "/objects/alpha";
    const MISSING_OBJECT_PATH: &str = "/objects/missing";
    const CAS_MISMATCH_PATH: &str = "/objects/alpha?expected_version=9";
    const INVALID_VERSION_PATH: &str = "/objects/alpha?expected_version=0";
    const FIRST_VALUE: &str = "first";
    const SECOND_VALUE: &str = "second";
    const HELLO_VALUE: &str = "hello";
    const ALPHA_KEY: &str = "alpha";
    const VERSION_ONE_HEADER: &str = "1";
    const VERSION_TWO_HEADER: &str = "2";
    const REQUEST_BODY_VALUE: &str = "value";
    const FIRST_VERSION: i64 = 1;
    const SECOND_VERSION: i64 = 2;
    const VERSION_INCREMENT: i64 = 1;

    async fn test_app() -> (axum::Router, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let state_machine = LocalStateMachine::new(
            SqliteFsObjectRepository::new(
                temp_dir.path().join("metadata"),
                temp_dir.path().join("blobs"),
            )
            .await
            .unwrap(),
        );
        let service = ObjectService::new(state_machine);

        (
            object_controller(ObjectApiState {
                service,
                request_timeout: TEST_REQUEST_TIMEOUT,
            }),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn put_then_get_returns_object_and_version_headers() {
        let (app, _temp_dir) = test_app().await;

        let put_response = app
            .clone()
            .oneshot(
                Request::put(OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(put_response.status(), StatusCode::OK);
        let write_body = to_bytes(put_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        let write: WriteResponse = serde_json::from_slice(&write_body).unwrap();
        assert_eq!(write.key, ALPHA_KEY);
        assert_eq!(write.version, FIRST_VERSION);

        let get_response = app
            .oneshot(Request::get(OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(
            get_response
                .headers()
                .get(VERSION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            VERSION_ONE_HEADER
        );
        let get_body = to_bytes(get_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(&get_body[..], HELLO_VALUE.as_bytes());
    }

    #[tokio::test]
    async fn get_missing_object_returns_not_found() {
        let (app, _temp_dir) = test_app().await;

        let response = app
            .oneshot(
                Request::get(MISSING_OBJECT_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cas_mismatch_returns_conflict() {
        let (app, _temp_dir) = test_app().await;

        let _ = app
            .clone()
            .oneshot(
                Request::put(OBJECT_PATH)
                    .body(Body::from(FIRST_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::put(CAS_MISMATCH_PATH)
                    .body(Body::from(SECOND_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn successful_cas_updates_value_and_increments_version() {
        let (app, _temp_dir) = test_app().await;

        let initial_response = app
            .clone()
            .oneshot(
                Request::put(OBJECT_PATH)
                    .body(Body::from(FIRST_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();
        let initial_body = to_bytes(initial_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        let initial: WriteResponse = serde_json::from_slice(&initial_body).unwrap();

        let cas_response = app
            .clone()
            .oneshot(
                Request::put(format!(
                    "/objects/alpha?expected_version={}",
                    initial.version
                ))
                .body(Body::from(SECOND_VALUE))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cas_response.status(), StatusCode::OK);
        let cas_body = to_bytes(cas_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        let cas: WriteResponse = serde_json::from_slice(&cas_body).unwrap();
        assert_eq!(cas.version, initial.version + VERSION_INCREMENT);

        let get_response = app
            .oneshot(Request::get(OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(
            get_response
                .headers()
                .get(VERSION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            VERSION_TWO_HEADER
        );
        let get_body = to_bytes(get_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(&get_body[..], SECOND_VALUE.as_bytes());
        assert_eq!(cas.version, SECOND_VERSION);
    }

    #[tokio::test]
    async fn invalid_expected_version_returns_bad_request() {
        let (app, _temp_dir) = test_app().await;

        let response = app
            .oneshot(
                Request::put(INVALID_VERSION_PATH)
                    .body(Body::from(REQUEST_BODY_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
