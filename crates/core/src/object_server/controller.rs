use std::time::{Duration, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::consensus::state_machine::ObjectCommandExecutor;
use crate::domain::command::CasResult;
use crate::domain::error::So3Error;
use crate::domain::object::StoredObject;
use crate::domain::object_key::ObjectKey;
use crate::domain::object_version::ObjectVersion;
use crate::object_server::service::ObjectService;
use crate::repository::blob::BlobRepository;

#[derive(Clone)]
pub struct ObjectApiState<E: ObjectCommandExecutor, B: BlobRepository> {
    pub service: ObjectService<E, B>,
    pub request_timeout: Duration,
}

pub fn object_controller<E, B>(state: ObjectApiState<E, B>) -> Router
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let request_timeout = state.request_timeout;

    Router::new()
        .route(
            S3_OBJECT_ROUTE_PATH,
            get(handle_s3_get)
                .head(handle_s3_head)
                .put(handle_s3_put)
                .delete(handle_s3_delete),
        )
        .with_state(state)
        .layer((
            TraceLayer::new_for_http(),
            TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout),
        ))
}

async fn handle_s3_get<E, B>(
    State(state): State<ObjectApiState<E, B>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let object = load_object(&state, s3_object_key(&bucket, &key)?).await?;
    let metadata = ObjectMetadataResponse::from(&object);
    let mut response = object.value.into_response();
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_head<E, B>(
    State(state): State<ObjectApiState<E, B>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let object = load_object(&state, s3_object_key(&bucket, &key)?).await?;
    let mut response = StatusCode::OK.into_response();
    let metadata = ObjectMetadataResponse::from(&object);
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_put<E, B>(
    State(state): State<ObjectApiState<E, B>>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(s3_object_key(&bucket, &key)?)?;
    let record = state.service.write(key, body.to_vec()).await?;
    let metadata = ObjectMetadataResponse::from(record);
    let mut response = StatusCode::OK.into_response();
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_delete<E, B>(
    State(state): State<ObjectApiState<E, B>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(s3_object_key(&bucket, &key)?)?;
    state.service.delete(key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn handle_put<E, B>(
    State(state): State<ObjectApiState<E, B>>,
    Path(key): Path<String>,
    Query(query): Query<WriteQuery>,
    body: Bytes,
) -> Result<Json<ObjectMetadataResponse>, ApiError>
where
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
    B: BlobRepository + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(key)?;
    match query.expected_version {
        None => {
            let record = state.service.write(key, body.to_vec()).await?;
            Ok(Json(ObjectMetadataResponse::from(record)))
        }
        Some(expected_version) => {
            let expected = ObjectVersion::try_from(expected_version)?;
            match state
                .service
                .cas(key.clone(), expected, body.to_vec())
                .await?
            {
                CasResult::Applied(record) => Ok(Json(ObjectMetadataResponse::from(record))),
                CasResult::NotFound => Err(ApiError::from(So3Error::not_found(&key))),
                CasResult::Mismatch { current_version } => Err(ApiError::from(
                    So3Error::cas_mismatch(&key, expected, current_version),
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

async fn load_object<E, B>(
    state: &ObjectApiState<E, B>,
    key: String,
) -> Result<StoredObject, ApiError>
where
    B: BlobRepository + Clone + Send + Sync + 'static,
    E: ObjectCommandExecutor + Clone + Send + Sync + 'static,
{
    let key = ObjectKey::new(key)?;
    state
        .service
        .read(key.clone())
        .await?
        .ok_or_else(|| ApiError::from(So3Error::not_found(&key)))
}

fn attach_metadata_headers(
    headers: &mut axum::http::HeaderMap,
    metadata: &ObjectMetadataResponse,
) -> Result<(), ApiError> {
    insert_str_header(headers, VERSION_HEADER, &metadata.version.to_string())?;
    attach_common_object_headers(headers, metadata)
}

fn attach_s3_metadata_headers(
    headers: &mut axum::http::HeaderMap,
    metadata: &ObjectMetadataResponse,
) -> Result<(), ApiError> {
    insert_str_header(headers, S3_VERSION_ID_HEADER, &metadata.version.to_string())?;
    insert_str_header(
        headers,
        S3_OBJECT_SIZE_HEADER,
        &metadata.content_length.to_string(),
    )?;
    headers.insert(
        S3_STORAGE_CLASS_HEADER,
        HeaderValue::from_static("STANDARD"),
    );
    attach_common_object_headers(headers, metadata)
}

fn attach_common_object_headers(
    headers: &mut axum::http::HeaderMap,
    metadata: &ObjectMetadataResponse,
) -> Result<(), ApiError> {
    insert_str_header(headers, ETAG_HEADER, &quoted_etag(&metadata.checksum))?;
    insert_str_header(
        headers,
        CONTENT_LENGTH.as_str(),
        &metadata.content_length.to_string(),
    )?;
    insert_str_header(
        headers,
        LAST_MODIFIED_HEADER,
        &http_last_modified(metadata.last_modified_unix_millis)?,
    )
}

fn insert_str_header(
    headers: &mut axum::http::HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), ApiError> {
    headers.insert(
        name,
        HeaderValue::from_str(value)
            .map_err(|error| ApiError::from(So3Error::InvalidRequest(error.to_string())))?,
    );
    Ok(())
}

fn http_last_modified(unix_millis: i64) -> Result<String, ApiError> {
    let unix_millis = u64::try_from(unix_millis).map_err(|_| {
        ApiError::from(So3Error::InvalidRequest(format!(
            "last_modified_unix_millis cannot be negative: {unix_millis}"
        )))
    })?;
    let system_time = UNIX_EPOCH
        .checked_add(Duration::from_millis(unix_millis))
        .ok_or_else(|| {
            ApiError::from(So3Error::InvalidRequest(format!(
                "last_modified_unix_millis exceeds supported HTTP date range: {unix_millis}"
            )))
        })?;

    Ok(httpdate::fmt_http_date(system_time))
}

fn quoted_etag(checksum: &str) -> String {
    if checksum.starts_with('"') && checksum.ends_with('"') {
        checksum.to_owned()
    } else {
        format!("\"{checksum}\"")
    }
}

fn s3_object_key(bucket: &str, key: &str) -> Result<String, ApiError> {
    let bucket = bucket.trim();
    if bucket.is_empty() || key.trim().is_empty() {
        return Err(ApiError::from(So3Error::InvalidKey));
    }

    Ok(format!("{bucket}/{key}"))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    use super::{object_controller, ObjectApiState};
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::object_server::api::{
        ObjectMetadataResponse, LAST_MODIFIED_HEADER, S3_OBJECT_SIZE_HEADER,
        S3_STORAGE_CLASS_HEADER, VERSION_HEADER,
    };
    use crate::object_server::service::ObjectService;
    use crate::repository::registry::SqliteFsPersistentObjectRepository;

    const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
    const TEST_MAX_RESPONSE_BYTES: usize = usize::MAX;
    const OBJECT_PATH: &str = "/objects/alpha";
    const OBJECT_METADATA_PATH: &str = "/objects/alpha/metadata";
    const MISSING_OBJECT_PATH: &str = "/objects/missing";
    const S3_OBJECT_PATH: &str = "/bench/alpha";
    const S3_NESTED_OBJECT_PATH: &str = "/bench/path/to/object";
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
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
            .await
            .unwrap();
        let blob_repository = repository.blob_repository().clone();
        let state_machine = LocalStateMachine::new(repository);
        let service = ObjectService::new(state_machine, blob_repository);

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
        let write: ObjectMetadataResponse = serde_json::from_slice(&write_body).unwrap();
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
    async fn head_returns_object_headers_without_body() {
        let (app, _temp_dir) = test_app().await;

        let _ = app
            .clone()
            .oneshot(
                Request::put(OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(Request::head(OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(VERSION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            VERSION_ONE_HEADER
        );
        let body = to_bytes(response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn metadata_endpoint_returns_serialized_object_metadata() {
        let (app, _temp_dir) = test_app().await;

        let _ = app
            .clone()
            .oneshot(
                Request::put(OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::get(OBJECT_METADATA_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        let metadata: ObjectMetadataResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(metadata.key, ALPHA_KEY);
        assert_eq!(metadata.version, FIRST_VERSION);
        assert_eq!(metadata.content_length, HELLO_VALUE.len() as u64);
        assert!(metadata.last_modified_unix_millis > 0);
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
        let initial: ObjectMetadataResponse = serde_json::from_slice(&initial_body).unwrap();

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
        let cas: ObjectMetadataResponse = serde_json::from_slice(&cas_body).unwrap();
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

    #[tokio::test]
    async fn s3_put_then_get_returns_object_headers_and_body() {
        let (app, _temp_dir) = test_app().await;

        let put_response = app
            .clone()
            .oneshot(
                Request::put(S3_OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(put_response.status(), StatusCode::OK);
        assert_eq!(
            put_response
                .headers()
                .get("x-amz-version-id")
                .unwrap()
                .to_str()
                .unwrap(),
            VERSION_ONE_HEADER
        );
        assert!(
            put_response
                .headers()
                .get("etag")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with('"')
        );
        assert_eq!(
            put_response
                .headers()
                .get(S3_OBJECT_SIZE_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            HELLO_VALUE.len().to_string()
        );

        let get_response = app
            .oneshot(Request::get(S3_OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(get_response.status(), StatusCode::OK);
        assert_eq!(
            get_response
                .headers()
                .get("x-amz-version-id")
                .unwrap()
                .to_str()
                .unwrap(),
            VERSION_ONE_HEADER
        );
        assert_eq!(
            get_response
                .headers()
                .get("etag")
                .unwrap()
                .to_str()
                .unwrap(),
            put_response
                .headers()
                .get("etag")
                .unwrap()
                .to_str()
                .unwrap()
        );
        let last_modified = get_response
            .headers()
            .get(LAST_MODIFIED_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            last_modified,
            put_response
                .headers()
                .get(LAST_MODIFIED_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
        );
        assert!(httpdate::parse_http_date(last_modified).unwrap() > UNIX_EPOCH);
        assert_eq!(
            get_response
                .headers()
                .get(S3_STORAGE_CLASS_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "STANDARD"
        );
        let get_body = to_bytes(get_response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(&get_body[..], HELLO_VALUE.as_bytes());
    }

    #[tokio::test]
    async fn s3_delete_returns_no_content_for_existing_object() {
        let (app, _temp_dir) = test_app().await;

        let _ = app
            .clone()
            .oneshot(
                Request::put(S3_OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        let delete_response = app
            .clone()
            .oneshot(Request::delete(S3_OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        let get_response = app
            .oneshot(Request::get(S3_OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn s3_delete_returns_no_content_for_missing_object() {
        let (app, _temp_dir) = test_app().await;

        let response = app
            .oneshot(Request::delete(S3_OBJECT_PATH).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn s3_head_supports_nested_object_keys() {
        let (app, _temp_dir) = test_app().await;

        let _ = app
            .clone()
            .oneshot(
                Request::put(S3_NESTED_OBJECT_PATH)
                    .body(Body::from(HELLO_VALUE))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::head(S3_NESTED_OBJECT_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            HELLO_VALUE.len().to_string()
        );
        let body = to_bytes(response.into_body(), TEST_MAX_RESPONSE_BYTES)
            .await
            .unwrap();
        assert!(body.is_empty());
    }
}
