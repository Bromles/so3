use crate::api::s3::dto::{
    ObjectMetadataResponse, ETAG_HEADER, LAST_MODIFIED_HEADER, S3_OBJECT_ROUTE_PATH,
    S3_OBJECT_SIZE_HEADER, S3_STORAGE_CLASS_HEADER, S3_VERSION_ID_HEADER,
};
use crate::api::s3::error::ApiError;
use crate::domain::error::So3Error;
use crate::domain::object_key::ObjectKey;
use crate::repository::blob::BlobRepository;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use std::time::{Duration, UNIX_EPOCH};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct ObjectApiState {
    pub request_timeout: Duration,
}

pub fn object_controller(state: ObjectApiState) -> Router {
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

async fn handle_s3_get(
    State(state): State<ObjectApiState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let object = load_object(&state, s3_object_key(&bucket, &key)?).await?;
    let metadata = ObjectMetadataResponse::from(&object);
    let mut response = object.value.into_response();
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_head(
    State(state): State<ObjectApiState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let object = load_object(&state, s3_object_key(&bucket, &key)?).await?;
    let mut response = StatusCode::OK.into_response();
    let metadata = ObjectMetadataResponse::from(&object);
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_put(
    State(state): State<ObjectApiState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let key = ObjectKey::new(s3_object_key(&bucket, &key)?)?;
    let record = state.service.write(key, body.to_vec()).await?;
    let metadata = ObjectMetadataResponse::from(record);
    let mut response = StatusCode::OK.into_response();
    attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

    Ok(response)
}

async fn handle_s3_delete(
    State(state): State<ObjectApiState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let key = ObjectKey::new(s3_object_key(&bucket, &key)?)?;
    state.service.delete(key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
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
