use crate::api::s3::axum::error::ApiError;
use crate::domain::blob::stream::BlobStream;
use crate::domain::error::So3Error;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::use_case::object::ObjectUseCase;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio_stream::StreamExt;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub const S3_OBJECT_ROUTE_PATH: &str = "/{bucket}/{*key}";
pub const S3_VERSION_ID_HEADER: &str = "x-amz-version-id";
pub const S3_OBJECT_SIZE_HEADER: &str = "x-amz-object-size";
pub const S3_STORAGE_CLASS_HEADER: &str = "x-amz-repository-class";
pub const ETAG_HEADER: &str = "etag";
pub const LAST_MODIFIED_HEADER: &str = "last-modified";
pub const DEFAULT_ERROR_LABEL: &str = "error";

pub struct ObjectApiController<O: ObjectUseCase> {
    pub request_timeout: Duration,
    pub object_use_case: Arc<O>,
}

impl<O: ObjectUseCase> ObjectApiController<O> {
    pub fn new(request_timeout: Duration, object_use_case: Arc<O>) -> Self {
        Self {
            request_timeout,
            object_use_case,
        }
    }

    pub fn router(self: Arc<Self>) -> Router {
        let request_timeout = self.request_timeout;

        Router::new()
            .route(
                S3_OBJECT_ROUTE_PATH,
                get(Self::handle_s3_get)
                    .head(Self::handle_s3_head)
                    .put(Self::handle_s3_put)
                    .delete(Self::handle_s3_delete),
            )
            .with_state(self)
            .layer((
                TraceLayer::new_for_http(),
                TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout),
            ))
    }

    async fn handle_s3_get(
        State(state): State<Arc<Self>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> Result<Response, ApiError> {
        let object_key = s3_object_key(&bucket, &key)?;

        let stored_object = state
            .object_use_case
            .read(&object_key)
            .await?
            .ok_or_else(|| So3Error::not_found(&object_key))?;

        let body = Body::from_stream(stored_object.blob);
        let mut response = Response::new(body);

        attach_s3_metadata_headers(response.headers_mut(), &stored_object.metadata)?;

        Ok(response)
    }

    async fn handle_s3_head(
        State(state): State<Arc<Self>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> Result<Response, ApiError> {
        let object_key = s3_object_key(&bucket, &key)?;

        let object_metadata = state
            .object_use_case
            .head(&object_key)
            .await?
            .ok_or_else(|| So3Error::not_found(&object_key))?;

        let mut response = StatusCode::OK.into_response();

        attach_s3_metadata_headers(response.headers_mut(), &object_metadata)?;

        Ok(response)
    }

    async fn handle_s3_put(
        State(state): State<Arc<Self>>,
        Path((bucket, key)): Path<(String, String)>,
        body: Body,
    ) -> Result<Response, ApiError> {
        let object_key = s3_object_key(&bucket, &key)?;

        let stream = body
            .into_data_stream()
            .map(|r| r.map_err(|e| So3Error::Io(e.to_string())));
        let blob_stream = BlobStream::new(stream);

        let metadata = state.object_use_case.write(object_key, blob_stream).await?;

        let mut response = StatusCode::OK.into_response();

        attach_s3_metadata_headers(response.headers_mut(), &metadata)?;

        Ok(response)
    }

    async fn handle_s3_delete(
        State(state): State<Arc<Self>>,
        Path((bucket, key)): Path<(String, String)>,
    ) -> Result<Response, ApiError> {
        let object_key = s3_object_key(&bucket, &key)?;

        state.object_use_case.delete(&object_key).await?;

        Ok(StatusCode::NO_CONTENT.into_response())
    }
}

fn attach_s3_metadata_headers(
    headers: &mut axum::http::HeaderMap,
    metadata: &ObjectMetadata,
) -> Result<(), ApiError> {
    insert_str_header(
        headers,
        S3_VERSION_ID_HEADER,
        &metadata.version.get().to_string(),
    )?;
    insert_str_header(headers, S3_OBJECT_SIZE_HEADER, &metadata.size.to_string())?;
    headers.insert(
        S3_STORAGE_CLASS_HEADER,
        HeaderValue::from_static("STANDARD"),
    );

    attach_common_object_headers(headers, metadata)
}

fn attach_common_object_headers(
    headers: &mut axum::http::HeaderMap,
    metadata: &ObjectMetadata,
) -> Result<(), ApiError> {
    insert_str_header(
        headers,
        ETAG_HEADER,
        &quoted_etag(&metadata.sha256.to_hex()),
    )?;
    insert_str_header(headers, CONTENT_LENGTH.as_str(), &metadata.size.to_string())?;
    insert_str_header(
        headers,
        LAST_MODIFIED_HEADER,
        &http_last_modified(metadata.last_modified_ms)?,
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

fn http_last_modified(unix_millis: u64) -> Result<String, ApiError> {
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

fn s3_object_key(bucket: &str, key: &str) -> Result<ObjectKey, ApiError> {
    let bucket = bucket.trim();
    if bucket.is_empty() || key.trim().is_empty() {
        return Err(ApiError::from(So3Error::InvalidKey));
    }

    Ok(ObjectKey::new(format!("{bucket}/{key}"))?)
}
