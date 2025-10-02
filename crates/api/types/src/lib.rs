use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("not found")]
    NotFound,
}

pub type ApiResult<T> = Result<T, ApiError>;

pub type ByteStream = Pin<Box<dyn Stream<Item = ApiResult<Bytes>> + Send>>;
