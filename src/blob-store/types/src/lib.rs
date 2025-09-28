use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlobStoreError {
    #[error("not found")]
    NotFound,
}

pub type BlobStoreResult<T> = Result<T, BlobStoreError>;
