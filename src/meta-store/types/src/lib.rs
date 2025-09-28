use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetaStoreError {
    #[error("not found")]
    NotFound,
}

pub type MetaStoreResult<T> = Result<T, MetaStoreError>;
