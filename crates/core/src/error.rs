use so3_api_core::ApiError;
use so3_blob_store_core::BlobStoreError;
use so3_consensus_core::ConsensusError;
use so3_meta_store_core::MetaStoreError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error(transparent)]
    BlobStore(#[from] BlobStoreError),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    MetaStore(#[from] MetaStoreError),
}

pub type AppResult<T> = Result<T, AppError>;
