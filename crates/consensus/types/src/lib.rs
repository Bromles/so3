use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("unknown consensus error")]
    Unknown,
}

pub type ConsensusResult<T> = Result<T, ConsensusError>;
