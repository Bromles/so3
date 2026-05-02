use crate::domain::command::ObjectCommand;
use crate::domain::error::So3Result;
use async_trait::async_trait;

#[async_trait]
pub trait ConsensusService {
    async fn coordinate(&self, command: ObjectCommand) -> So3Result<()>;
}
