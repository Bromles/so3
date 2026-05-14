use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::error::So3Result;

#[async_trait]
pub trait NodeIdentityRepository: Send + Sync + 'static {
    async fn load(&self) -> So3Result<Option<Uuid>>;
    async fn store(&self, id: Uuid) -> So3Result<()>;
}
