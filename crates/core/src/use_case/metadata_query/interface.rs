use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;

#[async_trait]
pub trait MetadataQueryUseCase: Send + Sync + 'static {
    async fn query(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>>;
}
