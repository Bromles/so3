use async_trait::async_trait;

use crate::domain::error::So3Result;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::use_case::metadata_query::MetadataQueryUseCase;
use std::sync::Arc;

pub struct MetadataQueryUseCaseImpl<OMR: ObjectMetadataRepository> {
    metadata_repository: Arc<OMR>,
}

impl<OMR: ObjectMetadataRepository> MetadataQueryUseCaseImpl<OMR> {
    pub fn new(metadata_repository: Arc<OMR>) -> Self {
        Self {
            metadata_repository,
        }
    }
}

#[async_trait]
impl<OMR: ObjectMetadataRepository> MetadataQueryUseCase for MetadataQueryUseCaseImpl<OMR> {
    async fn query(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        self.metadata_repository.load(key).await
    }
}
