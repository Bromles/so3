use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};
use crate::repository::node_identity::NodeIdentityRepository;
use crate::use_case::node_identity::interface::NodeIdentityUseCase;

pub struct NodeIdentityUseCaseImpl<R> {
    repository: Arc<R>,
}

impl<R: NodeIdentityRepository> NodeIdentityUseCaseImpl<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl<R: NodeIdentityRepository> NodeIdentityUseCase for NodeIdentityUseCaseImpl<R> {
    async fn ensure(&self, configured: Option<Uuid>) -> So3Result<Uuid> {
        let stored = self.repository.load().await?;
        match (configured, stored) {
            (None, Some(stored)) => Ok(stored),
            (None, None) => {
                let id = Uuid::new_v4();
                self.repository.store(id).await?;
                Ok(id)
            }
            (Some(id), None) => {
                self.repository.store(id).await?;
                Ok(id)
            }
            (Some(id), Some(stored)) if id == stored => Ok(id),
            (Some(id), Some(stored)) => Err(So3Error::InvalidRequest(format!(
                "configured node_id {id} does not match stored identity {stored}; \
                 remove the stored identity file to change node identity"
            ))),
        }
    }
}
