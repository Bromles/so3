use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::{Sha256Digest, Sha256Hasher};
use crate::domain::blob::id::BlobId;
use crate::domain::blob::stream::BlobStream;
use crate::domain::command::{CasResult, CommandResult};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::node::NodeId;
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::{ObjectMetadata, StoredObject};
use crate::domain::object::version::ObjectVersion;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::ObjectUseCase;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

pub struct ObjectUseCaseImpl<
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
> {
    pub consensus_coordinator_service: Arc<CCS>,
    pub consensus_journal_repository: Arc<CJR>,
    pub object_metadata_repository: Arc<OMR>,
    pub blob_repository: Arc<BR>,
    pub blob_client_map: HashMap<NodeId, Arc<BC>>,
}

impl<CCS, CJR, OMR, BR, BC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub fn new(
        consensus_coordinator_service: Arc<CCS>,
        consensus_journal_repository: Arc<CJR>,
        object_metadata_repository: Arc<OMR>,
        blob_repository: Arc<BR>,
        blob_client_map: HashMap<NodeId, Arc<BC>>,
    ) -> Self {
        Self {
            consensus_coordinator_service,
            consensus_journal_repository,
            object_metadata_repository,
            blob_repository,
            blob_client_map,
        }
    }

    pub fn unexpected_result<T>(operation: &str, result: &CommandResult) -> So3Result<T> {
        Err(So3Error::InvalidRequest(format!(
            "unexpected state machine result for {operation}: {result:?}"
        )))
    }

    pub(crate) async fn fetch_blob_from_any_peer(&self, blob_id: &BlobId) -> So3Result<()> {
        for client in self.blob_client_map.values() {
            if let Ok(mut stream) = client.fetch(blob_id).await {
                let mut failed = false;
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(c) => {
                            if self.blob_repository.append_chunk(blob_id, c).await.is_err() {
                                failed = true;
                                break;
                            }
                        }
                        Err(_) => {
                            failed = true;
                            break;
                        }
                    }
                }
                if failed {
                    let _ = self.blob_repository.abort(blob_id).await;
                    continue;
                }
                return self.blob_repository.commit(blob_id).await;
            }
        }
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available on any peer"
        )))
    }

    pub(crate) async fn stream_to_local(
        &self,
        blob_id: &BlobId,
        mut body: BlobStream,
    ) -> So3Result<(Sha256Digest, u64)> {
        let mut hasher = Sha256Hasher::new();
        let mut size = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if !chunk.is_empty() {
                hasher.update(&chunk);
                size += chunk.len() as u64;
                self.blob_repository.append_chunk(blob_id, chunk).await?;
            }
        }
        self.blob_repository.commit(blob_id).await?;
        Ok((hasher.finalize(), size))
    }
}

#[async_trait]
impl<CCS, CJR, OMR, BR, BC> ObjectUseCase for ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    async fn head(&self, key: &ObjectKey) -> So3Result<Option<ObjectMetadata>> {
        self.head_internal(key).await
    }

    async fn read(&self, key: &ObjectKey) -> So3Result<Option<StoredObject>> {
        self.read_internal(key).await
    }

    async fn write(&self, key: ObjectKey, body: BlobStream) -> So3Result<ObjectMetadata> {
        self.write_internal(key, body).await
    }

    async fn delete(&self, key: &ObjectKey) -> So3Result<()> {
        self.delete_internal(key).await
    }

    async fn cas(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        body: BlobStream,
    ) -> So3Result<CasResult> {
        self.cas_internal(key, expected_version, body).await
    }
}
