use crate::client::interface::BlobPeerClient;
use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::blob::id::BlobId;
use crate::domain::blob::payload::BlobPayload;
use crate::domain::command::{CommandResult, ObjectCommand};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::repository::blob::BlobRepository;
use crate::repository::consensus_journal::ConsensusJournalRepository;
use crate::repository::metadata::ObjectMetadataRepository;
use crate::service::consensus_coordinator::ConsensusCoordinatorService;
use crate::use_case::object::use_case::ObjectUseCaseImpl;

impl<CCS, CJR, OMR, BR, BC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
{
    pub async fn write_internal(
        &self,
        key: ObjectKey,
        payload: BlobPayload,
    ) -> So3Result<ObjectMetadata> {
        let blob_id = BlobId::new();
        let sha256 = Sha256Digest::compute(payload.as_bytes());
        let size = payload.len() as u64;

        self.blob_repository.store(&blob_id, &payload).await?;

        let peers: Vec<_> = self.blob_client_map.values().cloned().collect();
        let n = 1 + peers.len();
        let quorum = n / 2 + 1;
        let peers_needed = quorum - 1;

        let handles: Vec<_> = peers
            .into_iter()
            .map(|client| {
                let id = blob_id.clone();
                let p = payload.clone();
                tokio::spawn(async move { client.push(id, &p).await })
            })
            .collect();

        let mut ok = 0usize;
        for handle in handles {
            if matches!(handle.await, Ok(Ok(()))) {
                ok += 1;
            }
        }
        if ok < peers_needed {
            return Err(So3Error::PeerUnavailable(format!(
                "blob push: only {}/{} peers reachable, need quorum of {quorum}",
                ok + 1,
                n,
            )));
        }

        let result = self
            .consensus_coordinator_service
            .coordinate(ObjectCommand::Write {
                key,
                blob_id,
                sha256,
                size,
            })
            .await?;

        match result {
            CommandResult::Write(r) => Ok(r.metadata),
            _ => Err(So3Error::Storage(
                "unexpected result from Write coordinate".into(),
            )),
        }
    }
}
