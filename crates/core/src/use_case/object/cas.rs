use crate::client::interface::BlobPeerClient;
use crate::domain::blob::stream::BlobStream;
use crate::domain::command::{CasResult, CommandResult, ObjectCommand};
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::version::ObjectVersion;
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
    pub async fn cas_internal(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        body: BlobStream,
    ) -> So3Result<CasResult> {
        use crate::domain::blob::id::BlobId;

        let blob_id = BlobId::new();

        let (sha256, size) = match self.stream_to_local(&blob_id, body).await {
            Ok(r) => r,
            Err(e) => {
                let _ = self.blob_repository.abort(&blob_id).await;
                return Err(e);
            }
        };

        let peers: Vec<_> = self.blob_client_map.values().cloned().collect();
        let n = 1 + peers.len();
        let quorum = n / 2 + 1;
        let peers_needed = quorum - 1;

        let mut ok = 0usize;
        for client in &peers {
            if let Ok(reader) = self.blob_repository.open_reader(&blob_id).await {
                if client
                    .push(blob_id.clone(), size, sha256.clone(), reader)
                    .await
                    .is_ok()
                {
                    ok += 1;
                }
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
            .coordinate(ObjectCommand::Cas {
                key,
                expected_version,
                blob_id,
                sha256,
                size,
            })
            .await?;

        match result {
            CommandResult::Cas(r) => Ok(r),
            _ => Err(So3Error::Storage(
                "unexpected result from CAS coordinate".into(),
            )),
        }
    }
}
