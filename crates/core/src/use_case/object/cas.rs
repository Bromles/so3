use crate::client::interface::{BlobPeerClient, MetadataQueryClient};
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

impl<CCS, CJR, OMR, BR, BC, MQC> ObjectUseCaseImpl<CCS, CJR, OMR, BR, BC, MQC>
where
    CCS: ConsensusCoordinatorService,
    CJR: ConsensusJournalRepository,
    OMR: ObjectMetadataRepository,
    BR: BlobRepository,
    BC: BlobPeerClient,
    MQC: MetadataQueryClient,
{
    pub async fn cas_internal(
        &self,
        key: ObjectKey,
        expected_version: ObjectVersion,
        body: BlobStream,
    ) -> So3Result<CasResult> {
        use crate::domain::blob::id::BlobId;

        let temp_blob_id = BlobId::new();

        let (sha256, size) = match self.stream_to_local(&temp_blob_id, body).await {
            Ok(r) => r,
            Err(e) => {
                let _ = self.blob_repository.abort(&temp_blob_id).await;
                return Err(e);
            }
        };

        if let Some(existing) = self.object_metadata_repository.load(&key).await?
            && existing.version == expected_version.next()
            && existing.sha256 == sha256
        {
            let _ = self.blob_repository.abort(&temp_blob_id).await;
            return Ok(CasResult::Updated(existing));
        }

        let blob_id = BlobId::from_sha256(&sha256);
        self.blob_repository
            .commit_as(&temp_blob_id, &blob_id)
            .await?;

        let peers: Vec<_> = self.blob_client_map.values().cloned().collect();
        let n = 1 + peers.len();
        let quorum = n / 2 + 1;
        let peers_needed = quorum - 1;

        let mut push_set = tokio::task::JoinSet::new();
        for client in &peers {
            let client = std::sync::Arc::clone(client);
            let blob_id = blob_id.clone();
            let repo = std::sync::Arc::clone(&self.blob_repository);
            push_set.spawn(async move {
                let Ok(reader) = repo.open_reader(&blob_id).await else {
                    return false;
                };
                client.push(blob_id, size, sha256, reader).await.is_ok()
            });
        }
        let mut ok = 0usize;
        while let Some(res) = push_set.join_next().await {
            if res.is_ok_and(|success| success) {
                ok += 1;
                if ok >= peers_needed {
                    break;
                }
            }
        }
        push_set.abort_all();
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
