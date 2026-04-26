use std::path::Path;

use crate::consensus::journal::SqliteConsensusJournal;
use crate::domain::error::So3Result;
use crate::storage::blob::fs::FileSystemBlobRepository;
use crate::storage::metadata::sqlite::SqliteObjectMetadataRepository;
use crate::storage::object::persistent::PersistentObjectRepository;

pub type SqliteFsPersistentObjectRepository =
    PersistentObjectRepository<SqliteObjectMetadataRepository, FileSystemBlobRepository>;

pub struct PersistentStorage {
    pub metadata_repository: SqliteObjectMetadataRepository,
    pub object_repository: SqliteFsPersistentObjectRepository,
    pub consensus_journal: SqliteConsensusJournal,
}

impl PersistentStorage {
    /// # Errors
    ///
    /// Returns an error if any durable local storage component cannot be created or opened.
    pub async fn open(
        metadata_dir: impl AsRef<Path>,
        blob_dir: impl AsRef<Path>,
    ) -> So3Result<Self> {
        let metadata_repository =
            SqliteObjectMetadataRepository::new(metadata_dir.as_ref()).await?;
        let blob_repository = FileSystemBlobRepository::new(blob_dir.as_ref()).await?;
        let object_repository =
            PersistentObjectRepository::from_parts(metadata_repository.clone(), blob_repository);
        let consensus_journal = SqliteConsensusJournal::new(metadata_dir.as_ref()).await?;

        Ok(Self {
            metadata_repository,
            object_repository,
            consensus_journal,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::PersistentStorage;
    use crate::consensus::ConsensusCommandId;
    use crate::domain::{
        ObjectCommand, ObjectKey, ObjectLastModified, ObjectResult, ReadResult, WriteCommand,
    };
    use crate::storage::applied_command::repository::AppliedCommandStore;
    use crate::storage::object::repository::ObjectRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;

    #[tokio::test]
    async fn open_exposes_shared_durable_repositories() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let key = ObjectKey::new(ALPHA_KEY).unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);

        let written = storage
            .object_repository
            .write(&key, FIRST_VALUE.to_vec(), last_modified())
            .await
            .unwrap();
        let expected_result = ObjectResult::Read(ReadResult {
            object: Some(written.clone()),
        });
        storage
            .metadata_repository
            .save_result(&command_id, &expected_result)
            .await
            .unwrap();

        let loaded = storage.object_repository.read(&key).await.unwrap().unwrap();
        let result = storage
            .metadata_repository
            .load_result(&command_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded, written);
        assert_eq!(result, expected_result);
    }

    #[tokio::test]
    async fn open_initializes_consensus_journal_in_metadata_directory() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let command_id =
            ConsensusCommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
            last_modified: last_modified(),
        });

        let entry = storage
            .consensus_journal
            .record_committed(&command_id, &command.to_bytes().unwrap())
            .await
            .unwrap();

        assert_eq!(entry.command_id, command_id);
        assert_eq!(entry.command, command.to_bytes().unwrap());
        let loaded = storage
            .consensus_journal
            .load(&command_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, entry);
    }

    fn last_modified() -> ObjectLastModified {
        ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap()
    }
}
