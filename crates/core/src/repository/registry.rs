use std::path::Path;

use crate::consensus::journal::SqliteConsensusJournal;
use crate::domain::error::So3Result;
use crate::repository::blob::fs::FileSystemBlobRepository;
use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;

pub struct RepositoryRegistry {
    pub metadata_repository: SqliteObjectMetadataRepository,
    pub blob_repository: FileSystemBlobRepository,
    pub consensus_journal: SqliteConsensusJournal,
}

impl RepositoryRegistry {
    /// # Errors
    ///
    /// Returns an error if any durable local repository component cannot be created or opened.
    pub async fn new(
        metadata_dir: impl AsRef<Path>,
        blob_dir: impl AsRef<Path>,
    ) -> So3Result<Self> {
        let metadata_repository =
            SqliteObjectMetadataRepository::new(metadata_dir.as_ref()).await?;
        let blob_repository = FileSystemBlobRepository::new(blob_dir.as_ref()).await?;
        let consensus_journal = SqliteConsensusJournal::new(metadata_dir.as_ref()).await?;

        Ok(Self {
            metadata_repository,
            blob_repository,
            consensus_journal,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::RepositoryRegistry;
    use crate::consensus::CommandId;
    use crate::domain::blob::BlobMetadata;
    use crate::domain::command::{CommandResult, ObjectCommand, ReadResult, WriteCommand};
    use crate::domain::object::ObjectLastModified;
    use crate::domain::object_key::ObjectKey;
    use crate::repository::applied_command::AppliedCommandRepository;

    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;

    #[tokio::test]
    async fn open_exposes_shared_durable_repositories() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RepositoryRegistry::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let key = ObjectKey::new(ALPHA_KEY).unwrap();
        let command_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);

        let written = storage
            .object_repository
            .write(
                &key,
                BlobMetadata::Inline(FIRST_VALUE.to_vec()),
                last_modified(),
            )
            .await
            .unwrap();
        let expected_result = CommandResult::Read(ReadResult {
            metadata: Some(written.clone()),
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
        let storage = RepositoryRegistry::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let command_id =
            CommandId::new(COMMAND_ORIGIN_NODE_ID.to_owned(), COMMAND_SEQUENCE_ONE);
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobMetadata::Inline(FIRST_VALUE.to_vec()),
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
