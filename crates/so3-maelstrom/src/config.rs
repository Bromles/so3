use std::path::{Path, PathBuf};

const DATA_DIR_ENV: &str = "SO3_MAELSTROM_DATA_DIR";
const METADATA_DIR_ENV: &str = "SO3_MAELSTROM_METADATA_DIR";
const BLOB_DIR_ENV: &str = "SO3_MAELSTROM_BLOB_DIR";
const DEFAULT_DATA_DIR: &str = "./var/so3-maelstrom";
const DEFAULT_METADATA_DIR_NAME: &str = "metadata";
const DEFAULT_BLOB_DIR_NAME: &str = "blobs";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRoots {
    pub metadata_dir: PathBuf,
    pub blob_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeStorageDirs {
    pub metadata_dir: PathBuf,
    pub blob_dir: PathBuf,
}

impl StorageRoots {
    #[must_use]
    pub fn for_node(&self, node_id: &str) -> NodeStorageDirs {
        NodeStorageDirs {
            metadata_dir: self.metadata_dir.join(node_id),
            blob_dir: self.blob_dir.join(node_id),
        }
    }
}

pub fn load_storage_roots() -> StorageRoots {
    load_storage_roots_with(|name| std::env::var(name).ok())
}

fn load_storage_roots_with(get_var: impl Fn(&str) -> Option<String>) -> StorageRoots {
    let data_dir =
        get_var(DATA_DIR_ENV).map_or_else(|| PathBuf::from(DEFAULT_DATA_DIR), PathBuf::from);

    StorageRoots {
        metadata_dir: get_var(METADATA_DIR_ENV).map_or_else(
            || data_dir.join(Path::new(DEFAULT_METADATA_DIR_NAME)),
            PathBuf::from,
        ),
        blob_dir: get_var(BLOB_DIR_ENV).map_or_else(
            || data_dir.join(Path::new(DEFAULT_BLOB_DIR_NAME)),
            PathBuf::from,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::load_storage_roots_with;

    #[test]
    fn load_storage_roots_uses_default_layout() {
        let roots = load_storage_roots_with(|_| None);

        assert_eq!(
            roots.metadata_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR).join(super::DEFAULT_METADATA_DIR_NAME)
        );
        assert_eq!(
            roots.blob_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR).join(super::DEFAULT_BLOB_DIR_NAME)
        );
    }

    #[test]
    fn storage_roots_builds_isolated_node_layout() {
        let roots = load_storage_roots_with(|_| None);
        let node_dirs = roots.for_node("n1");

        assert_eq!(
            node_dirs.metadata_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR)
                .join(super::DEFAULT_METADATA_DIR_NAME)
                .join("n1")
        );
        assert_eq!(
            node_dirs.blob_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR)
                .join(super::DEFAULT_BLOB_DIR_NAME)
                .join("n1")
        );
    }

    #[test]
    fn load_storage_roots_prefers_explicit_overrides() {
        let roots = load_storage_roots_with(|name| match name {
            super::DATA_DIR_ENV => Some("./tmp/ignored".to_owned()),
            super::METADATA_DIR_ENV => Some("./tmp/metadata".to_owned()),
            super::BLOB_DIR_ENV => Some("./tmp/blobs".to_owned()),
            _ => None,
        });

        assert_eq!(roots.metadata_dir, PathBuf::from("./tmp/metadata"));
        assert_eq!(roots.blob_dir, PathBuf::from("./tmp/blobs"));
    }
}
