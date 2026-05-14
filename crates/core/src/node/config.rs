use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: Option<Uuid>,
    pub object_api_addr: SocketAddr,
    pub rpc_api_addr: SocketAddr,
    pub object_request_timeout: Duration,
    pub metadata_dir: PathBuf,
    pub blob_dir: PathBuf,
    pub cluster: ClusterConfig,
}

#[derive(Clone, Debug)]
pub struct PeerConfig {
    pub node_id: Uuid,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterConfig {
    pub peers: Vec<PeerConfig>,
}

impl NodeConfig {
    /// # Errors
    ///
    /// Returns an error when the configured local endpoints are internally inconsistent.
    pub fn validate(&self) -> So3Result<()> {
        if self.object_api_addr == self.rpc_api_addr && self.object_api_addr.port() != 0 {
            return Err(So3Error::InvalidRequest(format!(
                "SO3_OBJECT_ADDR and SO3_RPC_ADDR must differ, both resolved to {}",
                self.object_api_addr
            )));
        }
        if self.metadata_dir == self.blob_dir {
            return Err(So3Error::InvalidRequest(format!(
                "SO3_METADATA_DIR and SO3_BLOB_DIR must differ, both resolved to {}",
                self.metadata_dir.display()
            )));
        }
        let mut seen_peer_ids = HashSet::new();
        for peer in &self.cluster.peers {
            if !seen_peer_ids.insert(peer.node_id) {
                return Err(So3Error::InvalidRequest(format!(
                    "duplicate peer node_id {} in cluster configuration",
                    peer.node_id
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::{ClusterConfig, NodeConfig};

    const EPHEMERAL_LOOPBACK_ADDR: &str = "127.0.0.1:0";
    const OBJECT_API_ADDR: &str = "127.0.0.1:3000";
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const DATA_DIR: &str = "./var/so3";

    fn test_config() -> NodeConfig {
        NodeConfig {
            node_id: Some(Uuid::nil()),
            object_api_addr: EPHEMERAL_LOOPBACK_ADDR.parse().unwrap(),
            rpc_api_addr: "127.0.0.1:4000".parse().unwrap(),
            object_request_timeout: Duration::from_secs(REQUEST_TIMEOUT_SECS),
            metadata_dir: std::path::PathBuf::from(DATA_DIR).join("metadata"),
            blob_dir: std::path::PathBuf::from(DATA_DIR).join("blobs"),
            cluster: ClusterConfig::default(),
        }
    }

    #[test]
    fn validate_allows_ephemeral_duplicate_ports() {
        let config = test_config();

        let result = config.validate();

        assert!(result.is_ok());
    }

    #[test]
    fn validate_rejects_same_object_and_rpc_addresses() {
        let mut config = test_config();
        config.object_api_addr = OBJECT_API_ADDR.parse().unwrap();
        config.rpc_api_addr = OBJECT_API_ADDR.parse().unwrap();

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("must differ"));
    }

    #[test]
    fn validate_rejects_same_metadata_and_blob_directories() {
        let mut config = test_config();
        config.blob_dir = config.metadata_dir.clone();

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("SO3_METADATA_DIR"));
    }
}
