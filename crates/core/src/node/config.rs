use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};

// Supported environment variables.
const OBJECT_ADDR_ENV: &str = "SO3_OBJECT_ADDR";
const RPC_ADDR_ENV: &str = "SO3_RPC_ADDR";
const DATA_DIR_ENV: &str = "SO3_DATA_DIR";
const NODE_ID_ENV: &str = "SO3_NODE_ID";
const CLUSTER_PEERS_ENV: &str = "SO3_CLUSTER_PEERS";
const OBJECT_REQUEST_TIMEOUT_SECS_ENV: &str = "SO3_OBJECT_REQUEST_TIMEOUT_SECS";

// Default node configuration.
const DEFAULT_OBJECT_API_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_RPC_API_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_DATA_DIR: &str = "./var/so3";
const DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS: u64 = 10;

// Delimiters and formatting.
const CLUSTER_PEERS_SEPARATOR: char = ',';

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub node_id: Uuid,
    pub object_api_addr: SocketAddr,
    pub rpc_api_addr: SocketAddr,
    pub object_request_timeout: Duration,
    pub data_dir: PathBuf,
    pub cluster: ClusterConfig,
}

#[derive(Clone, Debug, Default)]
pub struct ClusterConfig {
    pub peers: Vec<SocketAddr>,
}

impl NodeConfig {
    /// # Errors
    ///
    /// Returns an error when any supported environment variable contains an invalid value.
    pub fn from_env() -> So3Result<Self> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    /// # Errors
    ///
    /// Returns an error when any supplied configuration value cannot be parsed.
    pub fn from_env_with(get_var: impl Fn(&str) -> Option<String>) -> So3Result<Self> {
        let object_api_addr = read_socket_addr(&get_var, OBJECT_ADDR_ENV, DEFAULT_OBJECT_API_ADDR)?;
        let rpc_api_addr = read_socket_addr(&get_var, RPC_ADDR_ENV, DEFAULT_RPC_API_ADDR)?;
        let object_request_timeout = read_duration_secs(
            &get_var,
            OBJECT_REQUEST_TIMEOUT_SECS_ENV,
            DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS,
        )?;
        let data_dir =
            get_var(DATA_DIR_ENV).map_or_else(|| PathBuf::from(DEFAULT_DATA_DIR), PathBuf::from);
        let node_id = get_var(NODE_ID_ENV)
            .and_then(|value| Uuid::parse_str(&value).ok())
            .unwrap_or_else(Uuid::new_v4);
        let cluster = ClusterConfig {
            peers: read_socket_addr_list(&get_var, CLUSTER_PEERS_ENV)?,
        };

        Ok(Self {
            node_id,
            object_api_addr,
            rpc_api_addr,
            object_request_timeout,
            data_dir,
            cluster,
        })
    }

    /// # Errors
    ///
    /// Returns an error when the configured local endpoints are internally inconsistent.
    pub fn validate(&self) -> So3Result<()> {
        if self.object_api_addr == self.rpc_api_addr {
            return Err(So3Error::InvalidRequest(format!(
                "SO3_OBJECT_ADDR and SO3_RPC_ADDR must differ, both resolved to {}",
                self.object_api_addr
            )));
        }

        Ok(())
    }
}

fn read_socket_addr(
    get_var: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: &str,
) -> So3Result<SocketAddr> {
    let value = get_var(name).unwrap_or_else(|| default.to_owned());
    SocketAddr::from_str(&value).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to parse {name}={value}: {error}"))
    })
}

fn read_duration_secs(
    get_var: &impl Fn(&str) -> Option<String>,
    name: &str,
    default_secs: u64,
) -> So3Result<Duration> {
    let value = get_var(name).unwrap_or_else(|| default_secs.to_string());
    let seconds = value.parse::<u64>().map_err(|error| {
        So3Error::InvalidRequest(format!("failed to parse {name}={value}: {error}"))
    })?;

    Ok(Duration::from_secs(seconds))
}

fn read_socket_addr_list(
    get_var: &impl Fn(&str) -> Option<String>,
    name: &str,
) -> So3Result<Vec<SocketAddr>> {
    let Some(value) = get_var(name) else {
        return Ok(Vec::new());
    };

    value
        .split(CLUSTER_PEERS_SEPARATOR)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            SocketAddr::from_str(value).map_err(|error| {
                So3Error::InvalidRequest(format!("failed to parse {name} entry {value}: {error}"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::NodeConfig;

    const DEFAULT_OBJECT_ADDR: &str = "127.0.0.1:3000";
    const DEFAULT_RPC_ADDR: &str = "127.0.0.1:4000";
    const OVERRIDE_OBJECT_ADDR: &str = "127.0.0.1:3100";
    const OVERRIDE_RPC_ADDR: &str = "127.0.0.1:4100";
    const PEER_ONE_ADDR: &str = "127.0.0.1:4101";
    const PEER_TWO_ADDR: &str = "127.0.0.1:4102";
    const OVERRIDE_TIMEOUT_SECS: u64 = 25;
    const OVERRIDE_DATA_DIR: &str = "./tmp/so3";
    const FIXED_NODE_ID: &str = "123e4567-e89b-12d3-a456-426614174000";
    const INVALID_SOCKET_ADDR: &str = "not-an-address";
    const INVALID_TIMEOUT: &str = "NaN";

    #[test]
    fn from_env_with_uses_defaults() {
        let config = NodeConfig::from_env_with(|_| None).unwrap();
        config.validate().unwrap();

        assert_eq!(config.object_api_addr.to_string(), DEFAULT_OBJECT_ADDR);
        assert_eq!(config.rpc_api_addr.to_string(), DEFAULT_RPC_ADDR);
        assert_eq!(
            config.object_request_timeout,
            Duration::from_secs(super::DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS)
        );
        assert_eq!(config.data_dir.to_string_lossy(), "./var/so3");
        assert!(config.cluster.peers.is_empty());
    }

    #[test]
    fn from_env_with_parses_overrides() {
        let config = NodeConfig::from_env_with(|name| match name {
            super::OBJECT_ADDR_ENV => Some(OVERRIDE_OBJECT_ADDR.to_owned()),
            super::RPC_ADDR_ENV => Some(OVERRIDE_RPC_ADDR.to_owned()),
            super::OBJECT_REQUEST_TIMEOUT_SECS_ENV => Some(OVERRIDE_TIMEOUT_SECS.to_string()),
            super::DATA_DIR_ENV => Some(OVERRIDE_DATA_DIR.to_owned()),
            super::NODE_ID_ENV => Some(FIXED_NODE_ID.to_owned()),
            super::CLUSTER_PEERS_ENV => Some(format!("{PEER_ONE_ADDR}, {PEER_TWO_ADDR}")),
            _ => None,
        })
        .unwrap();
        config.validate().unwrap();

        assert_eq!(config.object_api_addr.to_string(), OVERRIDE_OBJECT_ADDR);
        assert_eq!(config.rpc_api_addr.to_string(), OVERRIDE_RPC_ADDR);
        assert_eq!(
            config.object_request_timeout,
            Duration::from_secs(OVERRIDE_TIMEOUT_SECS)
        );
        assert_eq!(config.data_dir.to_string_lossy(), OVERRIDE_DATA_DIR);
        assert_eq!(config.node_id.to_string(), FIXED_NODE_ID);
        assert_eq!(config.cluster.peers.len(), 2);
        assert_eq!(config.cluster.peers[0].to_string(), PEER_ONE_ADDR);
        assert_eq!(config.cluster.peers[1].to_string(), PEER_TWO_ADDR);
    }

    #[test]
    fn from_env_with_reports_invalid_socket_addr() {
        let error = NodeConfig::from_env_with(|name| match name {
            super::OBJECT_ADDR_ENV => Some(INVALID_SOCKET_ADDR.to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains(super::OBJECT_ADDR_ENV));
    }

    #[test]
    fn from_env_with_reports_invalid_timeout() {
        let error = NodeConfig::from_env_with(|name| match name {
            super::OBJECT_REQUEST_TIMEOUT_SECS_ENV => Some(INVALID_TIMEOUT.to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains(super::OBJECT_REQUEST_TIMEOUT_SECS_ENV));
    }

    #[test]
    fn from_env_with_reports_invalid_cluster_peer() {
        let error = NodeConfig::from_env_with(|name| match name {
            super::CLUSTER_PEERS_ENV => Some(format!("{PEER_ONE_ADDR},{INVALID_SOCKET_ADDR}")),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains(super::CLUSTER_PEERS_ENV));
    }

    #[test]
    fn validate_rejects_same_object_and_rpc_addresses() {
        let config = NodeConfig::from_env_with(|name| match name {
            super::OBJECT_ADDR_ENV | super::RPC_ADDR_ENV => Some(OVERRIDE_OBJECT_ADDR.to_owned()),
            _ => None,
        })
        .unwrap();

        let error = config.validate().unwrap_err();

        assert!(error.to_string().contains("must differ"));
    }
}
