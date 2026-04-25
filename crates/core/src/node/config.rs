use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use uuid::Uuid;

use crate::domain::error::{So3Error, So3Result};

// Default local node endpoints.
const DEFAULT_OBJECT_API_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_RPC_API_ADDR: &str = "127.0.0.1:4000";

// Default local persistence/config values.
const DEFAULT_DATA_DIR: &str = "./var/so3";
const DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS: u64 = 10;

// Env parsing delimiters.
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
        let object_api_addr =
            read_socket_addr(&get_var, "SO3_OBJECT_ADDR", DEFAULT_OBJECT_API_ADDR)?;
        let rpc_api_addr = read_socket_addr(&get_var, "SO3_RPC_ADDR", DEFAULT_RPC_API_ADDR)?;
        let object_request_timeout = read_duration_secs(
            &get_var,
            "SO3_OBJECT_REQUEST_TIMEOUT_SECS",
            DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS,
        )?;
        let data_dir =
            get_var("SO3_DATA_DIR").map_or_else(|| PathBuf::from(DEFAULT_DATA_DIR), PathBuf::from);
        let node_id = get_var("SO3_NODE_ID")
            .and_then(|value| Uuid::parse_str(&value).ok())
            .unwrap_or_else(Uuid::new_v4);
        let cluster = ClusterConfig {
            peers: read_socket_addr_list(&get_var, "SO3_CLUSTER_PEERS")?,
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

    #[test]
    fn from_env_with_uses_defaults() {
        let config = NodeConfig::from_env_with(|_| None).unwrap();

        assert_eq!(config.object_api_addr.to_string(), "127.0.0.1:3000");
        assert_eq!(config.rpc_api_addr.to_string(), "127.0.0.1:4000");
        assert_eq!(config.object_request_timeout, Duration::from_secs(10));
        assert_eq!(config.data_dir.to_string_lossy(), "./var/so3");
        assert!(config.cluster.peers.is_empty());
    }

    #[test]
    fn from_env_with_parses_overrides() {
        let config = NodeConfig::from_env_with(|name| match name {
            "SO3_OBJECT_ADDR" => Some("127.0.0.1:3100".to_owned()),
            "SO3_RPC_ADDR" => Some("127.0.0.1:4100".to_owned()),
            "SO3_OBJECT_REQUEST_TIMEOUT_SECS" => Some("25".to_owned()),
            "SO3_DATA_DIR" => Some("./tmp/so3".to_owned()),
            "SO3_NODE_ID" => Some("123e4567-e89b-12d3-a456-426614174000".to_owned()),
            "SO3_CLUSTER_PEERS" => Some("127.0.0.1:4101, 127.0.0.1:4102".to_owned()),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.object_api_addr.to_string(), "127.0.0.1:3100");
        assert_eq!(config.rpc_api_addr.to_string(), "127.0.0.1:4100");
        assert_eq!(config.object_request_timeout, Duration::from_secs(25));
        assert_eq!(config.data_dir.to_string_lossy(), "./tmp/so3");
        assert_eq!(
            config.node_id.to_string(),
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(config.cluster.peers.len(), 2);
        assert_eq!(config.cluster.peers[0].to_string(), "127.0.0.1:4101");
        assert_eq!(config.cluster.peers[1].to_string(), "127.0.0.1:4102");
    }

    #[test]
    fn from_env_with_reports_invalid_socket_addr() {
        let error = NodeConfig::from_env_with(|name| match name {
            "SO3_OBJECT_ADDR" => Some("not-an-address".to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains("SO3_OBJECT_ADDR"));
    }

    #[test]
    fn from_env_with_reports_invalid_timeout() {
        let error = NodeConfig::from_env_with(|name| match name {
            "SO3_OBJECT_REQUEST_TIMEOUT_SECS" => Some("NaN".to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("SO3_OBJECT_REQUEST_TIMEOUT_SECS")
        );
    }

    #[test]
    fn from_env_with_reports_invalid_cluster_peer() {
        let error = NodeConfig::from_env_with(|name| match name {
            "SO3_CLUSTER_PEERS" => Some("127.0.0.1:4101,not-an-address".to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains("SO3_CLUSTER_PEERS"));
    }
}
