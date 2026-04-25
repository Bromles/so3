use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use uuid::Uuid;

use so3_core::domain::error::{So3Error, So3Result};
use so3_core::node::config::{ClusterConfig, NodeConfig};

const CONFIG_PATH_ENV: &str = "SO3_CONFIG";
const OBJECT_ADDR_ENV: &str = "SO3_OBJECT_ADDR";
const RPC_ADDR_ENV: &str = "SO3_RPC_ADDR";
const DATA_DIR_ENV: &str = "SO3_DATA_DIR";
const METADATA_DIR_ENV: &str = "SO3_METADATA_DIR";
const BLOB_DIR_ENV: &str = "SO3_BLOB_DIR";
const NODE_ID_ENV: &str = "SO3_NODE_ID";
const CLUSTER_PEERS_ENV: &str = "SO3_CLUSTER_PEERS";
const OBJECT_REQUEST_TIMEOUT_SECS_ENV: &str = "SO3_OBJECT_REQUEST_TIMEOUT_SECS";

const DEFAULT_OBJECT_API_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_RPC_API_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_DATA_DIR: &str = "./var/so3";
const DEFAULT_METADATA_DIR_NAME: &str = "metadata";
const DEFAULT_BLOB_DIR_NAME: &str = "blobs";
const DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS: u64 = 10;
const DEFAULT_CONFIG_FILE_NAME: &str = "so3.toml";

const CLUSTER_PEERS_SEPARATOR: char = ',';

#[derive(Clone, Debug, Default, Deserialize)]
struct NodeConfigFile {
    node_id: Option<String>,
    object_api_addr: Option<String>,
    rpc_api_addr: Option<String>,
    object_request_timeout_secs: Option<u64>,
    data_dir: Option<PathBuf>,
    metadata_dir: Option<PathBuf>,
    blob_dir: Option<PathBuf>,
    #[serde(default)]
    cluster: ClusterConfigFile,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ClusterConfigFile {
    #[serde(default)]
    peers: Vec<String>,
}

/// # Errors
///
/// Returns an error when the optional TOML file or any supported environment value
/// cannot be parsed.
pub fn load_node_config() -> So3Result<NodeConfig> {
    load_node_config_with(|name| std::env::var(name).ok())
}

/// # Errors
///
/// Returns an error when the optional TOML file or any supplied configuration value
/// cannot be parsed.
fn load_node_config_with(get_var: impl Fn(&str) -> Option<String>) -> So3Result<NodeConfig> {
    let file_config = load_optional_config_file(&get_var)?;
    let config = build_node_config(file_config.as_ref(), get_var)?;
    config.validate()?;
    Ok(config)
}

fn build_node_config(
    file_config: Option<&NodeConfigFile>,
    get_var: impl Fn(&str) -> Option<String>,
) -> So3Result<NodeConfig> {
    let object_api_addr_value = pick_string(
        &get_var,
        OBJECT_ADDR_ENV,
        file_config.and_then(|config| config.object_api_addr.as_deref()),
        DEFAULT_OBJECT_API_ADDR,
    );
    let object_api_addr = parse_socket_addr(OBJECT_ADDR_ENV, &object_api_addr_value)?;
    let rpc_api_addr_value = pick_string(
        &get_var,
        RPC_ADDR_ENV,
        file_config.and_then(|config| config.rpc_api_addr.as_deref()),
        DEFAULT_RPC_API_ADDR,
    );
    let rpc_api_addr = parse_socket_addr(RPC_ADDR_ENV, &rpc_api_addr_value)?;
    let object_request_timeout = Duration::from_secs(pick_u64(
        &get_var,
        OBJECT_REQUEST_TIMEOUT_SECS_ENV,
        file_config.and_then(|config| config.object_request_timeout_secs),
        DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS,
    )?);
    let base_data_dir = pick_path_buf(
        &get_var,
        DATA_DIR_ENV,
        file_config.and_then(|config| config.data_dir.as_ref()),
        Path::new(DEFAULT_DATA_DIR),
    );
    let metadata_dir = pick_path_buf(
        &get_var,
        METADATA_DIR_ENV,
        file_config.and_then(|config| config.metadata_dir.as_ref()),
        &base_data_dir.join(DEFAULT_METADATA_DIR_NAME),
    );
    let blob_dir = pick_path_buf(
        &get_var,
        BLOB_DIR_ENV,
        file_config.and_then(|config| config.blob_dir.as_ref()),
        &base_data_dir.join(DEFAULT_BLOB_DIR_NAME),
    );
    let node_id = pick_uuid(
        &get_var,
        NODE_ID_ENV,
        file_config.and_then(|config| config.node_id.as_deref()),
    )?;
    let cluster = ClusterConfig {
        peers: pick_socket_addr_list(
            &get_var,
            CLUSTER_PEERS_ENV,
            file_config.map_or(&[][..], |config| config.cluster.peers.as_slice()),
        )?,
    };

    Ok(NodeConfig {
        node_id,
        object_api_addr,
        rpc_api_addr,
        object_request_timeout,
        metadata_dir,
        blob_dir,
        cluster,
    })
}

fn parse_socket_addr(name: &str, value: &str) -> So3Result<SocketAddr> {
    SocketAddr::from_str(value).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to parse {name}={value}: {error}"))
    })
}

fn pick_string(
    get_var: &impl Fn(&str) -> Option<String>,
    env_name: &str,
    file_value: Option<&str>,
    default_value: &str,
) -> String {
    get_var(env_name)
        .or_else(|| file_value.map(ToOwned::to_owned))
        .unwrap_or_else(|| default_value.to_owned())
}

fn pick_u64(
    get_var: &impl Fn(&str) -> Option<String>,
    env_name: &str,
    file_value: Option<u64>,
    default_value: u64,
) -> So3Result<u64> {
    let value = get_var(env_name)
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                So3Error::InvalidRequest(format!("failed to parse {env_name}={value}: {error}"))
            })
        })
        .transpose()?;

    Ok(value.or(file_value).unwrap_or(default_value))
}

fn pick_path_buf(
    get_var: &impl Fn(&str) -> Option<String>,
    env_name: &str,
    file_value: Option<&PathBuf>,
    default_value: &Path,
) -> PathBuf {
    get_var(env_name)
        .map(PathBuf::from)
        .or_else(|| file_value.cloned())
        .unwrap_or_else(|| default_value.to_path_buf())
}

fn pick_uuid(
    get_var: &impl Fn(&str) -> Option<String>,
    env_name: &str,
    file_value: Option<&str>,
) -> So3Result<Uuid> {
    match get_var(env_name).or_else(|| file_value.map(ToOwned::to_owned)) {
        Some(value) => Uuid::parse_str(&value).map_err(|error| {
            So3Error::InvalidRequest(format!("failed to parse {env_name}={value}: {error}"))
        }),
        None => Ok(Uuid::new_v4()),
    }
}

fn pick_socket_addr_list(
    get_var: &impl Fn(&str) -> Option<String>,
    env_name: &str,
    file_values: &[String],
) -> So3Result<Vec<SocketAddr>> {
    if let Some(value) = get_var(env_name) {
        return value
            .split(CLUSTER_PEERS_SEPARATOR)
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| parse_socket_addr_entry(env_name, entry))
            .collect();
    }

    file_values
        .iter()
        .map(String::as_str)
        .map(|entry| parse_socket_addr_entry(env_name, entry))
        .collect()
}

fn parse_socket_addr_entry(name: &str, value: &str) -> So3Result<SocketAddr> {
    SocketAddr::from_str(value).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to parse {name} entry {value}: {error}"))
    })
}

fn load_optional_config_file(
    get_var: &impl Fn(&str) -> Option<String>,
) -> So3Result<Option<NodeConfigFile>> {
    if let Some(path) = get_var(CONFIG_PATH_ENV) {
        return parse_config_file(path).map(Some);
    }

    let default_path = PathBuf::from(DEFAULT_CONFIG_FILE_NAME);
    if default_path.exists() {
        return parse_config_file(default_path).map(Some);
    }

    Ok(None)
}

fn parse_config_file(path: impl AsRef<Path>) -> So3Result<NodeConfigFile> {
    let path = path.as_ref();
    let value = std::fs::read_to_string(path).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to read {}: {error}", path.display()))
    })?;
    parse_toml_config(&value)
}

fn parse_toml_config(value: &str) -> So3Result<NodeConfigFile> {
    toml::from_str(value).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to parse so3 TOML config: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::load_node_config_with;

    const DEFAULT_OBJECT_ADDR: &str = "127.0.0.1:3000";
    const DEFAULT_RPC_ADDR: &str = "127.0.0.1:4000";
    const OVERRIDE_OBJECT_ADDR: &str = "127.0.0.1:3100";
    const OVERRIDE_RPC_ADDR: &str = "127.0.0.1:4100";
    const PEER_ONE_ADDR: &str = "127.0.0.1:4101";
    const PEER_TWO_ADDR: &str = "127.0.0.1:4102";
    const OVERRIDE_TIMEOUT_SECS: u64 = 25;
    const OVERRIDE_DATA_DIR: &str = "./tmp/so3";
    const OVERRIDE_METADATA_DIR: &str = "./tmp/so3-metadata";
    const OVERRIDE_BLOB_DIR: &str = "./tmp/so3-blobs";
    const FIXED_NODE_ID: &str = "123e4567-e89b-12d3-a456-426614174000";
    const INVALID_SOCKET_ADDR: &str = "not-an-address";
    const INVALID_TIMEOUT: &str = "NaN";
    const TOML_CONFIG: &str = r#"
        node_id = "123e4567-e89b-12d3-a456-426614174000"
        object_api_addr = "127.0.0.1:3200"
        rpc_api_addr = "127.0.0.1:4200"
        object_request_timeout_secs = 15
        data_dir = "./tmp/toml"

        [cluster]
        peers = ["127.0.0.1:4201", "127.0.0.1:4202"]
    "#;

    #[test]
    fn load_node_config_with_uses_defaults() {
        let config = load_node_config_with(|_| None).unwrap();

        assert_eq!(config.object_api_addr.to_string(), DEFAULT_OBJECT_ADDR);
        assert_eq!(config.rpc_api_addr.to_string(), DEFAULT_RPC_ADDR);
        assert_eq!(
            config.object_request_timeout,
            Duration::from_secs(super::DEFAULT_OBJECT_REQUEST_TIMEOUT_SECS)
        );
        assert_eq!(
            config.metadata_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR).join(super::DEFAULT_METADATA_DIR_NAME)
        );
        assert_eq!(
            config.blob_dir,
            PathBuf::from(super::DEFAULT_DATA_DIR).join(super::DEFAULT_BLOB_DIR_NAME)
        );
        assert!(config.cluster.peers.is_empty());
    }

    #[test]
    fn load_node_config_with_parses_overrides() {
        let config = load_node_config_with(|name| match name {
            super::OBJECT_ADDR_ENV => Some(OVERRIDE_OBJECT_ADDR.to_owned()),
            super::RPC_ADDR_ENV => Some(OVERRIDE_RPC_ADDR.to_owned()),
            super::OBJECT_REQUEST_TIMEOUT_SECS_ENV => Some(OVERRIDE_TIMEOUT_SECS.to_string()),
            super::DATA_DIR_ENV => Some(OVERRIDE_DATA_DIR.to_owned()),
            super::METADATA_DIR_ENV => Some(OVERRIDE_METADATA_DIR.to_owned()),
            super::BLOB_DIR_ENV => Some(OVERRIDE_BLOB_DIR.to_owned()),
            super::NODE_ID_ENV => Some(FIXED_NODE_ID.to_owned()),
            super::CLUSTER_PEERS_ENV => Some(format!("{PEER_ONE_ADDR}, {PEER_TWO_ADDR}")),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.object_api_addr.to_string(), OVERRIDE_OBJECT_ADDR);
        assert_eq!(config.rpc_api_addr.to_string(), OVERRIDE_RPC_ADDR);
        assert_eq!(
            config.object_request_timeout,
            Duration::from_secs(OVERRIDE_TIMEOUT_SECS)
        );
        assert_eq!(config.metadata_dir.to_string_lossy(), OVERRIDE_METADATA_DIR);
        assert_eq!(config.blob_dir.to_string_lossy(), OVERRIDE_BLOB_DIR);
        assert_eq!(config.node_id.to_string(), FIXED_NODE_ID);
        assert_eq!(config.cluster.peers.len(), 2);
        assert_eq!(config.cluster.peers[0].to_string(), PEER_ONE_ADDR);
        assert_eq!(config.cluster.peers[1].to_string(), PEER_TWO_ADDR);
    }

    #[test]
    fn load_node_config_with_reports_invalid_socket_addr() {
        let error = load_node_config_with(|name| match name {
            super::OBJECT_ADDR_ENV => Some(INVALID_SOCKET_ADDR.to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains(super::OBJECT_ADDR_ENV));
    }

    #[test]
    fn load_node_config_with_reports_invalid_timeout() {
        let error = load_node_config_with(|name| match name {
            super::OBJECT_REQUEST_TIMEOUT_SECS_ENV => Some(INVALID_TIMEOUT.to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(super::OBJECT_REQUEST_TIMEOUT_SECS_ENV)
        );
    }

    #[test]
    fn load_node_config_with_reports_invalid_cluster_peer() {
        let error = load_node_config_with(|name| match name {
            super::CLUSTER_PEERS_ENV => Some(format!("{PEER_ONE_ADDR},{INVALID_SOCKET_ADDR}")),
            _ => None,
        })
        .unwrap_err();

        assert!(error.to_string().contains(super::CLUSTER_PEERS_ENV));
    }

    #[test]
    fn build_node_config_parses_toml_file() {
        let file_config = super::parse_toml_config(TOML_CONFIG).unwrap();
        let config = super::build_node_config(Some(&file_config), |_| None).unwrap();

        assert_eq!(config.object_api_addr.to_string(), "127.0.0.1:3200");
        assert_eq!(config.rpc_api_addr.to_string(), "127.0.0.1:4200");
        assert_eq!(config.object_request_timeout, Duration::from_secs(15));
        assert_eq!(
            config.metadata_dir,
            PathBuf::from("./tmp/toml").join(super::DEFAULT_METADATA_DIR_NAME)
        );
        assert_eq!(
            config.blob_dir,
            PathBuf::from("./tmp/toml").join(super::DEFAULT_BLOB_DIR_NAME)
        );
        assert_eq!(config.cluster.peers.len(), 2);
    }

    #[test]
    fn build_node_config_prefers_env_over_toml_values() {
        let file_config = super::parse_toml_config(TOML_CONFIG).unwrap();
        let config = super::build_node_config(Some(&file_config), |name| match name {
            super::OBJECT_ADDR_ENV => Some(OVERRIDE_OBJECT_ADDR.to_owned()),
            super::OBJECT_REQUEST_TIMEOUT_SECS_ENV => Some(OVERRIDE_TIMEOUT_SECS.to_string()),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.object_api_addr.to_string(), OVERRIDE_OBJECT_ADDR);
        assert_eq!(
            config.object_request_timeout,
            Duration::from_secs(OVERRIDE_TIMEOUT_SECS)
        );
        assert_eq!(config.rpc_api_addr.to_string(), "127.0.0.1:4200");
    }

    #[test]
    fn parse_config_file_reads_config_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("so3.toml");
        std::fs::write(&config_path, TOML_CONFIG).unwrap();

        let file_config = super::parse_config_file(&config_path).unwrap();
        let config = super::build_node_config(Some(&file_config), |_| None).unwrap();

        assert_eq!(config.node_id.to_string(), FIXED_NODE_ID);
    }
}
