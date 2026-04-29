use std::collections::HashMap;

use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response};

use crate::consensus::coordinator::ConsensusPeerTransport;
use crate::domain::error::{So3Error, So3Result};
use crate::proto::consensus_transport_client::ConsensusTransportClient;
use crate::proto::{PreAcceptRequest, PreAcceptResponse};

const HTTP_SCHEME_PREFIX: &str = "http://";
const HTTPS_SCHEME_PREFIX: &str = "https://";

#[derive(Clone, Debug, Default)]
pub struct TonicConsensusPeerTransport {
    channels: HashMap<String, Channel>,
}

impl TonicConsensusPeerTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// # Errors
    ///
    /// Returns an error when any configured peer endpoint cannot be parsed.
    pub fn from_peer_ids(peer_ids: impl IntoIterator<Item = String>) -> So3Result<Self> {
        let mut transport = Self::new();
        for peer_id in peer_ids {
            let endpoint = endpoint_from_peer_id(&peer_id)?;
            transport.channels.insert(peer_id, endpoint.connect_lazy());
        }

        Ok(transport)
    }

    fn client_for(&self, peer_id: &str) -> So3Result<ConsensusTransportClient<Channel>> {
        let channel = self.channels.get(peer_id).cloned().ok_or_else(|| {
            So3Error::InvalidRequest(format!("unknown consensus peer endpoint: {peer_id}"))
        })?;

        Ok(ConsensusTransportClient::new(channel))
    }

    /// # Errors
    ///
    /// Returns an error when the peer is unknown or rejects the pre-accept request.
    pub async fn pre_accept(
        &self,
        peer_id: &str,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        self.client_for(peer_id)?
            .pre_accept(Request::new(request))
            .await
            .map(Response::into_inner)
            .map_err(|status| map_tonic_status(&status))
    }

    /// # Errors
    ///
    /// Returns an error when the peer is unknown or rejects the accept request.
    pub async fn accept(&self, peer_id: &str, request: AcceptRequest) -> So3Result<AcceptResponse> {
        self.client_for(peer_id)?
            .accept(Request::new(request))
            .await
            .map(Response::into_inner)
            .map_err(|status| map_tonic_status(&status))
    }

    /// # Errors
    ///
    /// Returns an error when the peer is unknown or rejects the commit request.
    pub async fn commit(&self, peer_id: &str, request: CommitRequest) -> So3Result<CommitResponse> {
        self.client_for(peer_id)?
            .commit(Request::new(request))
            .await
            .map(Response::into_inner)
            .map_err(|status| map_tonic_status(&status))
    }

    /// # Errors
    ///
    /// Returns an error when the peer is unknown or rejects the recover request.
    pub async fn recover(
        &self,
        peer_id: &str,
        request: RecoverRequest,
    ) -> So3Result<RecoverResponse> {
        self.client_for(peer_id)?
            .recover(Request::new(request))
            .await
            .map(Response::into_inner)
            .map_err(|status| map_tonic_status(&status))
    }
}

#[async_trait]
impl ConsensusPeerTransport for TonicConsensusPeerTransport {
    async fn pre_accept_peer(
        &mut self,
        peer_id: &str,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        self.pre_accept(peer_id, request).await
    }

    async fn accept_peer(
        &mut self,
        peer_id: &str,
        request: AcceptRequest,
    ) -> So3Result<AcceptResponse> {
        self.accept(peer_id, request).await
    }

    async fn commit_peer(
        &mut self,
        peer_id: &str,
        request: CommitRequest,
    ) -> So3Result<CommitResponse> {
        self.commit(peer_id, request).await
    }

    async fn recover_peer(
        &mut self,
        peer_id: &str,
        request: RecoverRequest,
    ) -> So3Result<RecoverResponse> {
        self.recover(peer_id, request).await
    }
}

fn endpoint_from_peer_id(peer_id: &str) -> So3Result<Endpoint> {
    let uri = if peer_id.starts_with(HTTP_SCHEME_PREFIX) || peer_id.starts_with(HTTPS_SCHEME_PREFIX)
    {
        peer_id.to_owned()
    } else {
        format!("{HTTP_SCHEME_PREFIX}{peer_id}")
    };

    Endpoint::from_shared(uri).map_err(|error| {
        So3Error::InvalidRequest(format!("invalid peer endpoint {peer_id}: {error}"))
    })
}

fn map_tonic_status(status: &tonic::Status) -> So3Error {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => So3Error::PeerUnavailable(
            format!("peer returned {}: {}", status.code(), status.message()),
        ),
        _ => So3Error::InvalidRequest(format!(
            "consensus peer returned {}: {}",
            status.code(),
            status.message()
        )),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::spawn;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::TonicConsensusPeerTransport;
    use crate::consensus::coordinator::ConsensusPeerTransport;
    use crate::consensus::executor::PersistentReplicatedCommandExecutor;
    use crate::consensus::journal::SqliteConsensusJournal;
    use crate::domain::{BlobPayload, ObjectCommand, ObjectKey, ObjectResult, WriteCommand};
    use crate::domain::blob::BlobPayload;
    use crate::domain::command::{ObjectCommand, WriteCommand};
    use crate::domain::object_key::ObjectKey;
    use crate::proto::EventPayload;
    use crate::repository::metadata::sqlite::SqliteObjectMetadataRepository;
    use crate::repository::registry::SqliteFsPersistentObjectRepository;
    use crate::rpc_server::proto::{
        Ballot, CommandId, CommitRequest, EventPayload, PreAcceptRequest, RecoverRequest, State,
    };
    use crate::rpc_server::server::RpcServer;
    use crate::rpc_server::transport::{ApplyingConsensusTransport, RejectingConsensusTransport};

    const LOOPBACK_EPHEMERAL_ADDR: &str = "127.0.0.1:0";
    const COMMAND_ORIGIN_NODE_ID: &str = "node-a";
    const COMMAND_SEQUENCE_ONE: u64 = 1;
    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";

    #[tokio::test]
    async fn pre_accept_peer_roundtrips_to_rpc_server() {
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let peer_id = listener.local_addr().unwrap().to_string();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();
        let server_task = spawn(async move {
            RpcServer::new(RejectingConsensusTransport::new(Uuid::nil()))
                .run(listener, shutdown_token)
                .await
        });
        let mut transport = TonicConsensusPeerTransport::from_peer_ids([peer_id.clone()]).unwrap();

        let response = transport
            .pre_accept_peer(&peer_id, PreAcceptRequest::default())
            .await
            .unwrap();

        assert!(response.nack);
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn commit_peer_executes_serialized_command_on_remote_rpc_server() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let metadata_repository =
            SqliteObjectMetadataRepository::new(temp_dir.path().join("metadata"))
                .await
                .unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path().join("consensus"))
            .await
            .unwrap();
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let peer_id = listener.local_addr().unwrap().to_string();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();
        let server_task = spawn(async move {
            RpcServer::new(ApplyingConsensusTransport::new(
                Uuid::nil().to_string(),
                PersistentReplicatedCommandExecutor::new(repository, metadata_repository),
                journal,
            ))
            .run(listener, shutdown_token)
            .await
        });
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            metadata: BlobPayload::Inline(FIRST_VALUE.to_vec()),
            last_modified: test_last_modified(),
        });
        let mut transport = TonicConsensusPeerTransport::from_peer_ids([peer_id.clone()]).unwrap();

        let response = transport
            .commit_peer(
                &peer_id,
                CommitRequest {
                    command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                    event: Some(EventPayload {
                        command: command.to_bytes().unwrap(),
                    }),
                    ..CommitRequest::default()
                },
            )
            .await
            .unwrap();

        let result = ObjectResult::from_bytes(&response.result).unwrap();
        let ObjectResult::Write(write) = result else {
            panic!("expected write result");
        };
        assert_eq!(write.record.content_length, FIRST_VALUE.len() as u64);
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn recover_peer_roundtrips_to_rpc_server() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsPersistentObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let metadata_repository =
            SqliteObjectMetadataRepository::new(temp_dir.path().join("metadata"))
                .await
                .unwrap();
        let journal = SqliteConsensusJournal::new(temp_dir.path().join("consensus"))
            .await
            .unwrap();
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let peer_id = listener.local_addr().unwrap().to_string();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();
        let server_task = spawn(async move {
            RpcServer::new(ApplyingConsensusTransport::new(
                Uuid::nil().to_string(),
                PersistentReplicatedCommandExecutor::new(repository, metadata_repository),
                journal,
            ))
            .run(listener, shutdown_token)
            .await
        });
        let mut transport = TonicConsensusPeerTransport::from_peer_ids([peer_id.clone()]).unwrap();

        let response = transport
            .recover_peer(
                &peer_id,
                RecoverRequest {
                    command_id: Some(command_id(COMMAND_SEQUENCE_ONE)),
                    ballot: Some(Ballot {
                        round: 0,
                        node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
                    }),
                    ..RecoverRequest::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(response.local_state, State::Undefined as i32);
        assert!(response.wait_for.is_empty());
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }

    fn command_id(sequence: u64) -> CommandId {
        CommandId {
            origin_node_id: COMMAND_ORIGIN_NODE_ID.to_owned(),
            sequence,
        }
    }
    fn test_last_modified() -> crate::domain::ObjectLastModified {
        const TEST_LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;
        crate::domain::ObjectLastModified::try_from(TEST_LAST_MODIFIED_UNIX_MILLIS).unwrap()
    }
}
