use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;

use crate::domain::error::{So3Error, So3Result};
use crate::rpc_server::proto::consensus_transport_server::ConsensusTransportServer;
use crate::rpc_server::service::ConsensusTransportService;
use crate::rpc_server::transport::{ConsensusTransportHandler, RejectingConsensusTransport};
use uuid::Uuid;

pub struct RpcServer<H: ConsensusTransportHandler> {
    handler: H,
}

impl Default for RpcServer<RejectingConsensusTransport> {
    fn default() -> Self {
        Self::new(RejectingConsensusTransport::new(Uuid::nil()))
    }
}

impl<H: ConsensusTransportHandler> RpcServer<H> {
    #[must_use]
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

impl<H> RpcServer<H>
where
    H: ConsensusTransportHandler + Clone + Send + Sync + 'static,
{
    /// # Errors
    ///
    /// Returns an error if the gRPC server fails while serving requests.
    pub async fn run(
        self,
        listener: TcpListener,
        cancellation_token: CancellationToken,
    ) -> So3Result<()> {
        let local_addr = listener.local_addr().map_err(So3Error::from)?;
        info!(%local_addr, "rpc server started");
        let mut server = Server::builder();

        server
            .add_service(ConsensusTransportServer::new(
                ConsensusTransportService::new(self.handler),
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                cancellation_token.cancelled().await;
            })
            .await
            .map_err(|error| So3Error::Io(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::spawn;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::Channel;
    use tonic::{Code, Request};
    use uuid::Uuid;

    use super::RpcServer;
    use crate::consensus::state_machine::LocalStateMachine;
    use crate::domain::{ObjectCommand, ObjectKey, ObjectResult, ReadCommand, WriteCommand};
    use crate::rpc_server::proto::PreAcceptRequest;
    use crate::rpc_server::proto::consensus_transport_client::ConsensusTransportClient;
    use crate::rpc_server::proto::{ApplyRequest, CommitRequest, EventPayload};
    use crate::rpc_server::transport::{ApplyingConsensusTransport, RejectingConsensusTransport};
    use crate::storage::object::persistent::SqliteFsObjectRepository;

    const CONNECT_RETRY_ATTEMPTS: usize = 20;
    const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(25);
    const LOOPBACK_EPHEMERAL_ADDR: &str = "127.0.0.1:0";
    const ALPHA_KEY: &str = "alpha";
    const FIRST_VALUE: &[u8] = b"first";

    async fn connect_with_retry(endpoint: String) -> Channel {
        let mut last_error = None;

        for _ in 0..CONNECT_RETRY_ATTEMPTS {
            match Channel::from_shared(endpoint.clone())
                .unwrap()
                .connect()
                .await
            {
                Ok(channel) => return channel,
                Err(error) => {
                    last_error = Some(error);
                    sleep(CONNECT_RETRY_DELAY).await;
                }
            }
        }

        panic!("failed to connect to rpc server: {last_error:?}");
    }

    #[tokio::test]
    async fn rpc_server_exposes_consensus_transport_service() {
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();

        let server_task = spawn(async move {
            RpcServer::new(RejectingConsensusTransport::new(Uuid::nil()))
                .run(listener, shutdown_token)
                .await
        });
        let channel = connect_with_retry(format!("http://{local_addr}")).await;
        let mut client = ConsensusTransportClient::new(channel);

        let response = client
            .pre_accept(Request::new(PreAcceptRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.timestamp, None);
        assert!(response.nack);
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rpc_server_returns_failed_precondition_for_commit() {
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();

        let server_task = spawn(async move {
            RpcServer::new(RejectingConsensusTransport::new(Uuid::nil()))
                .run(listener, shutdown_token)
                .await
        });
        let channel = connect_with_retry(format!("http://{local_addr}")).await;
        let mut client = ConsensusTransportClient::new(channel);

        let error = client
            .commit(Request::new(CommitRequest::default()))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::FailedPrecondition);
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rpc_server_apply_executes_serialized_object_command() {
        let temp_dir = TempDir::new().unwrap();
        let repository = SqliteFsObjectRepository::new(
            temp_dir.path().join("metadata"),
            temp_dir.path().join("blobs"),
        )
        .await
        .unwrap();
        let listener = TcpListener::bind(LOOPBACK_EPHEMERAL_ADDR).await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let cancellation_token = CancellationToken::new();
        let shutdown_token = cancellation_token.clone();
        let state_machine = LocalStateMachine::new(repository);

        let server_task = spawn(async move {
            RpcServer::new(ApplyingConsensusTransport::new(Uuid::nil(), state_machine))
                .run(listener, shutdown_token)
                .await
        });
        let channel = connect_with_retry(format!("http://{local_addr}")).await;
        let mut client = ConsensusTransportClient::new(channel);
        let write = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
            value: FIRST_VALUE.to_vec(),
        });
        let read = ObjectCommand::Read(ReadCommand {
            key: ObjectKey::new(ALPHA_KEY).unwrap(),
        });

        let _ = client
            .apply(Request::new(ApplyRequest {
                event: Some(EventPayload {
                    command: write.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            }))
            .await
            .unwrap();
        let response = client
            .apply(Request::new(ApplyRequest {
                event: Some(EventPayload {
                    command: read.to_bytes().unwrap(),
                }),
                ..ApplyRequest::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let result = ObjectResult::from_bytes(&response.result).unwrap();
        let ObjectResult::Read(read) = result else {
            panic!("expected read result");
        };

        assert_eq!(read.object.unwrap().value, FIRST_VALUE.to_vec());
        cancellation_token.cancel();
        server_task.await.unwrap().unwrap();
    }
}
