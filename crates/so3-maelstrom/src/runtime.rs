use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prost::Message as ProstMessage;
use serde::Serialize;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout, stdin, stdout,
};
use tokio::sync::{mpsc, oneshot};

use so3_core::consensus::ConsensusCommandId;
use so3_core::consensus::coordinator::{
    AccordCoordinator, AccordCoordinatorConfig, ConsensusPeerTransport,
};
use so3_core::consensus::executor::PersistentReplicatedCommandExecutor;
use so3_core::consensus::recovery::replay_committed_commands;
use so3_core::consensus::state_machine::LocalStateMachine;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::object_server::service::ObjectService;
use so3_core::rpc_server::proto::{
    AcceptRequest, AcceptResponse, CommitRequest, CommitResponse, PreAcceptRequest,
    PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use so3_core::rpc_server::transport::{ApplyingConsensusTransport, ConsensusTransportHandler};
use so3_core::storage::metadata::sqlite::SqliteObjectMetadataRepository;
use so3_core::storage::registry::{PersistentStorage, SqliteFsPersistentObjectRepository};

use crate::config::StorageRoots;
use crate::protocol::{
    CRASH_CODE, ClientRequest, ConsensusRpc, Message, RequestBody, ResponseBody, error_response,
    reply,
};
use crate::service::MaelstromService;

type MaelstromObjectService =
    MaelstromService<LocalStateMachine<SqliteFsPersistentObjectRepository>>;
type MaelstromLocalTransport = ApplyingConsensusTransport<
    PersistentReplicatedCommandExecutor<
        SqliteFsPersistentObjectRepository,
        SqliteObjectMetadataRepository,
    >,
>;

struct SharedRuntime {
    node_id: String,
    node_ids: Vec<String>,
    service: MaelstromObjectService,
    local_transport: MaelstromLocalTransport,
    output: mpsc::UnboundedSender<Vec<u8>>,
    pending_consensus: Mutex<HashMap<u64, oneshot::Sender<So3Result<Vec<u8>>>>>,
    pending_forwards: Mutex<HashMap<u64, oneshot::Sender<So3Result<ResponseBody>>>>,
    next_msg_id: AtomicU64,
    next_command_sequence: AtomicU64,
}

impl SharedRuntime {
    fn next_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    fn next_command_sequence(&self) -> u64 {
        self.next_command_sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn is_leader(&self) -> bool {
        self.node_id == self.leader_id()
    }

    fn leader_id(&self) -> &str {
        self.node_ids
            .first()
            .map_or(self.node_id.as_str(), String::as_str)
    }

    fn follower_ids(&self) -> Vec<String> {
        self.node_ids
            .iter()
            .filter(|id| id.as_str() != self.node_id)
            .cloned()
            .collect()
    }

    fn send_message(&self, message: &Message<impl Serialize>) -> So3Result<()> {
        let encoded = serde_json::to_vec(message)
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        self.output
            .send(encoded)
            .map_err(|_| So3Error::InvalidRequest("output channel closed".into()))
    }
}

struct MaelstromPeerTransport {
    shared: Arc<SharedRuntime>,
}

#[async_trait]
impl ConsensusPeerTransport for MaelstromPeerTransport {
    async fn pre_accept_peer(
        &mut self,
        peer_id: &str,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        let payload =
            send_consensus_rpc(&self.shared, peer_id, ConsensusRpc::PreAccept, request).await?;
        decode_proto::<PreAcceptResponse>(&payload)
    }

    async fn accept_peer(
        &mut self,
        peer_id: &str,
        request: AcceptRequest,
    ) -> So3Result<AcceptResponse> {
        let payload =
            send_consensus_rpc(&self.shared, peer_id, ConsensusRpc::Accept, request).await?;
        decode_proto::<AcceptResponse>(&payload)
    }

    async fn commit_peer(
        &mut self,
        peer_id: &str,
        request: CommitRequest,
    ) -> So3Result<CommitResponse> {
        let payload =
            send_consensus_rpc(&self.shared, peer_id, ConsensusRpc::Commit, request).await?;
        decode_proto::<CommitResponse>(&payload)
    }

    async fn recover_peer(
        &mut self,
        peer_id: &str,
        request: RecoverRequest,
    ) -> So3Result<RecoverResponse> {
        let payload =
            send_consensus_rpc(&self.shared, peer_id, ConsensusRpc::Recover, request).await?;
        decode_proto::<RecoverResponse>(&payload)
    }
}

async fn send_consensus_rpc<T: ProstMessage>(
    shared: &SharedRuntime,
    peer_id: &str,
    rpc: ConsensusRpc,
    request: T,
) -> So3Result<Vec<u8>> {
    let msg_id = shared.next_msg_id();
    let (tx, rx) = oneshot::channel();
    shared.pending_consensus.lock().unwrap().insert(msg_id, tx);
    shared.send_message(&Message {
        src: shared.node_id.clone(),
        dest: peer_id.to_owned(),
        body: RequestBody::Consensus {
            msg_id,
            rpc,
            payload: request.encode_to_vec(),
        },
    })?;
    rx.await
        .map_err(|_| So3Error::InvalidRequest("consensus response channel dropped".into()))?
}

struct RuntimeComponents {
    service: MaelstromObjectService,
    local_transport: MaelstromLocalTransport,
    next_command_sequence: u64,
}

pub async fn run(storage_roots: StorageRoots) -> So3Result<()> {
    let mut input = BufReader::new(stdin()).lines();
    let mut init_output = BufWriter::new(stdout());

    let init_request = next_request(&mut input).await?;
    let RequestBody::Init {
        msg_id,
        node_id,
        node_ids,
    } = &init_request.body
    else {
        return Err(So3Error::InvalidRequest(
            "first maelstrom message must be init".to_owned(),
        ));
    };

    let components = build_components(&storage_roots, node_id).await?;
    write_message(
        &mut init_output,
        &reply(
            &init_request,
            ResponseBody::InitOk {
                in_reply_to: *msg_id,
            },
        ),
    )
    .await?;

    // The init_output BufWriter is flushed; transfer stdout ownership to the writer task.
    let output_stdout = init_output.into_inner();

    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        let mut w = BufWriter::new(output_stdout);
        while let Some(bytes) = output_rx.recv().await {
            if w.write_all(&bytes).await.is_err()
                || w.write_u8(b'\n').await.is_err()
                || w.flush().await.is_err()
            {
                break;
            }
        }
    });

    let shared = Arc::new(SharedRuntime {
        node_id: node_id.clone(),
        node_ids: node_ids.clone(),
        service: components.service,
        local_transport: components.local_transport,
        output: output_tx,
        pending_consensus: Mutex::new(HashMap::new()),
        pending_forwards: Mutex::new(HashMap::new()),
        next_msg_id: AtomicU64::new(msg_id.saturating_add(1)),
        next_command_sequence: AtomicU64::new(components.next_command_sequence),
    });

    while let Some(message) = next_request_if_available(&mut input).await? {
        route_or_spawn(&shared, message);
    }

    drop(shared);
    let _ = writer.await;
    Ok(())
}

fn route_or_spawn(shared: &Arc<SharedRuntime>, message: Message<RequestBody>) {
    match &message.body {
        RequestBody::ConsensusOk { in_reply_to, payload } => {
            if let Some(tx) = shared.pending_consensus.lock().unwrap().remove(in_reply_to) {
                let _ = tx.send(Ok(payload.clone()));
            }
        }
        RequestBody::ForwardOk { in_reply_to, response } => {
            if let Some(tx) = shared.pending_forwards.lock().unwrap().remove(in_reply_to) {
                let _ = tx.send(Ok(response.clone()));
            }
        }
        RequestBody::Error { in_reply_to, text, .. } => {
            let err = So3Error::InvalidRequest(text.clone());
            let consensus_tx = shared.pending_consensus.lock().unwrap().remove(in_reply_to);
            if let Some(tx) = consensus_tx {
                let _ = tx.send(Err(err));
            } else if let Some(tx) = shared.pending_forwards.lock().unwrap().remove(in_reply_to) {
                let _ = tx.send(Err(err));
            }
        }
        _ => {
            let shared = Arc::clone(shared);
            tokio::spawn(async move {
                if let Err(error) = handle_message(shared, message).await {
                    tracing::error!(%error, "error handling message");
                }
            });
        }
    }
}

async fn handle_message(
    shared: Arc<SharedRuntime>,
    message: Message<RequestBody>,
) -> So3Result<()> {
    let src = message.src;
    match message.body {
        RequestBody::Init { msg_id, .. } => shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: src,
            body: error_response(msg_id, CRASH_CODE, "duplicate init request"),
        }),
        RequestBody::Read { msg_id, key } => {
            handle_client_message(shared, src, msg_id, ClientRequest::Read { key }).await
        }
        RequestBody::Write { msg_id, key, value } => {
            handle_client_message(shared, src, msg_id, ClientRequest::Write { key, value }).await
        }
        RequestBody::Cas {
            msg_id,
            key,
            from,
            to,
            create_if_not_exists,
        } => {
            handle_client_message(
                shared,
                src,
                msg_id,
                ClientRequest::Cas {
                    key,
                    from,
                    to,
                    create_if_not_exists,
                },
            )
            .await
        }
        RequestBody::Forward {
            msg_id,
            client_msg_id,
            request,
        } => handle_forward(shared, src, msg_id, client_msg_id, request).await,
        RequestBody::Consensus {
            msg_id,
            rpc,
            payload,
        } => handle_consensus(shared, src, msg_id, rpc, payload).await,
        RequestBody::ForwardOk { .. }
        | RequestBody::ConsensusOk { .. }
        | RequestBody::Error { .. } => Ok(()),
    }
}

async fn handle_client_message(
    shared: Arc<SharedRuntime>,
    client: String,
    msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    if shared.is_leader() {
        let response = execute_leader_command(&shared, msg_id, request).await;
        shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: client,
            body: response,
        })
    } else {
        let forward_msg_id = shared.next_msg_id();
        let (tx, rx) = oneshot::channel();
        shared
            .pending_forwards
            .lock()
            .unwrap()
            .insert(forward_msg_id, tx);
        shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: shared.leader_id().to_owned(),
            body: RequestBody::Forward {
                msg_id: forward_msg_id,
                client_msg_id: msg_id,
                request,
            },
        })?;
        let response = rx
            .await
            .map_err(|_| So3Error::InvalidRequest("forward response channel dropped".into()))??;
        shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: client,
            body: response,
        })
    }
}

async fn handle_forward(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    client_msg_id: u64,
    request: ClientRequest,
) -> So3Result<()> {
    if !shared.is_leader() {
        return shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: sender,
            body: RequestBody::Error {
                in_reply_to: msg_id,
                code: CRASH_CODE,
                text: "only the leader can handle forwarded client requests".to_owned(),
            },
        });
    }

    let response = execute_leader_command(&shared, client_msg_id, request).await;
    shared.send_message(&Message {
        src: shared.node_id.clone(),
        dest: sender,
        body: RequestBody::ForwardOk {
            in_reply_to: msg_id,
            response,
        },
    })
}

async fn handle_consensus(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    rpc: ConsensusRpc,
    payload: Vec<u8>,
) -> So3Result<()> {
    let result: So3Result<Vec<u8>> = match rpc {
        ConsensusRpc::PreAccept => match decode_proto::<PreAcceptRequest>(&payload) {
            Ok(req) => shared
                .local_transport
                .pre_accept(req)
                .await
                .map(|r| r.encode_to_vec())
                .map_err(status_error),
            Err(e) => Err(e),
        },
        ConsensusRpc::Accept => match decode_proto::<AcceptRequest>(&payload) {
            Ok(req) => shared
                .local_transport
                .accept(req)
                .await
                .map(|r| r.encode_to_vec())
                .map_err(status_error),
            Err(e) => Err(e),
        },
        ConsensusRpc::Commit => match decode_proto::<CommitRequest>(&payload) {
            Ok(req) => shared
                .local_transport
                .commit(req)
                .await
                .map(|r| r.encode_to_vec())
                .map_err(status_error),
            Err(e) => Err(e),
        },
        ConsensusRpc::Recover => match decode_proto::<RecoverRequest>(&payload) {
            Ok(req) => shared
                .local_transport
                .recover(req)
                .await
                .map(|r| r.encode_to_vec())
                .map_err(status_error),
            Err(e) => Err(e),
        },
    };

    match result {
        Ok(response_payload) => shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: sender,
            body: RequestBody::ConsensusOk {
                in_reply_to: msg_id,
                payload: response_payload,
            },
        }),
        Err(error) => shared.send_message(&Message {
            src: shared.node_id.clone(),
            dest: sender,
            body: RequestBody::Error {
                in_reply_to: msg_id,
                code: CRASH_CODE,
                text: error.to_string(),
            },
        }),
    }
}

async fn execute_leader_command(
    shared: &Arc<SharedRuntime>,
    msg_id: u64,
    request: ClientRequest,
) -> ResponseBody {
    let command = match shared.service.prepare_command(msg_id, request).await {
        Ok(command) => command,
        Err(response) => return response,
    };
    let command_id =
        ConsensusCommandId::new(shared.node_id.clone(), shared.next_command_sequence());
    let config = AccordCoordinatorConfig {
        node_id: shared.node_id.clone(),
        peer_ids: shared.follower_ids(),
    };
    let local_transport = shared.local_transport.clone();
    let mut peer_transport = MaelstromPeerTransport {
        shared: Arc::clone(shared),
    };
    let mut coordinator = AccordCoordinator::new(config, &local_transport, &mut peer_transport);

    match coordinator.execute(&command_id, command).await {
        Ok(result) => MaelstromObjectService::response_from_result(msg_id, result),
        Err(error) => map_internal_error(msg_id, &error),
    }
}

async fn build_components(
    storage_roots: &StorageRoots,
    node_id: &str,
) -> So3Result<RuntimeComponents> {
    let storage = PersistentStorage::open(
        node_storage_dir(&storage_roots.metadata_dir, node_id),
        node_storage_dir(&storage_roots.blob_dir, node_id),
    )
    .await?;
    let executor = PersistentReplicatedCommandExecutor::new(
        storage.object_repository.clone(),
        storage.metadata_repository.clone(),
    );
    replay_committed_commands(&storage.consensus_journal, &executor).await?;
    let next_command_sequence = storage
        .consensus_journal
        .next_sequence_for_origin(node_id)
        .await?;
    let service = MaelstromService::new(ObjectService::new(LocalStateMachine::new(
        storage.object_repository,
    )));
    let local_transport =
        ApplyingConsensusTransport::new(node_id.to_owned(), executor, storage.consensus_journal);

    Ok(RuntimeComponents {
        service,
        local_transport,
        next_command_sequence,
    })
}

fn node_storage_dir(root: &Path, node_id: &str) -> PathBuf {
    root.join(node_id)
}

async fn next_request(lines: &mut Lines<BufReader<Stdin>>) -> So3Result<Message<RequestBody>> {
    next_request_if_available(lines)
        .await?
        .ok_or_else(|| So3Error::InvalidRequest("maelstrom stdin closed before init".to_owned()))
}

async fn next_request_if_available(
    lines: &mut Lines<BufReader<Stdin>>,
) -> So3Result<Option<Message<RequestBody>>> {
    let Some(line) = lines.next_line().await? else {
        return Ok(None);
    };

    serde_json::from_str(&line).map(Some).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to decode maelstrom request: {error}"))
    })
}

async fn write_message(
    output: &mut BufWriter<Stdout>,
    message: &Message<impl Serialize>,
) -> So3Result<()> {
    let encoded =
        serde_json::to_vec(message).map_err(|error| So3Error::Serialization(error.to_string()))?;
    output.write_all(&encoded).await?;
    output.write_u8(b'\n').await?;
    output.flush().await?;
    Ok(())
}

fn map_internal_error(in_reply_to: u64, error: &So3Error) -> ResponseBody {
    error_response(in_reply_to, CRASH_CODE, error.to_string())
}

fn decode_proto<T: Default + ProstMessage>(payload: &[u8]) -> So3Result<T> {
    T::decode(payload).map_err(|error| So3Error::Serialization(error.to_string()))
}

fn status_error(status: impl std::fmt::Display) -> So3Error {
    So3Error::InvalidRequest(status.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::{build_components, node_storage_dir};
    use crate::config::StorageRoots;
    use crate::protocol::{ClientRequest, ResponseBody};
    use so3_core::consensus::ConsensusCommandId;
    use so3_core::domain::{ObjectCommand, ObjectKey, ObjectLastModified, WriteCommand};
    use so3_core::storage::registry::PersistentStorage;

    const NODE_ID: &str = "n0";
    const KEY_ALPHA: &str = "alpha";
    const COMMAND_SEQUENCE_SEVEN: u64 = 7;
    const MESSAGE_ID: u64 = 1;
    const LAST_MODIFIED_UNIX_MILLIS: i64 = 1_775_000_000_123;

    #[tokio::test]
    async fn build_components_replays_committed_commands_and_recovers_next_sequence() {
        let temp_dir = TempDir::new().unwrap();
        let storage_roots = StorageRoots {
            metadata_dir: temp_dir.path().join("metadata"),
            blob_dir: temp_dir.path().join("blobs"),
        };
        let storage = PersistentStorage::open(
            node_storage_dir(&storage_roots.metadata_dir, NODE_ID),
            node_storage_dir(&storage_roots.blob_dir, NODE_ID),
        )
        .await
        .unwrap();
        let command = ObjectCommand::Write(WriteCommand {
            key: ObjectKey::new(serde_json::to_string(KEY_ALPHA).unwrap()).unwrap(),
            value: serde_json::to_vec(&json!(42)).unwrap(),
            last_modified: ObjectLastModified::try_from(LAST_MODIFIED_UNIX_MILLIS).unwrap(),
        });
        storage
            .consensus_journal
            .record_committed(
                &ConsensusCommandId::new(NODE_ID.to_owned(), COMMAND_SEQUENCE_SEVEN),
                &command.to_bytes().unwrap(),
            )
            .await
            .unwrap();

        let components = build_components(&storage_roots, NODE_ID).await.unwrap();

        assert_eq!(components.next_command_sequence, COMMAND_SEQUENCE_SEVEN + 1);
        let response = components
            .service
            .handle_client(
                MESSAGE_ID,
                ClientRequest::Read {
                    key: json!(KEY_ALPHA),
                },
            )
            .await;
        assert_eq!(
            response,
            ResponseBody::ReadOk {
                in_reply_to: MESSAGE_ID,
                value: json!(42),
            }
        );
    }
}
