use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prost::Message as ProstMessage;
use serde::Serialize;
use tokio::io::{
    stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout,
};
use tokio::sync::{mpsc, oneshot};

use so3_core::client::interface::{BlobPeerClient, ConsensusPeerClient};
use so3_core::domain::blob::id::BlobId;
use so3_core::domain::blob::payload::BlobPayload;
use so3_core::domain::consensus::transport::{
    AcceptRequest, AcceptResponse, ApplyRequest, ApplyResponse, CommitRequest, CommitResponse,
    PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
};
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::domain::node::NodeId;
use so3_core::proto::consensus::{
    AcceptRequest as ProtoAcceptRequest, AcceptResponse as ProtoAcceptResponse,
    CommitRequest as ProtoCommitRequest, CommitResponse as ProtoCommitResponse,
    PreAcceptRequest as ProtoPreAcceptRequest, PreAcceptResponse as ProtoPreAcceptResponse,
    RecoverRequest as ProtoRecoverRequest, RecoverResponse as ProtoRecoverResponse,
};
use so3_core::proto::mappers::{
    accept_req_to_domain, accept_req_to_proto, accept_res_to_domain, accept_res_to_proto,
    commit_req_to_domain, commit_req_to_proto, commit_res_to_domain, commit_res_to_proto,
    pre_accept_req_to_domain, pre_accept_req_to_proto, pre_accept_res_to_domain,
    pre_accept_res_to_proto, recover_req_to_domain, recover_req_to_proto, recover_res_to_domain,
    recover_res_to_proto,
};
use so3_core::repository::blob::fs::FileSystemBlobRepository;
use so3_core::repository::consensus_journal::sqlite::SqliteConsensusJournal;
use so3_core::repository::metadata::sqlite::SqliteObjectMetadataRepository;
use so3_core::service::consensus_coordinator::service::AccordConsensusCoordinatorService;
use so3_core::use_case::inbound_consensus::use_case::InboundConsensusUseCaseImpl;
use so3_core::use_case::inbound_consensus::InboundConsensusUseCase;
use so3_core::use_case::object::use_case::ObjectUseCaseImpl;

use crate::config::StorageRoots;
use crate::protocol::{
    error_response, reply, ClientRequest, ConsensusRpc, Message, RequestBody, ResponseBody,
    CRASH_CODE,
};
use crate::service::MaelstromService;

// Concrete repository types
type Journal = SqliteConsensusJournal;
type MetaRepo = SqliteObjectMetadataRepository;
type BlobRepo = FileSystemBlobRepository;

// Concrete use-case stack for maelstrom
type Coordinator = AccordConsensusCoordinatorService<
    Journal,
    MaelstromConsensusPeerClient,
    MetaRepo,
    BlobRepo,
    NoBlobPeerClient,
>;
type Handler = InboundConsensusUseCaseImpl<Journal, MetaRepo, BlobRepo, NoBlobPeerClient>;
type ObjectUC = ObjectUseCaseImpl<Coordinator, Journal, MetaRepo, BlobRepo, NoBlobPeerClient>;
type Service = MaelstromService<ObjectUC>;

// State shared between peer clients, the inbound handler, and the main runtime loop.
struct SharedState {
    node_id: String,
    output: mpsc::UnboundedSender<Vec<u8>>,
    pending_consensus: Mutex<HashMap<u64, oneshot::Sender<So3Result<Vec<u8>>>>>,
    next_msg_id: AtomicU64,
}

impl SharedState {
    fn next_msg_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::Relaxed)
    }
}

// Stub blob peer client — maelstrom nodes share a common blob directory,
// so blobs written locally are always visible without cross-node fetching.
struct NoBlobPeerClient;

#[async_trait]
impl BlobPeerClient for NoBlobPeerClient {
    async fn push(&self, _blob_id: BlobId, _payload: &BlobPayload) -> So3Result<()> {
        Err(So3Error::PeerUnavailable(
            "no blob distribution in maelstrom mode".into(),
        ))
    }

    async fn fetch(&self, blob_id: &BlobId) -> So3Result<BlobPayload> {
        Err(So3Error::NotFound(format!(
            "blob {blob_id} not available without peer client"
        )))
    }
}

// Consensus peer client that routes RPCs through the Maelstrom stdin/stdout protocol.
struct MaelstromConsensusPeerClient {
    peer_id: String,
    shared: Arc<SharedState>,
}

impl MaelstromConsensusPeerClient {
    async fn send_rpc(&self, rpc: ConsensusRpc, payload: Vec<u8>) -> So3Result<Vec<u8>> {
        let msg_id = self.shared.next_msg_id();
        let (tx, rx) = oneshot::channel();
        self.shared
            .pending_consensus
            .lock()
            .unwrap()
            .insert(msg_id, tx);

        let encoded = serde_json::to_vec(&Message {
            src: self.shared.node_id.clone(),
            dest: self.peer_id.clone(),
            body: RequestBody::Consensus {
                msg_id,
                rpc,
                payload,
            },
        })
        .map_err(|e| So3Error::Serialization(e.to_string()))?;

        self.shared
            .output
            .send(encoded)
            .map_err(|_| So3Error::PeerUnavailable("output channel closed".into()))?;

        rx.await
            .map_err(|_| So3Error::PeerUnavailable("consensus response channel dropped".into()))?
    }
}

#[async_trait]
impl ConsensusPeerClient for MaelstromConsensusPeerClient {
    async fn pre_accept(&self, req: PreAcceptRequest) -> So3Result<PreAcceptResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::PreAccept,
                pre_accept_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoPreAcceptResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        pre_accept_res_to_domain(proto)
    }

    async fn accept(&self, req: AcceptRequest) -> So3Result<AcceptResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Accept,
                accept_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoAcceptResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        accept_res_to_domain(proto)
    }

    async fn commit(&self, req: CommitRequest) -> So3Result<CommitResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Commit,
                commit_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoCommitResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        commit_res_to_domain(proto)
    }

    async fn apply(&self, _req: ApplyRequest) -> So3Result<ApplyResponse> {
        // Apply is driven locally after commit — no cross-node Apply RPC in maelstrom mode.
        Err(So3Error::PeerUnavailable(
            "apply not supported in maelstrom peer client".into(),
        ))
    }

    async fn recover(&self, req: RecoverRequest) -> So3Result<RecoverResponse> {
        let bytes = self
            .send_rpc(
                ConsensusRpc::Recover,
                recover_req_to_proto(req).encode_to_vec(),
            )
            .await?;
        let proto = ProtoRecoverResponse::decode(bytes.as_slice())
            .map_err(|e| So3Error::Serialization(e.to_string()))?;
        recover_res_to_domain(proto)
    }
}

struct SharedRuntime {
    node_ids: Vec<String>,
    service: Service,
    local_handler: Arc<Handler>,
    shared: Arc<SharedState>,
    pending_forwards: Mutex<HashMap<u64, oneshot::Sender<So3Result<ResponseBody>>>>,
}

impl SharedRuntime {
    fn is_coordinator(&self) -> bool {
        self.shared.node_id == self.coordinator_id()
    }

    fn coordinator_id(&self) -> &str {
        self.node_ids
            .first()
            .map_or(self.shared.node_id.as_str(), String::as_str)
    }

    fn send_message(&self, message: &Message<impl Serialize>) -> So3Result<()> {
        let encoded =
            serde_json::to_vec(message).map_err(|e| So3Error::Serialization(e.to_string()))?;
        self.shared
            .output
            .send(encoded)
            .map_err(|_| So3Error::InvalidRequest("output channel closed".into()))
    }
}

struct RuntimeComponents {
    service: Service,
    local_handler: Arc<Handler>,
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

    let node_id = node_id.clone();
    let node_ids = node_ids.clone();

    // Create the output channel before building components — peer clients need it.
    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let peer_ids: Vec<String> = node_ids
        .iter()
        .filter(|id| id.as_str() != node_id)
        .cloned()
        .collect();

    let shared = Arc::new(SharedState {
        node_id: node_id.clone(),
        output: output_tx,
        pending_consensus: Mutex::new(HashMap::new()),
        next_msg_id: AtomicU64::new(msg_id.saturating_add(1)),
    });

    let components =
        build_components(&storage_roots, &node_id, peer_ids, Arc::clone(&shared)).await?;

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

    let output_stdout = init_output.into_inner();

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

    let shared_runtime = Arc::new(SharedRuntime {
        node_ids,
        service: components.service,
        local_handler: components.local_handler,
        shared,
        pending_forwards: Mutex::new(HashMap::new()),
    });

    while let Some(message) = next_request_if_available(&mut input).await? {
        route_or_spawn(&shared_runtime, message);
    }

    drop(shared_runtime);
    let _ = writer.await;
    Ok(())
}

async fn build_components(
    storage_roots: &StorageRoots,
    node_id: &str,
    peer_ids: Vec<String>,
    shared: Arc<SharedState>,
) -> So3Result<RuntimeComponents> {
    let node_dir = storage_roots.metadata_dir.join(node_id);
    // All nodes share one blob directory so blobs are always locally accessible
    // regardless of which coordinator originally stored them.
    let blob_dir = &storage_roots.blob_dir;

    let journal = Arc::new(SqliteConsensusJournal::new(&node_dir).await?);
    let meta = Arc::new(SqliteObjectMetadataRepository::new(&node_dir).await?);
    let blobs = Arc::new(FileSystemBlobRepository::new(blob_dir).await?);

    let peer_clients: HashMap<NodeId, Arc<MaelstromConsensusPeerClient>> = peer_ids
        .iter()
        .map(|id| {
            (
                NodeId::new(id.clone()),
                Arc::new(MaelstromConsensusPeerClient {
                    peer_id: id.clone(),
                    shared: Arc::clone(&shared),
                }),
            )
        })
        .collect();

    let coordinator = AccordConsensusCoordinatorService::new(
        NodeId::new(node_id.to_owned()),
        0,
        0,
        peer_clients,
        Arc::clone(&journal),
        Arc::clone(&meta),
        Arc::clone(&blobs),
        HashMap::<NodeId, Arc<NoBlobPeerClient>>::new(),
    )
    .await?;

    let local_handler = Arc::new(InboundConsensusUseCaseImpl::new(
        NodeId::new(node_id.to_owned()),
        0,
        Arc::clone(&journal),
        Arc::clone(&meta),
        Arc::clone(&blobs),
        HashMap::<NodeId, Arc<NoBlobPeerClient>>::new(),
    ));

    let object_uc = ObjectUseCaseImpl::new(
        coordinator,
        Arc::clone(&journal),
        Arc::clone(&meta),
        Arc::clone(&blobs),
        HashMap::<NodeId, Arc<NoBlobPeerClient>>::new(),
    );

    Ok(RuntimeComponents {
        service: MaelstromService::new(object_uc),
        local_handler,
    })
}

fn route_or_spawn(shared: &Arc<SharedRuntime>, message: Message<RequestBody>) {
    match &message.body {
        RequestBody::ConsensusOk {
            in_reply_to,
            payload,
        } => {
            if let Some(tx) = shared
                .shared
                .pending_consensus
                .lock()
                .unwrap()
                .remove(in_reply_to)
            {
                let _ = tx.send(Ok(payload.clone()));
            }
        }
        RequestBody::ForwardOk {
            in_reply_to,
            response,
        } => {
            if let Some(tx) = shared.pending_forwards.lock().unwrap().remove(in_reply_to) {
                let _ = tx.send(Ok(response.clone()));
            }
        }
        RequestBody::Error {
            in_reply_to, text, ..
        } => {
            let err = So3Error::InvalidRequest(text.clone());
            let consensus_tx = shared
                .shared
                .pending_consensus
                .lock()
                .unwrap()
                .remove(in_reply_to);
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
            src: shared.shared.node_id.clone(),
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
    if shared.is_coordinator() {
        let response = execute_leader_command(&shared, msg_id, request).await;
        shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: client,
            body: response,
        })
    } else {
        let forward_msg_id = shared.shared.next_msg_id();
        let (tx, rx) = oneshot::channel();
        shared
            .pending_forwards
            .lock()
            .unwrap()
            .insert(forward_msg_id, tx);
        shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: shared.coordinator_id().to_owned(),
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
            src: shared.shared.node_id.clone(),
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
    if !shared.is_coordinator() {
        return shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
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
        src: shared.shared.node_id.clone(),
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
        ConsensusRpc::PreAccept => {
            match ProtoPreAcceptRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(pre_accept_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .pre_accept(req)
                    .await
                    .map(|r| pre_accept_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Accept => {
            match ProtoAcceptRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(accept_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .accept(req)
                    .await
                    .map(|r| accept_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Commit => {
            match ProtoCommitRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(commit_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .commit(req)
                    .await
                    .map(|r| commit_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
        ConsensusRpc::Recover => {
            match ProtoRecoverRequest::decode(payload.as_slice())
                .map_err(|e| So3Error::Serialization(e.to_string()))
                .and_then(recover_req_to_domain)
            {
                Ok(req) => shared
                    .local_handler
                    .recover(req)
                    .await
                    .map(|r| recover_res_to_proto(r).encode_to_vec()),
                Err(e) => Err(e),
            }
        }
    };

    match result {
        Ok(response_payload) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::ConsensusOk {
                in_reply_to: msg_id,
                payload: response_payload,
            },
        }),
        Err(error) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
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
    match request {
        ClientRequest::Read { key } => shared.service.handle_read(msg_id, key).await,
        ClientRequest::Write { key, value } => {
            shared.service.handle_write(msg_id, key, value).await
        }
        ClientRequest::Cas {
            key,
            from,
            to,
            create_if_not_exists,
        } => {
            shared
                .service
                .handle_cas(msg_id, key, from, to, create_if_not_exists)
                .await
        }
    }
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
