use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use prost::Message as ProstMessage;
use serde::Serialize;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout, stdin, stdout,
};

use so3_core::consensus::ConsensusCommandId;
use so3_core::consensus::coordinator::{
    AccordCoordinator, AccordCoordinatorConfig, ConsensusPeerTransport,
};
use so3_core::consensus::executor::PersistentReplicatedCommandExecutor;
use so3_core::consensus::state_machine::LocalStateMachine;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::object_server::service::ObjectService;
use so3_core::rpc_server::proto::{
    AcceptRequest, AcceptResponse, CommitRequest, CommitResponse, PreAcceptRequest,
    PreAcceptResponse,
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

pub async fn run(storage_roots: StorageRoots) -> So3Result<()> {
    let mut input = BufReader::new(stdin()).lines();
    let mut output = BufWriter::new(stdout());

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
        &mut output,
        &reply(
            &init_request,
            ResponseBody::InitOk {
                in_reply_to: *msg_id,
            },
        ),
    )
    .await?;

    Runtime {
        input,
        output,
        service: components.service,
        local_transport: components.local_transport,
        node_id: node_id.clone(),
        node_ids: node_ids.clone(),
        next_msg_id: msg_id.saturating_add(1),
        next_command_sequence: 1,
        forward_responses: HashMap::new(),
        consensus_responses: HashMap::new(),
        internal_errors: HashMap::new(),
    }
    .run()
    .await
}

struct Runtime {
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>,
    service: MaelstromObjectService,
    local_transport: MaelstromLocalTransport,
    node_id: String,
    node_ids: Vec<String>,
    next_msg_id: u64,
    next_command_sequence: u64,
    forward_responses: HashMap<u64, ResponseBody>,
    consensus_responses: HashMap<u64, Vec<u8>>,
    internal_errors: HashMap<u64, ResponseBody>,
}

struct RuntimeComponents {
    service: MaelstromObjectService,
    local_transport: MaelstromLocalTransport,
}

impl Runtime {
    async fn run(&mut self) -> So3Result<()> {
        while let Some(request) = self.next_request_if_available().await? {
            self.dispatch(request).await?;
        }

        Ok(())
    }

    fn dispatch(
        &mut self,
        message: Message<RequestBody>,
    ) -> Pin<Box<dyn Future<Output = So3Result<()>> + '_>> {
        Box::pin(async move {
            let src = message.src;
            match message.body {
                RequestBody::Init { msg_id, .. } => {
                    let response = error_response(msg_id, CRASH_CODE, "duplicate init request");
                    self.write_message(&Message {
                        src: self.node_id.clone(),
                        dest: src,
                        body: response,
                    })
                    .await
                }
                RequestBody::Read { msg_id, key } => {
                    self.handle_client_message(&src, msg_id, ClientRequest::Read { key })
                        .await
                }
                RequestBody::Write { msg_id, key, value } => {
                    self.handle_client_message(&src, msg_id, ClientRequest::Write { key, value })
                        .await
                }
                RequestBody::Cas {
                    msg_id,
                    key,
                    from,
                    to,
                    create_if_not_exists,
                } => {
                    self.handle_client_message(
                        &src,
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
                } => {
                    self.handle_forward(&src, msg_id, client_msg_id, request)
                        .await
                }
                RequestBody::Replicate { msg_id, request } => {
                    self.handle_replicate(&src, msg_id, request).await
                }
                RequestBody::Consensus {
                    msg_id,
                    rpc,
                    payload,
                } => self.handle_consensus(&src, msg_id, rpc, &payload).await,
                RequestBody::ForwardOk {
                    in_reply_to,
                    response,
                } => {
                    self.forward_responses.insert(in_reply_to, response);
                    Ok(())
                }
                RequestBody::ReplicateOk { in_reply_to } => {
                    self.internal_errors.insert(
                        in_reply_to,
                        ResponseBody::Error {
                            in_reply_to,
                            code: CRASH_CODE,
                            text: "legacy replicate response is not expected".to_owned(),
                        },
                    );
                    Ok(())
                }
                RequestBody::ConsensusOk {
                    in_reply_to,
                    payload,
                } => {
                    self.consensus_responses.insert(in_reply_to, payload);
                    Ok(())
                }
                RequestBody::Error {
                    in_reply_to,
                    code,
                    text,
                } => {
                    self.internal_errors.insert(
                        in_reply_to,
                        ResponseBody::Error {
                            in_reply_to,
                            code,
                            text,
                        },
                    );
                    Ok(())
                }
            }
        })
    }

    async fn handle_client_message(
        &mut self,
        client: &str,
        msg_id: u64,
        request: ClientRequest,
    ) -> So3Result<()> {
        if self.is_leader() {
            let response = self.execute_leader_command(msg_id, request).await;
            self.write_message(&Message {
                src: self.node_id.clone(),
                dest: client.to_owned(),
                body: response,
            })
            .await
        } else {
            let forward_msg_id = self.next_msg_id();
            self.write_message(&Message {
                src: self.node_id.clone(),
                dest: self.leader_id().to_owned(),
                body: RequestBody::Forward {
                    msg_id: forward_msg_id,
                    client_msg_id: msg_id,
                    request,
                },
            })
            .await?;
            let response = self.wait_for_forward_response(forward_msg_id).await?;
            self.write_message(&Message {
                src: self.node_id.clone(),
                dest: client.to_owned(),
                body: response,
            })
            .await
        }
    }

    async fn handle_forward(
        &mut self,
        sender: &str,
        msg_id: u64,
        client_msg_id: u64,
        request: ClientRequest,
    ) -> So3Result<()> {
        if !self.is_leader() {
            return self
                .write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::Error {
                        in_reply_to: msg_id,
                        code: CRASH_CODE,
                        text: "only the leader can handle forwarded client requests".to_owned(),
                    },
                })
                .await;
        }

        let response = self.execute_leader_command(client_msg_id, request).await;
        self.write_message(&Message {
            src: self.node_id.clone(),
            dest: sender.to_owned(),
            body: RequestBody::ForwardOk {
                in_reply_to: msg_id,
                response,
            },
        })
        .await
    }

    async fn handle_replicate(
        &mut self,
        sender: &str,
        msg_id: u64,
        request: ClientRequest,
    ) -> So3Result<()> {
        let response = self.service.handle_client(msg_id, request).await;
        match response {
            ResponseBody::WriteOk { .. } | ResponseBody::CasOk { .. } => {
                self.write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::ReplicateOk {
                        in_reply_to: msg_id,
                    },
                })
                .await
            }
            ResponseBody::Error { code, text, .. } => {
                self.write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::Error {
                        in_reply_to: msg_id,
                        code,
                        text,
                    },
                })
                .await
            }
            _ => {
                self.write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::Error {
                        in_reply_to: msg_id,
                        code: CRASH_CODE,
                        text: "replication request produced an unexpected response".to_owned(),
                    },
                })
                .await
            }
        }
    }

    async fn handle_consensus(
        &mut self,
        sender: &str,
        msg_id: u64,
        rpc: ConsensusRpc,
        payload: &[u8],
    ) -> So3Result<()> {
        let result = match rpc {
            ConsensusRpc::PreAccept => {
                let request = decode_proto::<PreAcceptRequest>(payload);
                match request {
                    Ok(request) => self
                        .local_transport
                        .pre_accept(request)
                        .await
                        .map(|response| response.encode_to_vec())
                        .map_err(status_error),
                    Err(error) => Err(error),
                }
            }
            ConsensusRpc::Accept => {
                let request = decode_proto::<AcceptRequest>(payload);
                match request {
                    Ok(request) => self
                        .local_transport
                        .accept(request)
                        .await
                        .map(|response| response.encode_to_vec())
                        .map_err(status_error),
                    Err(error) => Err(error),
                }
            }
            ConsensusRpc::Commit => {
                let request = decode_proto::<CommitRequest>(payload);
                match request {
                    Ok(request) => self
                        .local_transport
                        .commit(request)
                        .await
                        .map(|response| response.encode_to_vec())
                        .map_err(status_error),
                    Err(error) => Err(error),
                }
            }
        };

        match result {
            Ok(payload) => {
                self.write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::ConsensusOk {
                        in_reply_to: msg_id,
                        payload,
                    },
                })
                .await
            }
            Err(error) => {
                self.write_message(&Message {
                    src: self.node_id.clone(),
                    dest: sender.to_owned(),
                    body: RequestBody::Error {
                        in_reply_to: msg_id,
                        code: CRASH_CODE,
                        text: error.to_string(),
                    },
                })
                .await
            }
        }
    }

    async fn execute_leader_command(
        &mut self,
        msg_id: u64,
        request: ClientRequest,
    ) -> ResponseBody {
        let command = match self.service.prepare_command(msg_id, request).await {
            Ok(command) => command,
            Err(response) => return response,
        };
        let command_id =
            ConsensusCommandId::new(self.node_id.clone(), self.next_command_sequence());
        let config = AccordCoordinatorConfig {
            node_id: self.node_id.clone(),
            peer_ids: self.follower_ids(),
        };
        let local_transport = self.local_transport.clone();
        let mut coordinator = AccordCoordinator::new(config, &local_transport, self);

        match coordinator.execute(&command_id, command).await {
            Ok(result) => MaelstromObjectService::response_from_result(msg_id, result),
            Err(error) => map_internal_error(msg_id, &error),
        }
    }

    async fn wait_for_forward_response(&mut self, expected_msg_id: u64) -> So3Result<ResponseBody> {
        if let Some(response) = self.forward_responses.remove(&expected_msg_id) {
            return Ok(response);
        }
        if let Some(error) = self.internal_errors.remove(&expected_msg_id) {
            return Ok(error);
        }

        loop {
            let Some(message) = self.next_request_if_available().await? else {
                return Err(So3Error::InvalidRequest(
                    "maelstrom stdin closed while waiting for forwarded response".to_owned(),
                ));
            };

            match message.body.clone() {
                RequestBody::ForwardOk {
                    in_reply_to,
                    response,
                } if in_reply_to == expected_msg_id => return Ok(response),
                RequestBody::Error {
                    in_reply_to,
                    code,
                    text,
                } if in_reply_to == expected_msg_id => {
                    return Ok(ResponseBody::Error {
                        in_reply_to,
                        code,
                        text,
                    });
                }
                _ => self.dispatch(message).await?,
            }
        }
    }

    async fn wait_for_consensus_response(&mut self, expected_msg_id: u64) -> So3Result<Vec<u8>> {
        if let Some(response) = self.consensus_responses.remove(&expected_msg_id) {
            return Ok(response);
        }
        if let Some(error) = self.internal_errors.remove(&expected_msg_id) {
            return Err(So3Error::InvalidRequest(response_error_text(&error)));
        }

        loop {
            let Some(message) = self.next_request_if_available().await? else {
                return Err(So3Error::InvalidRequest(
                    "maelstrom stdin closed while waiting for consensus response".to_owned(),
                ));
            };

            match message.body.clone() {
                RequestBody::ConsensusOk {
                    in_reply_to,
                    payload,
                } if in_reply_to == expected_msg_id => return Ok(payload),
                RequestBody::Error {
                    in_reply_to, text, ..
                } if in_reply_to == expected_msg_id => return Err(So3Error::InvalidRequest(text)),
                _ => self.dispatch(message).await?,
            }
        }
    }

    async fn next_request_if_available(&mut self) -> So3Result<Option<Message<RequestBody>>> {
        next_request_if_available(&mut self.input).await
    }

    async fn write_message<T: Serialize>(&mut self, message: &Message<T>) -> So3Result<()> {
        write_message(&mut self.output, message).await
    }

    fn next_msg_id(&mut self) -> u64 {
        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.saturating_add(1);
        msg_id
    }

    fn next_command_sequence(&mut self) -> u64 {
        let sequence = self.next_command_sequence;
        self.next_command_sequence = self.next_command_sequence.saturating_add(1);
        sequence
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
            .filter(|node_id| node_id.as_str() != self.node_id)
            .cloned()
            .collect()
    }
}

#[async_trait(?Send)]
impl ConsensusPeerTransport for Runtime {
    async fn pre_accept_peer(
        &mut self,
        peer_id: &str,
        request: PreAcceptRequest,
    ) -> So3Result<PreAcceptResponse> {
        let payload = send_consensus_rpc(self, peer_id, ConsensusRpc::PreAccept, request).await?;
        decode_proto::<PreAcceptResponse>(&payload)
    }

    async fn accept_peer(
        &mut self,
        peer_id: &str,
        request: AcceptRequest,
    ) -> So3Result<AcceptResponse> {
        let payload = send_consensus_rpc(self, peer_id, ConsensusRpc::Accept, request).await?;
        decode_proto::<AcceptResponse>(&payload)
    }

    async fn commit_peer(
        &mut self,
        peer_id: &str,
        request: CommitRequest,
    ) -> So3Result<CommitResponse> {
        let payload = send_consensus_rpc(self, peer_id, ConsensusRpc::Commit, request).await?;
        decode_proto::<CommitResponse>(&payload)
    }
}

async fn send_consensus_rpc<T: ProstMessage>(
    runtime: &mut Runtime,
    peer_id: &str,
    rpc: ConsensusRpc,
    request: T,
) -> So3Result<Vec<u8>> {
    let msg_id = runtime.next_msg_id();
    runtime
        .write_message(&Message {
            src: runtime.node_id.clone(),
            dest: peer_id.to_owned(),
            body: RequestBody::Consensus {
                msg_id,
                rpc,
                payload: request.encode_to_vec(),
            },
        })
        .await?;
    runtime.wait_for_consensus_response(msg_id).await
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
    let service = MaelstromService::new(ObjectService::new(LocalStateMachine::new(
        storage.object_repository,
    )));
    let local_transport =
        ApplyingConsensusTransport::new(node_id.to_owned(), executor, storage.consensus_journal);

    Ok(RuntimeComponents {
        service,
        local_transport,
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

fn response_error_text(response: &ResponseBody) -> String {
    match response {
        ResponseBody::Error { text, .. } => text.clone(),
        _ => "unexpected internal response".to_owned(),
    }
}

fn decode_proto<T: Default + ProstMessage>(payload: &[u8]) -> So3Result<T> {
    T::decode(payload).map_err(|error| So3Error::Serialization(error.to_string()))
}

fn status_error(status: impl std::fmt::Display) -> So3Error {
    So3Error::InvalidRequest(status.to_string())
}
