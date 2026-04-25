use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use serde::Serialize;
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout, stdin, stdout,
};

use so3_core::consensus::state_machine::LocalStateMachine;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::object_server::service::ObjectService;
use so3_core::storage::registry::SqliteFsPersistentObjectRepository;

use crate::config::StorageRoots;
use crate::protocol::{
    CRASH_CODE, ClientRequest, Message, RequestBody, ResponseBody, error_response, reply,
};
use crate::service::MaelstromService;

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

    let service = build_service(&storage_roots, node_id).await?;
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
        service,
        node_id: node_id.clone(),
        node_ids: node_ids.clone(),
        next_msg_id: msg_id.saturating_add(1),
        forward_responses: HashMap::new(),
        replicate_acks: HashSet::new(),
        internal_errors: HashMap::new(),
    }
    .run()
    .await
}

struct Runtime {
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>,
    service: MaelstromService<SqliteFsPersistentObjectRepository>,
    node_id: String,
    node_ids: Vec<String>,
    next_msg_id: u64,
    forward_responses: HashMap<u64, ResponseBody>,
    replicate_acks: HashSet<u64>,
    internal_errors: HashMap<u64, ResponseBody>,
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
                    let response =
                        error_response(msg_id, CRASH_CODE, "duplicate init request");
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
                    self.handle_client_message(
                        &src,
                        msg_id,
                        ClientRequest::Write { key, value },
                    )
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
                } => self.handle_forward(&src, msg_id, client_msg_id, request).await,
                RequestBody::Replicate { msg_id, request } => {
                    self.handle_replicate(&src, msg_id, request).await
                }
                RequestBody::ForwardOk {
                    in_reply_to,
                    response,
                } => {
                    self.forward_responses.insert(in_reply_to, response);
                    Ok(())
                }
                RequestBody::ReplicateOk { in_reply_to } => {
                    self.replicate_acks.insert(in_reply_to);
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

    async fn execute_leader_command(
        &mut self,
        msg_id: u64,
        request: ClientRequest,
    ) -> ResponseBody {
        let response = self.service.handle_client(msg_id, request.clone()).await;
        if !matches!(
            response,
            ResponseBody::WriteOk { .. } | ResponseBody::CasOk { .. }
        ) {
            return response;
        }

        for follower in self.follower_ids() {
            let replicate_msg_id = self.next_msg_id();
            if let Err(error) = self
                .write_message(&Message {
                    src: self.node_id.clone(),
                    dest: follower.clone(),
                    body: RequestBody::Replicate {
                        msg_id: replicate_msg_id,
                        request: request.clone(),
                    },
                })
                .await
            {
                return map_internal_error(msg_id, &error);
            }

            if let Err(error) = self.wait_for_replicate_response(replicate_msg_id).await {
                return map_internal_error(msg_id, &error);
            }
        }

        response
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

    async fn wait_for_replicate_response(&mut self, expected_msg_id: u64) -> So3Result<()> {
        if self.replicate_acks.remove(&expected_msg_id) {
            return Ok(());
        }
        if let Some(error) = self.internal_errors.remove(&expected_msg_id) {
            return Err(So3Error::InvalidRequest(response_error_text(&error)));
        }

        loop {
            let Some(message) = self.next_request_if_available().await? else {
                return Err(So3Error::InvalidRequest(
                    "maelstrom stdin closed while waiting for replication response".to_owned(),
                ));
            };

            match message.body.clone() {
                RequestBody::ReplicateOk { in_reply_to } if in_reply_to == expected_msg_id => {
                    return Ok(());
                }
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

async fn build_service(
    storage_roots: &StorageRoots,
    node_id: &str,
) -> So3Result<MaelstromService<SqliteFsPersistentObjectRepository>> {
    let repository = SqliteFsPersistentObjectRepository::new(
        node_storage_dir(&storage_roots.metadata_dir, node_id),
        node_storage_dir(&storage_roots.blob_dir, node_id),
    )
    .await?;

    Ok(MaelstromService::new(ObjectService::new(
        LocalStateMachine::new(repository),
    )))
}

fn node_storage_dir(root: &Path, node_id: &str) -> PathBuf {
    root.join(node_id)
}

async fn next_request(lines: &mut Lines<BufReader<Stdin>>) -> So3Result<Message<RequestBody>> {
    next_request_if_available(lines).await?.ok_or_else(|| {
        So3Error::InvalidRequest("maelstrom stdin closed before init".to_owned())
    })
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
    let encoded = serde_json::to_vec(message)
        .map_err(|error| So3Error::Serialization(error.to_string()))?;
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
