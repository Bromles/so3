use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;

use so3_core::domain::error::{So3Error, So3Result};

use crate::config::StorageRoots;
use crate::protocol::{reply, RequestBody, ResponseBody};
use crate::runtime::components::build_components;
use crate::runtime::handler::route_or_spawn;
use crate::runtime::io::{next_request, next_request_if_available, write_message};
use crate::runtime::types::{SharedRuntime, SharedState};

mod components;
mod handler;
mod io;
mod peer;
mod types;

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
        pending_blobs: Mutex::new(HashMap::new()),
        pending_metadata_queries: Mutex::new(HashMap::new()),
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
        service: components.service,
        local_handler: components.local_handler,
        local_blobs: components.local_blobs,
        local_metadata_query: components.local_metadata_query,
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
