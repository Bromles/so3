use std::path::{Path, PathBuf};

use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout, stdin, stdout,
};

use so3_core::consensus::state_machine::LocalStateMachine;
use so3_core::domain::error::{So3Error, So3Result};
use so3_core::object_server::service::ObjectService;
use so3_core::storage::registry::SqliteFsPersistentObjectRepository;

use crate::config::StorageRoots;
use crate::protocol::{Message, RequestBody, ResponseBody, reply};
use crate::service::MaelstromService;

pub async fn run(storage_roots: StorageRoots) -> So3Result<()> {
    let mut input = BufReader::new(stdin()).lines();
    let mut output = BufWriter::new(stdout());

    let init_request = next_request(&mut input).await?;
    let RequestBody::Init {
        msg_id,
        node_id,
        node_ids: _node_ids,
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

    while let Some(request) = next_request_if_available(&mut input).await? {
        let response = service.handle(request.body.clone()).await;
        write_message(&mut output, &reply(&request, response)).await?;
    }

    Ok(())
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
    message: &Message<ResponseBody>,
) -> So3Result<()> {
    let encoded = serde_json::to_vec(message)
        .map_err(|error| So3Error::Serialization(error.to_string()))?;
    output.write_all(&encoded).await?;
    output.write_u8(b'\n').await?;
    output.flush().await?;
    Ok(())
}
