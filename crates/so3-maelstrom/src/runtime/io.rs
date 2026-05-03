use serde::Serialize;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout};

use so3_core::domain::error::{So3Error, So3Result};

use crate::protocol::{Message, RequestBody};

pub(super) async fn next_request(
    lines: &mut Lines<BufReader<Stdin>>,
) -> So3Result<Message<RequestBody>> {
    next_request_if_available(lines)
        .await?
        .ok_or_else(|| So3Error::InvalidRequest("maelstrom stdin closed before init".to_owned()))
}

pub(super) async fn next_request_if_available(
    lines: &mut Lines<BufReader<Stdin>>,
) -> So3Result<Option<Message<RequestBody>>> {
    let Some(line) = lines.next_line().await? else {
        return Ok(None);
    };

    serde_json::from_str(&line).map(Some).map_err(|error| {
        So3Error::InvalidRequest(format!("failed to decode maelstrom request: {error}"))
    })
}

pub(super) async fn write_message(
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
