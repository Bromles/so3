use std::sync::Arc;

use prost::Message as ProstMessage;

use so3_core::domain::error::{So3Error, So3Result};
use so3_core::domain::object::key::ObjectKey;
use so3_core::proto::metadata_query::GetMetadataRequest as ProtoRequest;
use so3_core::proto::metadata_query_mappers::metadata_option_to_proto_response;
use so3_core::use_case::metadata_query::MetadataQueryUseCase;

use crate::protocol::{Message, RequestBody, CRASH_CODE};
use crate::runtime::types::SharedRuntime;

pub(super) async fn handle_metadata_query(
    shared: Arc<SharedRuntime>,
    sender: String,
    msg_id: u64,
    payload: Vec<u8>,
) -> So3Result<()> {
    let result: So3Result<Vec<u8>> = match ProtoRequest::decode(payload.as_slice())
        .map_err(|e| So3Error::Serialization(e.to_string()))
    {
        Ok(req) => {
            let key = ObjectKey::new(req.key)
                .map_err(|e| So3Error::InvalidRequest(format!("invalid key: {e}")))?;
            match shared.local_metadata_query.query(&key).await {
                Ok(metadata) => {
                    let proto_res = metadata_option_to_proto_response(metadata.as_ref());
                    Ok(proto_res.encode_to_vec())
                }
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    };

    match result {
        Ok(response_payload) => shared.send_message(&Message {
            src: shared.shared.node_id.clone(),
            dest: sender,
            body: RequestBody::MetadataQueryOk {
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
