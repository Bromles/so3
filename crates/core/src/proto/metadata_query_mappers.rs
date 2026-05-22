use crate::domain::blob::checksum::Sha256Digest;
use crate::domain::error::{So3Error, So3Result};
use crate::domain::object::key::ObjectKey;
use crate::domain::object::metadata::ObjectMetadata;
use crate::proto::base::ObjectMetadata as ProtoObjectMetadata;
use crate::proto::metadata_query::GetMetadataResponse as ProtoResponse;

pub fn metadata_to_proto(metadata: &ObjectMetadata) -> ProtoObjectMetadata {
    ProtoObjectMetadata {
        key: metadata.key.as_ref().to_string(),
        version: metadata.version.get(),
        blob_id: metadata.blob_id.to_string(),
        sha256: metadata.sha256.as_bytes().to_vec().into(),
        size: metadata.size,
        last_modified_ms: metadata.last_modified_ms,
        deleted: metadata.deleted,
    }
}

pub fn proto_to_metadata(proto: ProtoObjectMetadata) -> So3Result<ObjectMetadata> {
    let sha256_bytes: Vec<u8> = proto.sha256.into();
    let sha256_arr: [u8; 32] = sha256_bytes
        .try_into()
        .map_err(|_| So3Error::InvalidRequest("sha256 must be 32 bytes".into()))?;
    Ok(ObjectMetadata {
        key: ObjectKey::new(proto.key)?,
        version: proto.version.try_into()?,
        blob_id: proto.blob_id.as_str().try_into()?,
        sha256: Sha256Digest::from_bytes(sha256_arr),
        size: proto.size,
        last_modified_ms: proto.last_modified_ms,
        deleted: proto.deleted,
    })
}

pub fn metadata_option_to_proto_response(metadata: Option<&ObjectMetadata>) -> ProtoResponse {
    ProtoResponse {
        entry: metadata.map(metadata_to_proto),
    }
}

pub fn proto_response_to_metadata_option(res: ProtoResponse) -> So3Result<Option<ObjectMetadata>> {
    res.entry.map(proto_to_metadata).transpose()
}
