fn main() {
    if let Err(error) = tonic_prost_build::configure().bytes(".").compile_protos(
        &[
            "proto/base.proto",
            "proto/consensus.proto",
            "proto/blob.proto",
            "proto/metadata_query.proto",
        ],
        &["proto"],
    ) {
        panic!("failed to compile protos: {error}");
    }
}
