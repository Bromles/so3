const CONSENSUS_PROTO_PATH: &str = "./proto/consensus.proto";

fn main() {
    if let Err(error) = tonic_prost_build::configure()
        .bytes(".")
        .compile_protos(&["proto/consensus.proto", "proto/blob.proto"], &["proto"])
    {
        panic!("failed to compile {CONSENSUS_PROTO_PATH}: {error}");
    }
}
