const CONSENSUS_PROTO_PATH: &str = "./proto/consensus.proto";

fn main() {
    if let Err(error) = tonic_prost_build::compile_protos(CONSENSUS_PROTO_PATH) {
        panic!("failed to compile {CONSENSUS_PROTO_PATH}: {error}");
    }
}
