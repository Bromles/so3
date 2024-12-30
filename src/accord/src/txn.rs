#[derive(Default)]
pub enum LocalTxnState {
    #[default]
    Unknown,
    PreAccepted,
    Accepted,
    Commited,
    Applied,
}
