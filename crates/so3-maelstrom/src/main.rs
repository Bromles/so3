#![warn(clippy::pedantic)]
#![forbid(unsafe_code)]
use std::process::exit;

use tracing::error;
use tracing_subscriber::fmt as tracing_fmt;

mod config;
mod protocol;
mod runtime;
mod service;

const PROCESS_EXIT_FAILURE: i32 = 1;

#[tokio::main]
async fn main() {
    tracing_fmt()
        .with_target(false)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .compact()
        .init();

    if let Err(error) = runtime::run(config::load_storage_roots()).await {
        error!(%error, "maelstrom adapter exited with error");
        exit(PROCESS_EXIT_FAILURE);
    }
}
