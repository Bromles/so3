#![warn(clippy::pedantic)]
#![forbid(unsafe_code)]
use std::process::exit;

use tokio::{select, signal, spawn};
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing_subscriber::fmt as tracing_fmt;

use so3_core::domain::error::So3Result;
use so3_core::node::runtime::Node;

mod config;

const PROCESS_EXIT_FAILURE: i32 = 1;

#[tokio::main]
async fn main() {
    tracing_fmt().with_target(false).compact().init();

    if let Err(error) = run().await {
        error!(%error, "node exited with error");
        exit(PROCESS_EXIT_FAILURE);
    }
}

async fn run() -> So3Result<()> {
    let config = config::load_node_config()?;
    let node = Node::new(config).await?;
    let cancellation_token = CancellationToken::new();
    let signal_token = cancellation_token.clone();

    spawn(async move {
        match shutdown_signal().await {
            Ok(()) => {
                signal_token.cancel();
            }
            Err(error) => {
                error!(%error, "failed to register signal handler");
            }
        }
    });

    node.run(cancellation_token).await
}

async fn shutdown_signal() -> So3Result<()> {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    select! {
        () = ctrl_c => Ok(()),
        () = terminate => Ok(()),
    }
}
