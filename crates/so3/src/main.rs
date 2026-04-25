use std::process::exit;

use tokio::signal::ctrl_c;
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing_subscriber::fmt as tracing_fmt;

use so3_core::domain::error::So3Result;
use so3_core::node::config::NodeConfig;
use so3_core::node::runtime::Node;

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
    let config = NodeConfig::load()?;
    let node = Node::new(config).await?;
    let cancellation_token = CancellationToken::new();
    let signal_token = cancellation_token.clone();

    spawn(async move {
        if ctrl_c().await.is_ok() {
            signal_token.cancel();
        }
    });

    node.run(cancellation_token).await
}
