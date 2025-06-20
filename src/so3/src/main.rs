#![forbid(unsafe_code)]

mod xml;

use axum::{Json, Router, routing::get};
use tokio::runtime;

fn main() {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(app());
}

async fn app() {
    tracing_subscriber::fmt::init();

    let app = Router::new().route("/", get(root));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> Json<&'static str> {
    Json("Hello, World!")
}
