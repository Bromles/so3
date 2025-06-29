#![forbid(unsafe_code)]

mod xml;

use axum::{Router, routing::get};
use serde::Serialize;
use tokio::runtime;

use crate::xml::Xml;

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

#[derive(Serialize)]
#[serde(rename = "note")]
struct Example {
    to: String,
    from: String,
    heading: String,
    body: String,
}

async fn root() -> Xml<Example> {
    Xml(Example {
        to: "Tove".to_string(),
        from: "Jani".to_string(),
        heading: "Reminder".to_string(),
        body: "Don't forget me this weekend!".to_string(),
    })
}
