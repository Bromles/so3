#![forbid(unsafe_code)]

mod xml;

use axum::{Router, routing::get};
use axum_extra::{TypedHeader, headers::UserAgent};
use serde::Serialize;
use tokio::runtime;
use tracing::info;

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

    let addr_str = "0.0.0.0:3000";

    let listener = tokio::net::TcpListener::bind(addr_str).await.unwrap();

    info!("Server is listening on {}", addr_str);

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

async fn root(TypedHeader(user_agent): TypedHeader<UserAgent>) -> Xml<Example> {
    Xml(Example {
        to: "Tove".to_string(),
        from: "Jani".to_string(),
        heading: "Reminder".to_string(),
        body: format!("Don't forget me this weekend! User-Agent: {}", user_agent),
    })
}
