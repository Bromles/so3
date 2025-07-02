mod routes;
mod xml;

use std::sync::Arc;

use axum::{Router, routing::get};
use axum_extra::{TypedHeader, headers::UserAgent};
use heed::{Env, EnvOpenOptions};
use serde::Serialize;
use tokio::runtime;
use tracing::info;

use crate::{routes::bucket::create_bucket, xml::Xml};

pub struct State {
    db: Env,
}

pub type AppState = Arc<State>;

fn main() {
    tracing_subscriber::fmt::init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    info!("Using tempdir {}", dir.path().display());

    let db = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };
    let state = Arc::new(State { db });

    rt.block_on(app(state));
}

async fn app(state: Arc<State>) {
    let app = Router::new()
        .route("/", get(root))
        .nest("/bucket", create_bucket())
        .with_state(state);

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
