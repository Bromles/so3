use axum::{Router, routing::MethodRouter};

use crate::AppState;

pub mod bucket;

fn route(path: &str, method_router: MethodRouter<AppState>) -> Router<AppState> {
    Router::new().route(path, method_router)
}
