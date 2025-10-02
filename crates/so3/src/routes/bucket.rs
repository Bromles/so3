use axum::{Router, extract::State, routing::get};
use heed::{
    Database, byteorder,
    types::{Str, U32},
};

use crate::{AppState, routes::route};

pub fn create_bucket() -> Router<AppState> {
    async fn handler(State(state): State<AppState>) -> String {
        let mut wtxn = state.db.write_txn().unwrap();
        let db: Database<U32<byteorder::NetworkEndian>, Str> =
            state.db.create_database(&mut wtxn, None).unwrap();

        db.put(&mut wtxn, &0, "test").unwrap();
        wtxn.commit().unwrap();

        let rtxn = state.db.read_txn().unwrap();

        db.get(&rtxn, &0).unwrap().unwrap().to_string()
    }

    route("/", get(handler))
}
