//! Simple HTTP server that returns incremented numbers
use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    counter: Arc<AtomicU64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState {
        counter: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/counter", get(handler))
        .with_state(state);

    println!("Starting counter server on http://127.0.0.1:8093/counter");
    
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8093").await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    let value = state.counter.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "value": value,
        "timestamp": chrono::Utc::now().timestamp()
    }))
}

