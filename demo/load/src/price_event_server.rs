//! Price Event Server
//!
//! HTTP server that generates and serves price events on each request
//! Used as a source for StreamForge HTTP polling

use axum::{routing::get, Json, Router};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceEvent {
    exchange: String,
    symbol: String,
    price: f64,
    volume: f64,
    timestamp: u64,
}

const EXCHANGES: &[&str] = &[
    "Binance",
    "Coinbase",
    "Kraken",
    "Huobi",
    "Bitfinex",
    "Gemini",
    "KuCoin",
];

const SYMBOLS: &[(&str, f64)] = &[
    ("BTC/USD", 43000.0),
    ("ETH/USD", 2300.0),
    ("SOL/USD", 98.0),
    ("AVAX/USD", 36.0),
    ("MATIC/USD", 0.85),
    ("DOT/USD", 7.2),
];

// Shared state for price tracking
static PRICE_STATE: Mutex<Option<Vec<f64>>> = Mutex::new(None);

fn init_price_state() {
    let mut state = PRICE_STATE.lock().unwrap();
    if state.is_none() {
        *state = Some(SYMBOLS.iter().map(|(_, base)| *base).collect());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "8092".to_string());

    info!("Starting Price Event Server on port {}", port);

    // Initialize price state
    init_price_state();

    let app = Router::new()
        .route("/event", get(serve_event))
        .route("/events", get(serve_batch));

    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Price Event Server listening on http://{}", addr);
    info!("GET http://{}/event - single price event", addr);
    info!("GET http://{}/events - batch of 10 events", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn serve_event() -> Json<PriceEvent> {
    Json(generate_event())
}

async fn serve_batch() -> Json<Vec<PriceEvent>> {
    let events: Vec<PriceEvent> = (0..10).map(|_| generate_event()).collect();
    Json(events)
}

fn generate_event() -> PriceEvent {
    let mut rng = rand::thread_rng();
    
    // Get and update price state
    let mut state = PRICE_STATE.lock().unwrap();
    let prices = state.as_mut().unwrap();
    
    let exchange = EXCHANGES[rng.gen_range(0..EXCHANGES.len())];
    let symbol_idx = rng.gen_range(0..SYMBOLS.len());
    let (symbol, _) = SYMBOLS[symbol_idx];
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Random walk for price movement
    let price_change = rng.gen_range(-0.01..0.01); // ±1% change
    prices[symbol_idx] *= 1.0 + price_change;

    // Exchange-specific bias
    let exchange_bias = match exchange {
        "Binance" => rng.gen_range(-0.001..0.001),
        "Coinbase" => rng.gen_range(0.0..0.002),
        "Kraken" => rng.gen_range(-0.002..0.0),
        _ => rng.gen_range(-0.001..0.001),
    };

    let price = prices[symbol_idx] * (1.0 + exchange_bias);

    // Volume varies by exchange and symbol
    let base_volume = match symbol {
        "BTC/USD" => rng.gen_range(0.1..5.0),
        "ETH/USD" => rng.gen_range(1.0..50.0),
        _ => rng.gen_range(10.0..1000.0),
    };

    let volume = base_volume * rng.gen_range(0.5..2.0);

    PriceEvent {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        price,
        volume,
        timestamp,
    }
}
