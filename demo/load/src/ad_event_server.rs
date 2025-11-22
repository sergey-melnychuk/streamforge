//! Ad Event Server
//!
//! HTTP server that generates and serves ad events on each request
//! Used as a source for StreamForge HTTP polling

use axum::{routing::get, Json, Router};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdEvent {
    event_type: String,
    campaign_id: String,
    ad_id: String,
    user_id: String,
    timestamp: u64,
    device: String,
    country: String,
    cost: f64,
}

const CAMPAIGNS: &[&str] = &[
    "campaign_holiday_sale",
    "campaign_new_product_launch",
    "campaign_brand_awareness",
    "campaign_retargeting",
    "campaign_flash_sale",
];

const DEVICES: &[&str] = &["mobile", "desktop", "tablet"];
const COUNTRIES: &[&str] = &["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "IN", "SG"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "8091".to_string());

    info!("Starting Ad Event Server on port {}", port);

    let app = Router::new()
        .route("/event", get(serve_event))
        .route("/events", get(serve_batch));

    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ad Event Server listening on http://{}", addr);
    info!("GET http://{}/event - single event", addr);
    info!("GET http://{}/events - batch of 10 events", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn serve_event() -> Json<AdEvent> {
    Json(generate_event())
}

async fn serve_batch() -> Json<Vec<AdEvent>> {
    let events: Vec<AdEvent> = (0..10).map(|_| generate_event()).collect();
    Json(events)
}

fn generate_event() -> AdEvent {
    let mut rng = rand::thread_rng();

    let campaign_id = CAMPAIGNS[rng.gen_range(0..CAMPAIGNS.len())].to_string();
    let ad_id = format!("ad_{}_{}", campaign_id, rng.gen_range(1..=10));
    let user_id = format!("user_{}", rng.gen_range(1..=1000));
    let device = DEVICES[rng.gen_range(0..DEVICES.len())].to_string();
    let country = COUNTRIES[rng.gen_range(0..COUNTRIES.len())].to_string();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Realistic event distribution
    let rand_val = rng.gen_range(0..100);
    let event_type = if rand_val < 3 {
        "click"
    } else if rand_val == 99 {
        "conversion"
    } else {
        "impression"
    };

    let cost = match event_type {
        "impression" => rng.gen_range(0.01..0.10),
        "click" => rng.gen_range(0.50..2.00),
        "conversion" => rng.gen_range(5.00..20.00),
        _ => 0.0,
    };

    AdEvent {
        event_type: event_type.to_string(),
        campaign_id,
        ad_id,
        user_id,
        timestamp,
        device,
        country,
        cost,
    }
}
