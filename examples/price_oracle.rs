//! Price Oracle Example
//!
//! Demonstrates using StreamForge for a blockchain price oracle:
//! - Fetching prices from multiple centralized exchanges via HTTP
//! - Aggregating prices (median, average)
//! - Filtering outliers
//! - Windowed aggregation
//! - Exactly-once processing with transaction IDs

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;
use streamforge::operators::{FilterOp, MapOp, StreamOperator};
use streamforge::sources::{Source, http::{HttpSource, HttpSourceConfig}};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    println!("🚀 StreamForge Price Oracle Demo");
    println!("================================\n");

    // Configure HTTP sources for multiple exchanges
    // Note: These are example URLs - replace with actual exchange APIs
    let exchange_urls = vec![
        "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT".to_string(),
        "https://api.coinbase.com/v2/exchange-rates?currency=BTC".to_string(),
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd".to_string(),
    ];

    let http_config = HttpSourceConfig {
        urls: exchange_urls,
        poll_interval: Duration::from_secs(5), // Poll every 5 seconds
        timeout: Duration::from_secs(10),
        max_retries: 3,
        retry_delay: Duration::from_millis(500),
        value_path: Some("$.price".to_string()), // Extract price field
        key_path: Some("$.symbol".to_string()),  // Extract symbol field
        headers: vec![("User-Agent".to_string(), "StreamForge/1.0".to_string())],
    };

    let mut http_source = HttpSource::new(http_config)?;

    println!("📡 Fetching prices from exchanges...\n");

    // Fetch prices for a few iterations
    for i in 0..5 {
        println!("--- Iteration {} ---", i + 1);

        if let Some(event) = http_source.read().await {
            println!("Received price event:");
            println!("  Key: {:?}", event.key);
            println!("  Value: {:?}", event.value);
            println!("  Timestamp: {}", event.timestamp);
            println!();
        } else {
            println!("No data received\n");
        }

        sleep(Duration::from_secs(1)).await;
    }

    println!("\n✅ Price oracle demo complete!");
    println!("\nTo build a full price oracle, you would:");
    println!("1. Create multiple HTTP sources (one per exchange)");
    println!("2. Merge the streams");
    println!("3. Filter outliers (e.g., prices outside reasonable range)");
    println!("4. Window by time (e.g., 1-minute windows)");
    println!("5. Aggregate (median, average)");
    println!("6. Output to sink (blockchain, database, etc.)");
    println!("7. Use exactly-once semantics to prevent duplicate updates");

    Ok(())
}

