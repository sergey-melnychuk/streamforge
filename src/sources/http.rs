//! HTTP source for reading data from HTTP/REST APIs
//!
//! Designed for price oracle use cases (fetching prices from centralized exchanges)

use crate::core::{Event, EventKey, EventValue};
use crate::sources::Source;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// HTTP source configuration
#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// URLs to poll (for multiple endpoints, e.g., multiple exchanges)
    pub urls: Vec<String>,
    /// Polling interval
    pub poll_interval: Duration,
    /// Request timeout
    pub timeout: Duration,
    /// Maximum number of retries on failure
    pub max_retries: u32,
    /// Retry delay
    pub retry_delay: Duration,
    /// JSON path to extract price/value (e.g., "$.price" or "$.data.price")
    pub value_path: Option<String>,
    /// JSON path to extract key (e.g., "$.symbol")
    pub key_path: Option<String>,
    /// Headers to include in requests
    pub headers: Vec<(String, String)>,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            poll_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
            value_path: None,
            key_path: None,
            headers: Vec::new(),
        }
    }
}

/// HTTP source for polling REST APIs
pub struct HttpSource {
    config: HttpSourceConfig,
    client: reqwest::Client,
    current_url_index: usize,
    exhausted: bool,
}

impl HttpSource {
    /// Create a new HTTP source
    pub fn new(config: HttpSourceConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::builder().timeout(config.timeout).build()?;

        Ok(Self {
            config,
            client,
            current_url_index: 0,
            exhausted: false,
        })
    }

    /// Create from a single URL (for simple use cases)
    pub fn from_url(
        url: impl Into<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new(HttpSourceConfig {
            urls: vec![url.into()],
            ..Default::default()
        })
    }

    /// Create for price oracle (multiple exchange URLs)
    pub fn for_price_oracle(
        urls: Vec<String>,
        poll_interval: Duration,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new(HttpSourceConfig {
            urls,
            poll_interval,
            value_path: Some("$.price".to_string()), // Common price path
            key_path: Some("$.symbol".to_string()),  // Common symbol path
            ..Default::default()
        })
    }

    /// Fetch data from current URL
    async fn fetch_data(&self) -> Result<Option<Event>, Box<dyn std::error::Error + Send + Sync>> {
        if self.config.urls.is_empty() {
            return Ok(None);
        }

        let url = &self.config.urls[self.current_url_index];
        let mut retries = 0;

        loop {
            match self.fetch_with_retry(url).await {
                Ok(Some(event)) => return Ok(Some(event)),
                Ok(None) => return Ok(None),
                Err(e) => {
                    retries += 1;
                    if retries >= self.config.max_retries {
                        error!(
                            "Failed to fetch from {} after {} retries: {}",
                            url, retries, e
                        );
                        return Err(e);
                    }
                    warn!(
                        "Retry {}/{} for {}: {}",
                        retries, self.config.max_retries, url, e
                    );
                    sleep(self.config.retry_delay).await;
                }
            }
        }
    }

    /// Fetch data from URL with retry logic
    async fn fetch_with_retry(
        &self,
        url: &str,
    ) -> Result<Option<Event>, Box<dyn std::error::Error + Send + Sync>> {
        debug!("Polling URL: {}", url);
        let mut request = self.client.get(url);

        // Add headers
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            warn!("HTTP error from {}: {}", url, response.status());
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let json: JsonValue = response.json().await?;
        debug!("Received JSON from {}: {} bytes", url, serde_json::to_string(&json).unwrap_or_default().len());

        // Extract value using JSON path
        let value = if let Some(path) = &self.config.value_path {
            Self::extract_json_path(&json, path).unwrap_or_else(|| EventValue::Json(json.clone()))
        } else {
            EventValue::Json(json.clone())
        };

        // Extract key using JSON path
        let key = if let Some(path) = &self.config.key_path {
            Self::extract_json_path(&json, path)
                .and_then(|v| match v {
                    EventValue::String(s) => Some(EventKey::from_str(s.as_ref())),
                    EventValue::Int(i) => Some(EventKey::from_int(i)),
                    _ => None,
                })
                .unwrap_or_default()
        } else {
            EventKey::default()
        };

        let event = Event::new(key.clone(), value, chrono::Utc::now().timestamp_millis());
        info!("Successfully fetched event from {}: key={:?}", url, key);

        Ok(Some(event))
    }

    /// Extract value from JSON using simple path (supports "$.field" or "$.field.subfield")
    fn extract_json_path(json: &JsonValue, path: &str) -> Option<EventValue> {
        // Simple JSON path extraction (supports "$.field" or "$.field.subfield")
        // For production, consider using a proper JSON path library
        let path = path.trim_start_matches("$.");
        let parts: Vec<&str> = path.split('.').collect();

        let mut current = json;
        for part in parts {
            current = current.get(part)?;
        }

        // Convert JSON value to EventValue
        match current {
            JsonValue::Null => Some(EventValue::Null),
            JsonValue::Bool(b) => Some(EventValue::Bool(*b)),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(EventValue::Int(i))
                } else {
                    n.as_f64().map(EventValue::Float)
                }
            }
            JsonValue::String(s) => Some(EventValue::String(s.clone().into())),
            JsonValue::Array(_) | JsonValue::Object(_) => Some(EventValue::Json(current.clone())),
        }
    }

    /// Rotate to next URL (for round-robin across multiple endpoints)
    fn rotate_url(&mut self) {
        if !self.config.urls.is_empty() {
            self.current_url_index = (self.current_url_index + 1) % self.config.urls.len();
        }
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn read(&mut self) -> Option<Event> {
        if self.exhausted {
            return None;
        }

        // Sleep before polling (except first call)
        if self.current_url_index == 0 {
            sleep(self.config.poll_interval).await;
        }

        match self.fetch_data().await {
            Ok(Some(event)) => {
                // Rotate to next URL for next call
                self.rotate_url();
                Some(event)
            }
            Ok(None) => {
                // No data, but not exhausted
                self.rotate_url();
                None
            }
            Err(e) => {
                error!("Error fetching data: {}", e);
                // Continue trying (don't mark as exhausted)
                self.rotate_url();
                None
            }
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_http_source_config() {
        let config = HttpSourceConfig {
            urls: vec!["https://api.example.com/price".to_string()],
            poll_interval: Duration::from_secs(1),
            ..Default::default()
        };

        assert!(!config.urls.is_empty());
    }

    #[tokio::test]
    async fn test_json_path_extraction() {
        let json: JsonValue = serde_json::json!({
            "price": 50000.5,
            "symbol": "BTC/USD",
            "data": {
                "value": 100
            }
        });

        // Test simple path
        let value = HttpSource::extract_json_path(&json, "$.price");
        assert!(matches!(value, Some(EventValue::Float(50000.5))));

        // Test nested path
        let value = HttpSource::extract_json_path(&json, "$.data.value");
        assert!(matches!(value, Some(EventValue::Int(100))));

        // Test string path
        let value = HttpSource::extract_json_path(&json, "$.symbol");
        assert!(matches!(value, Some(EventValue::String(_))));
    }
}
