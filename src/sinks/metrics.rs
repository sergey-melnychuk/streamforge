//! Metrics sink for exporting Prometheus metrics
//!
//! Uses the Prometheus client library to record metrics from query results

use crate::core::{Event, EventValue};
use crate::sinks::Sink;
use async_trait::async_trait;
use prometheus::{Encoder, GaugeVec, IntCounterVec, Opts, Registry, TextEncoder};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Metrics sink configuration
#[derive(Debug, Clone)]
pub struct MetricsSinkConfig {
    /// Metric name prefix
    pub metric_prefix: String,
    /// Optional HTTP server address to expose metrics endpoint (e.g., "127.0.0.1:9090")
    pub http_endpoint: Option<SocketAddr>,
    /// Whether to start HTTP server in background
    pub start_http_server: bool,
}

impl Default for MetricsSinkConfig {
    fn default() -> Self {
        Self {
            metric_prefix: "streamforge".to_string(),
            http_endpoint: Some("127.0.0.1:9090".parse().unwrap()),
            start_http_server: true,
        }
    }
}

/// Metrics sink that exports Prometheus metrics
pub struct MetricsSink {
    config: MetricsSinkConfig,
    /// Prometheus registry
    registry: Arc<Registry>,
    /// Dynamic gauges (for numeric values from query results)
    gauges: Arc<RwLock<HashMap<String, Arc<GaugeVec>>>>,
    /// Dynamic counters (for count metrics)
    counters: Arc<RwLock<HashMap<String, Arc<IntCounterVec>>>>,
}

impl MetricsSink {
    /// Create a new metrics sink
    pub fn new(
        config: MetricsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let registry = Registry::new();

        // Start HTTP server if configured
        if config.start_http_server {
            if let Some(addr) = config.http_endpoint {
                let registry_clone = registry.clone();
                tokio::spawn(async move {
                    Self::start_http_server(addr, registry_clone).await;
                });
                info!("Metrics HTTP server started on http://{}", addr);
            }
        }

        Ok(Self {
            config,
            registry: Arc::new(registry),
            gauges: Arc::new(RwLock::new(HashMap::new())),
            counters: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create metrics sink for ad analytics
    pub fn for_ad_analytics(
        http_endpoint: Option<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new(MetricsSinkConfig {
            metric_prefix: "ad".to_string(),
            http_endpoint,
            start_http_server: http_endpoint.is_some(),
        })
    }

    /// Create metrics sink for price oracle
    pub fn for_price_oracle(
        http_endpoint: Option<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new(MetricsSinkConfig {
            metric_prefix: "oracle".to_string(),
            http_endpoint,
            start_http_server: http_endpoint.is_some(),
        })
    }

    /// Start HTTP server for metrics endpoint
    async fn start_http_server(addr: SocketAddr, registry: Registry) {
        use axum::{body::Body, response::Response, routing::get, Router};
        use std::sync::Arc as StdArc;
        use tracing::{error, warn};

        let registry = StdArc::new(registry);

        let app = Router::new().route(
            "/metrics",
            get({
                let registry = registry.clone();
                move || {
                    let registry = registry.clone();
                    async move {
                        let encoder = TextEncoder::new();
                        let metric_families = registry.gather();
                        let mut buffer = Vec::new();
                        if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
                            error!("Failed to encode metrics: {}", e);
                            return Response::builder()
                                .status(500)
                                .body(Body::from(format!("Failed to encode metrics: {}", e)))
                                .unwrap();
                        }
                        let body = match String::from_utf8(buffer) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Failed to convert metrics to string: {}", e);
                                return Response::builder()
                                    .status(500)
                                    .body(Body::from(format!("Failed to convert metrics: {}", e)))
                                    .unwrap();
                            }
                        };
                        Response::builder()
                            .status(200)
                            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                            .body(Body::from(body))
                            .unwrap()
                    }
                }
            }),
        );

        // Try to bind with retries
        let mut listener = None;
        for attempt in 0..5 {
            match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => {
                    listener = Some(l);
                    break;
                }
                Err(e) => {
                    if attempt < 4 {
                        warn!(
                            "Failed to bind metrics server on {} (attempt {}/5): {}, retrying...",
                            addr,
                            attempt + 1,
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    } else {
                        error!(
                            "Failed to bind metrics server on {} after 5 attempts: {}",
                            addr, e
                        );
                        return;
                    }
                }
            }
        }

        let listener = match listener {
            Some(l) => l,
            None => {
                error!(
                    "Failed to bind metrics server on {}: all attempts exhausted",
                    addr
                );
                return;
            }
        };

        info!("Prometheus metrics endpoint: http://{}/metrics", addr);

        if let Err(e) = axum::serve(listener, app).await {
            error!("Metrics server failed on {}: {}", addr, e);
        }
    }

    /// Get or create a gauge metric
    async fn get_or_create_gauge(
        &self,
        metric_name: &str,
        label_names: &[&str],
    ) -> Result<Arc<GaugeVec>, Box<dyn std::error::Error + Send + Sync>> {
        let mut gauges = self.gauges.write().await;

        if let Some(gauge) = gauges.get(metric_name) {
            return Ok(gauge.clone());
        }

        let opts = Opts::new(metric_name, metric_name)
            .namespace(&self.config.metric_prefix)
            .subsystem("");
        let gauge = GaugeVec::new(opts, label_names)?;
        self.registry.register(Box::new(gauge.clone()))?;

        let gauge_arc = Arc::new(gauge);
        gauges.insert(metric_name.to_string(), gauge_arc.clone());
        Ok(gauge_arc)
    }

    /// Get or create a counter metric
    async fn get_or_create_counter(
        &self,
        metric_name: &str,
        label_names: &[&str],
    ) -> Result<Arc<IntCounterVec>, Box<dyn std::error::Error + Send + Sync>> {
        let mut counters = self.counters.write().await;

        if let Some(counter) = counters.get(metric_name) {
            return Ok(counter.clone());
        }

        let opts = Opts::new(metric_name, metric_name)
            .namespace(&self.config.metric_prefix)
            .subsystem("");
        let counter = IntCounterVec::new(opts, label_names)?;
        self.registry.register(Box::new(counter.clone()))?;

        let counter_arc = Arc::new(counter);
        counters.insert(metric_name.to_string(), counter_arc.clone());
        Ok(counter_arc)
    }

    /// Convert event to Prometheus metrics
    async fn event_to_metrics(
        &self,
        event: &Event,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tracing::debug;
        if let EventValue::Json(json) = &event.value {
            if let Some(obj) = json.as_object() {
                debug!("Converting event to metrics: {:?}", json);
                // Extract labels (string fields) and values (numeric fields)
                let mut labels: HashMap<String, String> = HashMap::new();
                let mut values: HashMap<String, f64> = HashMap::new();

                for (key, value) in obj {
                    match value {
                        JsonValue::String(s) => {
                            labels.insert(key.clone(), s.clone());
                        }
                        JsonValue::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                values.insert(key.clone(), f);
                            } else if let Some(i) = n.as_i64() {
                                values.insert(key.clone(), i as f64);
                            }
                        }
                        JsonValue::Bool(b) => {
                            labels.insert(key.clone(), b.to_string());
                        }
                        _ => {}
                    }
                }

                // Build label keys and values once
                let label_keys: Vec<&str> = labels.keys().map(|s| s.as_str()).collect();
                let label_values: Vec<&str> = labels.values().map(|s| s.as_str()).collect();

                // Record metrics for each numeric value
                for (field, value) in &values {
                    let metric_name = format!("{}_{}", self.config.metric_prefix, field);
                    info!(
                        "Creating metric: {} = {} with labels: {:?}",
                        metric_name, value, labels
                    );

                    // Get or create gauge
                    let gauge = self.get_or_create_gauge(&metric_name, &label_keys).await?;
                    gauge.with_label_values(&label_values).set(*value);
                    info!("Metric {} set successfully", metric_name);
                }

                // Special handling for count fields
                if let Some(count) = values.get("event_count") {
                    let metric_name = format!("{}_events_total", self.config.metric_prefix);
                    let counter = self
                        .get_or_create_counter(&metric_name, &label_keys)
                        .await?;
                    counter
                        .with_label_values(&label_values)
                        .inc_by(*count as u64);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for MetricsSink {
    async fn write(&mut self, event: Event) -> Result<(), crate::sinks::SinkError> {
        self.event_to_metrics(&event).await.map_err(|e| {
            crate::sinks::SinkError::Other(format!("Failed to record metrics: {}", e))
        })?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), crate::sinks::SinkError> {
        // Prometheus metrics are recorded immediately, no flush needed
        Ok(())
    }

    async fn close(&mut self) -> Result<(), crate::sinks::SinkError> {
        info!("Metrics sink closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Event, EventKey, EventValue};
    use serde_json::json;

    #[tokio::test]
    async fn test_metrics_sink_basic() {
        let config = MetricsSinkConfig {
            metric_prefix: "test".to_string(),
            http_endpoint: None,
            start_http_server: false,
        };

        let mut sink = MetricsSink::new(config).unwrap();

        let event_data = json!({
            "campaign_id": "campaign_1",
            "event_count": 100,
            "total_cost": 45.23,
            "avg_cost": 0.4523
        });

        let event = Event::new(
            EventKey::default(),
            EventValue::Json(event_data),
            chrono::Utc::now().timestamp_millis(),
        );

        sink.write(event).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();
    }
}
