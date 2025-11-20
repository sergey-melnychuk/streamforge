//! Metrics collector for tracking throughput, latency, and other metrics

/// Collects metrics about stream processing performance
pub struct MetricsCollector {
    pub name: String,
}

impl MetricsCollector {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}
