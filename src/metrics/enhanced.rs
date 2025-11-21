//! Enhanced monitoring with distributed metrics, operator-level metrics, and latency histograms

use crate::distributed::node::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Latency histogram for tracking percentiles
pub struct LatencyHistogram {
    /// Latency samples (microseconds)
    samples: Vec<u64>,
    /// Maximum number of samples to keep
    max_samples: usize,
}

impl LatencyHistogram {
    /// Create a new latency histogram
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
        }
    }

    /// Record a latency sample
    pub fn record(&mut self, latency: Duration) {
        let latency_us = latency.as_micros() as u64;
        self.samples.push(latency_us);

        // Keep only recent samples
        if self.samples.len() > self.max_samples {
            self.samples.remove(0);
        }
    }

    /// Calculate percentile (0.0 to 1.0, e.g., 0.95 for P95)
    pub fn percentile(&self, p: f64) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }

        let mut sorted = self.samples.clone();
        sorted.sort_unstable();

        let index = ((sorted.len() as f64 - 1.0) * p) as usize;
        Some(sorted[index])
    }

    /// Get P50, P95, P99 percentiles
    pub fn percentiles(&self) -> Percentiles {
        Percentiles {
            p50: self.percentile(0.50),
            p95: self.percentile(0.95),
            p99: self.percentile(0.99),
            min: self.samples.iter().min().copied(),
            max: self.samples.iter().max().copied(),
            count: self.samples.len(),
        }
    }

    /// Clear all samples
    pub fn clear(&mut self) {
        self.samples.clear();
    }
}

/// Latency percentiles
#[derive(Debug, Clone, Default)]
pub struct Percentiles {
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub count: usize,
}

/// Operator-level metrics
pub struct OperatorMetrics {
    /// Operator name/ID
    operator_id: String,
    /// Events processed
    events_processed: u64,
    /// Bytes processed
    bytes_processed: u64,
    /// Errors encountered
    errors: u64,
    /// Latency histogram
    latency_histogram: LatencyHistogram,
    /// Last update time
    last_update: Instant,
}

impl OperatorMetrics {
    /// Create new operator metrics
    pub fn new(operator_id: impl Into<String>) -> Self {
        Self {
            operator_id: operator_id.into(),
            events_processed: 0,
            bytes_processed: 0,
            errors: 0,
            latency_histogram: LatencyHistogram::new(10000),
            last_update: Instant::now(),
        }
    }

    /// Record an event being processed
    pub fn record_event(&mut self, bytes: usize, latency: Duration) {
        self.events_processed += 1;
        self.bytes_processed += bytes as u64;
        self.latency_histogram.record(latency);
        self.last_update = Instant::now();
    }

    /// Record an error
    pub fn record_error(&mut self) {
        self.errors += 1;
        self.last_update = Instant::now();
    }

    /// Get metrics snapshot
    pub fn snapshot(&self) -> OperatorMetricsSnapshot {
        let percentiles = self.latency_histogram.percentiles();
        let throughput = if self.last_update.elapsed().as_secs() > 0 {
            self.events_processed / self.last_update.elapsed().as_secs()
        } else {
            0
        };

        OperatorMetricsSnapshot {
            operator_id: self.operator_id.clone(),
            events_processed: self.events_processed,
            bytes_processed: self.bytes_processed,
            errors: self.errors,
            throughput,
            latency_percentiles: percentiles,
        }
    }
}

/// Operator metrics snapshot
#[derive(Debug, Clone)]
pub struct OperatorMetricsSnapshot {
    pub operator_id: String,
    pub events_processed: u64,
    pub bytes_processed: u64,
    pub errors: u64,
    pub throughput: u64,
    pub latency_percentiles: Percentiles,
}

/// Distributed metrics aggregator
pub struct DistributedMetricsAggregator {
    /// Metrics by node
    node_metrics: Arc<RwLock<HashMap<NodeId, NodeMetrics>>>,
    /// Metrics by operator
    operator_metrics: Arc<RwLock<HashMap<String, OperatorMetrics>>>,
    /// Global aggregated metrics
    global_metrics: Arc<RwLock<GlobalMetrics>>,
}

/// Node-level metrics
pub struct NodeMetrics {
    node_id: NodeId,
    events_processed: u64,
    bytes_processed: u64,
    errors: u64,
    last_update: Instant,
}

impl NodeMetrics {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            events_processed: 0,
            bytes_processed: 0,
            errors: 0,
            last_update: Instant::now(),
        }
    }

    fn record_event(&mut self, bytes: usize) {
        self.events_processed += 1;
        self.bytes_processed += bytes as u64;
        self.last_update = Instant::now();
    }

    fn record_error(&mut self) {
        self.errors += 1;
        self.last_update = Instant::now();
    }
}

/// Global aggregated metrics
pub struct GlobalMetrics {
    total_events: u64,
    total_bytes: u64,
    total_errors: u64,
    #[allow(dead_code)]
    node_count: usize,
    #[allow(dead_code)]
    operator_count: usize,
}

impl DistributedMetricsAggregator {
    /// Create a new distributed metrics aggregator
    pub fn new() -> Self {
        Self {
            node_metrics: Arc::new(RwLock::new(HashMap::new())),
            operator_metrics: Arc::new(RwLock::new(HashMap::new())),
            global_metrics: Arc::new(RwLock::new(GlobalMetrics {
                total_events: 0,
                total_bytes: 0,
                total_errors: 0,
                node_count: 0,
                operator_count: 0,
            })),
        }
    }

    /// Record metrics for a node
    pub async fn record_node_event(&self, node_id: NodeId, bytes: usize) {
        let mut node_metrics = self.node_metrics.write().await;
        let node_metric = node_metrics
            .entry(node_id)
            .or_insert_with(|| NodeMetrics::new(node_id));
        node_metric.record_event(bytes);

        // Update global metrics
        let mut global = self.global_metrics.write().await;
        global.total_events += 1;
        global.total_bytes += bytes as u64;
    }

    /// Record error for a node
    pub async fn record_node_error(&self, node_id: NodeId) {
        let mut node_metrics = self.node_metrics.write().await;
        let node_metric = node_metrics
            .entry(node_id)
            .or_insert_with(|| NodeMetrics::new(node_id));
        node_metric.record_error();

        let mut global = self.global_metrics.write().await;
        global.total_errors += 1;
    }

    /// Record metrics for an operator
    pub async fn record_operator_event(
        &self,
        operator_id: impl Into<String>,
        bytes: usize,
        latency: Duration,
    ) {
        let operator_id = operator_id.into();
        let mut operator_metrics = self.operator_metrics.write().await;
        let operator_metric = operator_metrics
            .entry(operator_id.clone())
            .or_insert_with(|| OperatorMetrics::new(operator_id));
        operator_metric.record_event(bytes, latency);
    }

    /// Record error for an operator
    pub async fn record_operator_error(&self, operator_id: impl Into<String>) {
        let operator_id = operator_id.into();
        let mut operator_metrics = self.operator_metrics.write().await;
        let operator_metric = operator_metrics
            .entry(operator_id)
            .or_insert_with(|| OperatorMetrics::new("unknown"));
        operator_metric.record_error();
    }

    /// Get aggregated metrics snapshot
    pub async fn get_aggregated_snapshot(&self) -> AggregatedMetricsSnapshot {
        let node_metrics = self.node_metrics.read().await;
        let operator_metrics = self.operator_metrics.read().await;
        let global = self.global_metrics.read().await;

        let node_snapshots: Vec<_> = node_metrics
            .values()
            .map(|m| NodeMetricsSnapshot {
                node_id: m.node_id,
                events_processed: m.events_processed,
                bytes_processed: m.bytes_processed,
                errors: m.errors,
            })
            .collect();

        let operator_snapshots: Vec<_> = operator_metrics.values().map(|m| m.snapshot()).collect();

        AggregatedMetricsSnapshot {
            global: GlobalMetricsSnapshot {
                total_events: global.total_events,
                total_bytes: global.total_bytes,
                total_errors: global.total_errors,
                node_count: node_metrics.len(),
                operator_count: operator_metrics.len(),
            },
            nodes: node_snapshots,
            operators: operator_snapshots,
        }
    }

    /// Get operator metrics
    pub async fn get_operator_metrics(&self, operator_id: &str) -> Option<OperatorMetricsSnapshot> {
        let operator_metrics = self.operator_metrics.read().await;
        operator_metrics.get(operator_id).map(|m| m.snapshot())
    }

    /// Get all operator metrics
    pub async fn get_all_operator_metrics(&self) -> Vec<OperatorMetricsSnapshot> {
        let operator_metrics = self.operator_metrics.read().await;
        operator_metrics.values().map(|m| m.snapshot()).collect()
    }
}

impl Default for DistributedMetricsAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregated metrics snapshot
#[derive(Debug, Clone)]
pub struct AggregatedMetricsSnapshot {
    pub global: GlobalMetricsSnapshot,
    pub nodes: Vec<NodeMetricsSnapshot>,
    pub operators: Vec<OperatorMetricsSnapshot>,
}

/// Global metrics snapshot
#[derive(Debug, Clone)]
pub struct GlobalMetricsSnapshot {
    pub total_events: u64,
    pub total_bytes: u64,
    pub total_errors: u64,
    #[allow(dead_code)]
    pub node_count: usize,
    #[allow(dead_code)]
    pub operator_count: usize,
}

/// Node metrics snapshot
#[derive(Debug, Clone)]
pub struct NodeMetricsSnapshot {
    pub node_id: NodeId,
    pub events_processed: u64,
    pub bytes_processed: u64,
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_histogram() {
        let mut histogram = LatencyHistogram::new(1000);

        // Add some samples
        for i in 1..=100 {
            histogram.record(Duration::from_micros(i));
        }

        let percentiles = histogram.percentiles();
        assert!(percentiles.p50.is_some());
        assert!(percentiles.p95.is_some());
        assert!(percentiles.p99.is_some());
        assert_eq!(percentiles.count, 100);
    }

    #[tokio::test]
    async fn test_operator_metrics() {
        let mut metrics = OperatorMetrics::new("test_operator");
        metrics.record_event(100, Duration::from_millis(10));
        metrics.record_event(200, Duration::from_millis(20));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_processed, 2);
        assert_eq!(snapshot.bytes_processed, 300);
    }

    #[tokio::test]
    async fn test_distributed_aggregator() {
        let aggregator = DistributedMetricsAggregator::new();
        let node_id = NodeId::from(1);

        aggregator.record_node_event(node_id, 100).await;
        aggregator
            .record_operator_event("op1", 100, Duration::from_millis(10))
            .await;

        let snapshot = aggregator.get_aggregated_snapshot().await;
        assert_eq!(snapshot.global.total_events, 1);
        assert_eq!(snapshot.operators.len(), 1);
    }
}
