//! Metrics collector for tracking throughput, latency, and other metrics

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Collects metrics about stream processing performance
pub struct MetricsCollector {
    name: String,
    /// Total events processed
    events_processed: Arc<AtomicU64>,
    /// Events processed per second
    events_per_second: Arc<AtomicU64>,
    /// Total bytes processed
    bytes_processed: Arc<AtomicU64>,
    /// Processing latency (microseconds)
    latency_sum: Arc<AtomicU64>,
    /// Number of latency samples
    latency_count: Arc<AtomicU64>,
    /// Start time for rate calculation
    start_time: Instant,
    /// Last update time
    last_update: Arc<std::sync::Mutex<Instant>>,
}

impl MetricsCollector {
    pub fn new(name: impl Into<String>) -> Self {
        let now = Instant::now();
        Self {
            name: name.into(),
            events_processed: Arc::new(AtomicU64::new(0)),
            events_per_second: Arc::new(AtomicU64::new(0)),
            bytes_processed: Arc::new(AtomicU64::new(0)),
            latency_sum: Arc::new(AtomicU64::new(0)),
            latency_count: Arc::new(AtomicU64::new(0)),
            start_time: now,
            last_update: Arc::new(std::sync::Mutex::new(now)),
        }
    }

    /// Record an event being processed
    pub fn record_event(&self, bytes: usize) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.update_rate();
    }

    /// Record processing latency
    pub fn record_latency(&self, latency: Duration) {
        let latency_us = latency.as_micros() as u64;
        self.latency_sum.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update events per second rate
    fn update_rate(&self) {
        let mut last_update = self.last_update.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_update);

        if elapsed >= Duration::from_secs(1) {
            let total_elapsed = now.duration_since(self.start_time);
            if total_elapsed.as_secs() > 0 {
                let total_events = self.events_processed.load(Ordering::Relaxed);
                let rate = total_events / total_elapsed.as_secs();
                self.events_per_second.store(rate, Ordering::Relaxed);
            }
            *last_update = now;
        }
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let events_processed = self.events_processed.load(Ordering::Relaxed);
        let events_per_second = self.events_per_second.load(Ordering::Relaxed);
        let bytes_processed = self.bytes_processed.load(Ordering::Relaxed);

        let latency_sum = self.latency_sum.load(Ordering::Relaxed);
        let latency_count = self.latency_count.load(Ordering::Relaxed);
        let avg_latency_us = if latency_count > 0 {
            latency_sum / latency_count
        } else {
            0
        };

        let uptime = self.start_time.elapsed();

        MetricsSnapshot {
            name: self.name.clone(),
            events_processed,
            events_per_second,
            bytes_processed,
            avg_latency_us,
            latency_count,
            uptime_seconds: uptime.as_secs(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.events_processed.store(0, Ordering::Relaxed);
        self.events_per_second.store(0, Ordering::Relaxed);
        self.bytes_processed.store(0, Ordering::Relaxed);
        self.latency_sum.store(0, Ordering::Relaxed);
        self.latency_count.store(0, Ordering::Relaxed);
        let now = Instant::now();
        *self.last_update.lock().unwrap() = now;
    }
}

/// Metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub name: String,
    pub events_processed: u64,
    pub events_per_second: u64,
    pub bytes_processed: u64,
    pub avg_latency_us: u64,
    pub latency_count: u64,
    pub uptime_seconds: u64,
}

impl MetricsSnapshot {
    /// Format as Prometheus metrics
    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP streamforge_events_processed_total Total number of events processed\n\
             # TYPE streamforge_events_processed_total counter\n\
             streamforge_events_processed_total{{collector=\"{}\"}} {}\n\
             # HELP streamforge_events_per_second Events processed per second\n\
             # TYPE streamforge_events_per_second gauge\n\
             streamforge_events_per_second{{collector=\"{}\"}} {}\n\
             # HELP streamforge_bytes_processed_total Total bytes processed\n\
             # TYPE streamforge_bytes_processed_total counter\n\
             streamforge_bytes_processed_total{{collector=\"{}\"}} {}\n\
             # HELP streamforge_latency_avg_us Average latency in microseconds\n\
             # TYPE streamforge_latency_avg_us gauge\n\
             streamforge_latency_avg_us{{collector=\"{}\"}} {}\n\
             # HELP streamforge_uptime_seconds Uptime in seconds\n\
             # TYPE streamforge_uptime_seconds gauge\n\
             streamforge_uptime_seconds{{collector=\"{}\"}} {}\n",
            self.name,
            self.events_processed,
            self.name,
            self.events_per_second,
            self.name,
            self.bytes_processed,
            self.name,
            self.avg_latency_us,
            self.name,
            self.uptime_seconds,
        )
    }

    /// Format as JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

impl serde::Serialize for MetricsSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MetricsSnapshot", 7)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("events_processed", &self.events_processed)?;
        state.serialize_field("events_per_second", &self.events_per_second)?;
        state.serialize_field("bytes_processed", &self.bytes_processed)?;
        state.serialize_field("avg_latency_us", &self.avg_latency_us)?;
        state.serialize_field("latency_count", &self.latency_count)?;
        state.serialize_field("uptime_seconds", &self.uptime_seconds)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_metrics_collection() {
        let collector = MetricsCollector::new("test");

        collector.record_event(100);
        collector.record_event(200);
        collector.record_latency(Duration::from_micros(50));
        collector.record_latency(Duration::from_micros(100));

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.events_processed, 2);
        assert_eq!(snapshot.bytes_processed, 300);
        assert_eq!(snapshot.latency_count, 2);
    }

    #[test]
    fn test_metrics_reset() {
        let collector = MetricsCollector::new("test");

        collector.record_event(100);
        collector.reset();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.events_processed, 0);
    }
}
