//! Watermark generation from event timestamps
//!
//! Generates watermarks based on observed event timestamps with configurable
//! out-of-orderness tolerance.

use crate::core::{Event, Timestamp, Watermark};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};

/// Strategy for generating watermarks from event timestamps
pub trait WatermarkStrategy: Send + Sync {
    /// Generate a watermark based on observed event timestamps
    fn generate_watermark(&self, timestamps: &[Timestamp]) -> Option<Watermark>;

    /// Get the maximum out-of-orderness allowed (in milliseconds)
    fn max_out_of_orderness(&self) -> i64;
}

/// Periodic watermark generator with bounded out-of-orderness
pub struct PeriodicWatermarkGenerator {
    /// Maximum out-of-orderness in milliseconds
    max_out_of_orderness: i64,
    /// Current watermark
    current_watermark: Arc<RwLock<Watermark>>,
    /// Strategy for generating watermarks
    strategy: Box<dyn WatermarkStrategy>,
}

impl PeriodicWatermarkGenerator {
    /// Create a new periodic watermark generator
    pub fn new(max_out_of_orderness: i64) -> Self {
        let strategy = Box::new(BoundedOutOfOrderStrategy::new(max_out_of_orderness));
        Self {
            max_out_of_orderness,
            current_watermark: Arc::new(RwLock::new(Watermark::min())),
            strategy,
        }
    }

    /// Create with a custom strategy
    pub fn with_strategy(strategy: Box<dyn WatermarkStrategy>) -> Self {
        let max_out_of_orderness = strategy.max_out_of_orderness();
        Self {
            max_out_of_orderness,
            current_watermark: Arc::new(RwLock::new(Watermark::min())),
            strategy,
        }
    }

    /// Update watermark based on event timestamps
    pub async fn update(&self, timestamps: &[Timestamp]) -> Option<Watermark> {
        if timestamps.is_empty() {
            return None;
        }

        if let Some(new_watermark) = self.strategy.generate_watermark(timestamps) {
            let mut current = self.current_watermark.write().await;

            // Only advance watermark (never go backwards)
            if new_watermark.timestamp() > current.timestamp() {
                let old_timestamp = current.timestamp();
                current.advance(new_watermark.timestamp());
                debug!(
                    "Watermark advanced: {} -> {}",
                    old_timestamp,
                    current.timestamp()
                );
                Some(*current)
            } else {
                trace!(
                    "Watermark not advanced: {} (new: {})",
                    current.timestamp(),
                    new_watermark.timestamp()
                );
                None
            }
        } else {
            None
        }
    }

    /// Update watermark from a single event
    pub async fn update_from_event(&self, event: &Event) -> Option<Watermark> {
        self.update(&[event.timestamp]).await
    }

    /// Get the current watermark
    pub async fn current_watermark(&self) -> Watermark {
        *self.current_watermark.read().await
    }

    /// Get the maximum out-of-orderness
    pub fn max_out_of_orderness(&self) -> i64 {
        self.max_out_of_orderness
    }
}

/// Bounded out-of-orderness watermark strategy
///
/// Generates watermarks by taking the minimum observed timestamp and
/// subtracting the maximum out-of-orderness tolerance.
pub struct BoundedOutOfOrderStrategy {
    max_out_of_orderness: i64,
}

impl BoundedOutOfOrderStrategy {
    pub fn new(max_out_of_orderness: i64) -> Self {
        Self {
            max_out_of_orderness,
        }
    }
}

impl WatermarkStrategy for BoundedOutOfOrderStrategy {
    fn generate_watermark(&self, timestamps: &[Timestamp]) -> Option<Watermark> {
        if timestamps.is_empty() {
            return None;
        }

        // Find the maximum timestamp (most recent event)
        let max_timestamp = timestamps.iter().max().copied()?;

        // Watermark is max_timestamp - max_out_of_orderness
        let watermark_timestamp = max_timestamp.saturating_sub(self.max_out_of_orderness);

        Some(Watermark::new(watermark_timestamp))
    }

    fn max_out_of_orderness(&self) -> i64 {
        self.max_out_of_orderness
    }
}

/// Per-partition watermark tracker
///
/// Tracks watermarks per partition and generates global watermarks
/// by taking the minimum across all partitions.
pub struct PartitionedWatermarkTracker {
    /// Watermark per partition
    partition_watermarks: Arc<RwLock<HashMap<u32, Watermark>>>,
    /// Maximum out-of-orderness
    max_out_of_orderness: i64,
}

impl PartitionedWatermarkTracker {
    /// Create a new partitioned watermark tracker
    pub fn new(max_out_of_orderness: i64) -> Self {
        Self {
            partition_watermarks: Arc::new(RwLock::new(HashMap::new())),
            max_out_of_orderness,
        }
    }

    /// Update watermark for a specific partition
    pub async fn update_partition(
        &self,
        partition: u32,
        event_timestamp: Timestamp,
    ) -> Option<Watermark> {
        let watermark_timestamp = event_timestamp.saturating_sub(self.max_out_of_orderness);
        let new_watermark = Watermark::new(watermark_timestamp);

        let mut watermarks = self.partition_watermarks.write().await;
        let partition_wm = watermarks.entry(partition).or_insert_with(Watermark::min);

        // Only advance if new watermark is later
        if new_watermark.timestamp() > partition_wm.timestamp() {
            partition_wm.advance(new_watermark.timestamp());
        }

        // Return global watermark (minimum across all partitions)
        Some(self.get_global_watermark_internal(&watermarks))
    }

    /// Get the global watermark (minimum across all partitions)
    pub async fn get_global_watermark(&self) -> Watermark {
        let watermarks = self.partition_watermarks.read().await;
        self.get_global_watermark_internal(&watermarks)
    }

    fn get_global_watermark_internal(&self, watermarks: &HashMap<u32, Watermark>) -> Watermark {
        if watermarks.is_empty() {
            return Watermark::min();
        }

        // Global watermark is the minimum across all partitions
        watermarks
            .values()
            .min()
            .copied()
            .unwrap_or_else(Watermark::min)
    }

    /// Get watermark for a specific partition
    pub async fn get_partition_watermark(&self, partition: u32) -> Watermark {
        let watermarks = self.partition_watermarks.read().await;
        watermarks
            .get(&partition)
            .copied()
            .unwrap_or_else(Watermark::min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};

    #[tokio::test]
    async fn test_watermark_generator() {
        let generator = PeriodicWatermarkGenerator::new(1000);

        // Initially at minimum
        assert_eq!(generator.current_watermark().await.timestamp(), i64::MIN);

        // Update with timestamp 5000, watermark should be 4000 (5000 - 1000)
        let event = Event::new(EventKey::None, EventValue::from_int(1), 5000);
        let wm = generator.update_from_event(&event).await;
        assert!(wm.is_some());
        assert_eq!(wm.unwrap().timestamp(), 4000);
    }

    #[tokio::test]
    async fn test_watermark_advance() {
        let generator = PeriodicWatermarkGenerator::new(1000);

        // First event
        let event1 = Event::new(EventKey::None, EventValue::from_int(1), 5000);
        generator.update_from_event(&event1).await;

        // Later event should advance watermark
        let event2 = Event::new(EventKey::None, EventValue::from_int(2), 10000);
        let wm = generator.update_from_event(&event2).await;
        assert_eq!(wm.unwrap().timestamp(), 9000);

        // Earlier event should not advance watermark
        let event3 = Event::new(EventKey::None, EventValue::from_int(3), 8000);
        let wm = generator.update_from_event(&event3).await;
        assert!(wm.is_none()); // Should not advance
    }

    #[tokio::test]
    async fn test_partitioned_watermark_tracker() {
        let tracker = PartitionedWatermarkTracker::new(1000);

        // Update partition 0
        tracker.update_partition(0, 5000).await;
        let global = tracker.get_global_watermark().await;
        assert_eq!(global.timestamp(), 4000);

        // Update partition 1 with later timestamp
        tracker.update_partition(1, 10000).await;
        let global = tracker.get_global_watermark().await;
        // Global should still be 4000 (minimum)
        assert_eq!(global.timestamp(), 4000);

        // Update partition 0 with later timestamp
        tracker.update_partition(0, 15000).await;
        let global = tracker.get_global_watermark().await;
        // Global should now be 9000 (minimum of 14000 and 9000)
        assert_eq!(global.timestamp(), 9000);
    }
}
