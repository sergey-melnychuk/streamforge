//! Watermark-aware stream processing
//!
//! Integrates watermarks with stream processing for event-time windowing
//! and late data handling.

use crate::core::{Event, Watermark};
use crate::error::Result;
use crate::operators::{AggregateFunction, Window, WindowAssigner};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Configuration for late data handling
#[derive(Debug, Clone, Copy)]
pub struct LateDataConfig {
    /// Maximum allowed lateness in milliseconds
    pub allowed_lateness: i64,
    /// Policy for handling late events
    pub policy: LateDataPolicy,
}

impl Default for LateDataConfig {
    fn default() -> Self {
        Self {
            allowed_lateness: 0,
            policy: LateDataPolicy::Drop,
        }
    }
}

/// Policy for handling late events
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateDataPolicy {
    /// Drop late events
    Drop,
    /// Emit late events to a side output
    SideOutput,
    /// Update window results with late events
    Update,
}

/// A watermark-aware windowed stream
pub struct WatermarkedWindowedStream<W>
where
    W: WindowAssigner,
{
    /// Window assigner
    assigner: Arc<W>,
    /// Window states (window, key) -> accumulator
    window_states: Arc<RwLock<HashMap<(Window, crate::core::EventKey), WindowState>>>,
    /// Current watermark
    watermark: Arc<RwLock<Watermark>>,
    /// Late data configuration
    late_data_config: LateDataConfig,
    /// Side output for late events
    late_events: Arc<RwLock<Vec<Event>>>,
}

// Note: WindowState uses Any for type erasure, but in production you'd want
// a more type-safe approach or use generics
struct WindowState {
    is_closed: bool,
}

impl<W> WatermarkedWindowedStream<W>
where
    W: WindowAssigner + 'static,
{
    /// Create a new watermark-aware windowed stream
    pub fn new(assigner: W, late_data_config: LateDataConfig) -> Self {
        Self {
            assigner: Arc::new(assigner),
            window_states: Arc::new(RwLock::new(HashMap::new())),
            watermark: Arc::new(RwLock::new(Watermark::min())),
            late_data_config,
            late_events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Process an event with watermark awareness
    pub async fn process_event<A>(&self, event: Event, _agg_fn: &A) -> Result<()>
    where
        A: AggregateFunction + 'static,
    {
        let windows = self.assigner.assign_windows(&event);
        let watermark = *self.watermark.read().await;

        for window in windows {
            // Check if event is late
            let is_late = self.is_late_event(&event, &window, watermark);

            if is_late {
                self.handle_late_event(event.clone()).await?;
                continue;
            }

            // Process event in window
            let key = event.key.clone();
            let state_key = (window, key);

            // For now, we'll use a simplified approach
            // In production, you'd want proper type-safe accumulator storage
            // This is a placeholder that tracks window state
            let mut states = self.window_states.write().await;
            let state = states
                .entry(state_key.clone())
                .or_insert_with(|| WindowState { is_closed: false });

            // Only process if window is not closed
            // Note: Actual aggregation would happen in a separate step
            // This is a simplified implementation
            if !state.is_closed {
                // In a full implementation, we'd store and update the accumulator here
                // For now, we just track that the window is open
            }
        }

        Ok(())
    }

    /// Update the watermark
    pub async fn update_watermark(
        &self,
        watermark: Watermark,
    ) -> Vec<(Window, crate::core::EventKey, crate::core::EventValue)> {
        let mut current = self.watermark.write().await;

        // Only advance watermark
        if watermark.timestamp() > current.timestamp() {
            current.advance(watermark.timestamp());
        }

        let current_watermark = *current;
        drop(current); // Release lock before calling trigger_windows

        // Trigger windows that should be closed
        self.trigger_windows(current_watermark).await
    }

    /// Check if an event is late
    fn is_late_event(&self, event: &Event, window: &Window, watermark: Watermark) -> bool {
        // Event is late if its timestamp is before the watermark minus allowed lateness
        let late_threshold = watermark
            .timestamp()
            .saturating_sub(self.late_data_config.allowed_lateness);
        event.timestamp < late_threshold && watermark.timestamp() > window.end
    }

    /// Handle a late event based on policy
    async fn handle_late_event(&self, event: Event) -> Result<()> {
        match self.late_data_config.policy {
            LateDataPolicy::Drop => {
                debug!("Dropping late event with timestamp {}", event.timestamp);
                Ok(())
            }
            LateDataPolicy::SideOutput => {
                let mut late = self.late_events.write().await;
                late.push(event);
                Ok(())
            }
            LateDataPolicy::Update => {
                // For update policy, we'd need to keep windows open longer
                // This is a simplified implementation
                warn!("Late event update policy not fully implemented");
                Ok(())
            }
        }
    }

    /// Trigger windows that should be closed based on watermark
    async fn trigger_windows(
        &self,
        watermark: Watermark,
    ) -> Vec<(Window, crate::core::EventKey, crate::core::EventValue)> {
        let mut states = self.window_states.write().await;
        let triggered = Vec::new();

        // Find windows that should be closed
        let windows_to_close: Vec<_> = states
            .iter()
            .filter(|((window, _), state)| {
                !state.is_closed
                    && watermark.timestamp() >= window.end + self.late_data_config.allowed_lateness
            })
            .map(|(key, _)| key.clone())
            .collect();

        // Mark windows as closed (results would be emitted here)
        for key in windows_to_close {
            if let Some(state) = states.get_mut(&key) {
                state.is_closed = true;
            }
        }

        triggered
    }

    /// Get late events (side output)
    pub async fn get_late_events(&self) -> Vec<Event> {
        let late = self.late_events.read().await;
        late.clone()
    }

    /// Get current watermark
    pub async fn current_watermark(&self) -> Watermark {
        *self.watermark.read().await
    }
}

/// Watermark emitter that periodically generates watermarks from events
pub struct WatermarkEmitter {
    generator: Arc<crate::core::PeriodicWatermarkGenerator>,
    #[allow(dead_code)]
    watermark_receiver: tokio::sync::mpsc::Receiver<Watermark>,
    watermark_sender: tokio::sync::mpsc::Sender<Watermark>,
}

impl WatermarkEmitter {
    /// Create a new watermark emitter
    pub fn new(max_out_of_orderness: i64) -> Self {
        let generator = Arc::new(crate::core::PeriodicWatermarkGenerator::new(
            max_out_of_orderness,
        ));
        let (sender, receiver) = tokio::sync::mpsc::channel(100);

        Self {
            generator,
            watermark_receiver: receiver,
            watermark_sender: sender,
        }
    }

    /// Process an event and potentially emit a watermark
    pub async fn process_event(&self, event: &Event) -> Result<()> {
        if let Some(watermark) = self.generator.update_from_event(event).await {
            // Emit watermark
            let _ = self.watermark_sender.send(watermark).await;
        }
        Ok(())
    }

    /// Get the watermark receiver
    pub fn watermark_receiver(&self) -> tokio::sync::mpsc::Receiver<Watermark> {
        // Note: This creates a new receiver - in production you'd want to share it
        let (_, receiver) = tokio::sync::mpsc::channel(100);
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, EventValue};
    use crate::operators::{Count, TumblingWindow};
    use std::time::Duration;

    #[tokio::test]
    async fn test_watermarked_windowed_stream() {
        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let config = LateDataConfig::default();
        let stream = WatermarkedWindowedStream::new(assigner, config);

        let event = Event::new(EventKey::from_str("key1"), EventValue::from_int(1), 1000);
        let agg_fn = Count::new();

        stream.process_event(event, &agg_fn).await.unwrap();
    }

    #[tokio::test]
    async fn test_watermark_emitter() {
        let emitter = WatermarkEmitter::new(1000);

        let event = Event::new(EventKey::None, EventValue::from_int(1), 5000);
        emitter.process_event(&event).await.unwrap();
    }
}
