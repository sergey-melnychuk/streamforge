//! Streaming windowed aggregations
//!
//! Implements incremental windowed aggregations that process events
//! as they arrive, using watermarks to trigger window results.

use crate::core::{Event, EventKey, EventValue, Watermark};
use crate::error::Result;
use crate::operators::{AggregateFunction, Window, WindowAssigner};
use crate::query::ast::Aggregation;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Configuration for streaming windowed aggregations
#[derive(Debug, Clone, Copy)]
pub struct StreamingWindowConfig {
    /// Maximum allowed lateness in milliseconds
    pub allowed_lateness: i64,
    /// Maximum out-of-orderness for watermark generation
    pub max_out_of_orderness: i64,
}

impl Default for StreamingWindowConfig {
    fn default() -> Self {
        Self {
            allowed_lateness: 0,
            max_out_of_orderness: 1000, // 1 second default
        }
    }
}

/// Window state for streaming aggregations
struct WindowState<A: AggregateFunction> {
    /// Accumulator for this window
    accumulator: A::Accumulator,
    /// Whether the window has been closed/triggered
    is_closed: bool,
    /// Number of events in this window
    event_count: usize,
}

/// Streaming windowed aggregator that processes events incrementally
pub struct StreamingWindowedAggregator<W, A>
where
    W: WindowAssigner + 'static,
    A: AggregateFunction + 'static,
{
    /// Window assigner
    assigner: Arc<W>,
    /// Aggregation function
    agg_fn: Arc<A>,
    /// Window states: (window, key) -> state
    window_states: Arc<RwLock<HashMap<(Window, EventKey), WindowState<A>>>>,
    /// Current watermark
    watermark: Arc<RwLock<Watermark>>,
    /// Configuration
    config: StreamingWindowConfig,
    /// Triggered window results (ready to emit)
    triggered_results: Arc<RwLock<Vec<Event>>>,
}

impl<W, A> StreamingWindowedAggregator<W, A>
where
    W: WindowAssigner + 'static,
    A: AggregateFunction + 'static,
{
    /// Create a new streaming windowed aggregator
    pub fn new(assigner: W, agg_fn: A, config: StreamingWindowConfig) -> Self {
        Self {
            assigner: Arc::new(assigner),
            agg_fn: Arc::new(agg_fn),
            window_states: Arc::new(RwLock::new(HashMap::new())),
            watermark: Arc::new(RwLock::new(Watermark::min())),
            config,
            triggered_results: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Process a single event incrementally
    pub async fn process_event(&self, event: &Event) -> Result<()> {
        // Update watermark based on event timestamp
        self.update_watermark_from_event(event).await;

        // Assign event to windows
        let windows = self.assigner.assign_windows(event);
        let watermark = *self.watermark.read().await;

        for window in windows {
            // Check if event is late
            if self.is_late_event(event, &window, watermark) {
                debug!(
                    "Dropping late event: timestamp={}, window_end={}, watermark={}",
                    event.timestamp, window.end, watermark.timestamp()
                );
                continue;
            }

            // Check if window is already closed
            let key = event.key.clone();
            let state_key = (window, key.clone());
            
            let mut states = self.window_states.write().await;
            let state = states.entry(state_key.clone()).or_insert_with(|| {
                WindowState {
                    accumulator: self.agg_fn.create_accumulator(),
                    is_closed: false,
                    event_count: 0,
                }
            });

            // Only process if window is not closed
            if !state.is_closed {
                // Add event to accumulator
                self.agg_fn.add(&mut state.accumulator, event)?;
                state.event_count += 1;
            }
        }

        // Check for windows that should be triggered
        self.check_and_trigger_windows().await;

        Ok(())
    }

    /// Update watermark based on event timestamp
    async fn update_watermark_from_event(&self, event: &Event) {
        let watermark_timestamp = event.timestamp.saturating_sub(self.config.max_out_of_orderness);
        let new_watermark = Watermark::new(watermark_timestamp);

        let mut current = self.watermark.write().await;
        if new_watermark.timestamp() > current.timestamp() {
            current.advance(new_watermark.timestamp());
            debug!("Watermark advanced to: {}", current.timestamp());
        }
    }

    /// Check if an event is late
    fn is_late_event(&self, event: &Event, window: &Window, watermark: Watermark) -> bool {
        // Event is late if watermark has passed window end + allowed lateness
        let late_threshold = window.end + self.config.allowed_lateness;
        watermark.timestamp() > late_threshold && event.timestamp < late_threshold
    }

    /// Check for windows that should be triggered and emit results
    async fn check_and_trigger_windows(&self) {
        let watermark = *self.watermark.read().await;
        let mut states = self.window_states.write().await;
        let mut triggered = Vec::new();

        // Find windows that should be closed
        let windows_to_close: Vec<_> = states
            .iter()
            .filter(|((window, _), state)| {
                !state.is_closed
                    && watermark.timestamp() >= window.end + self.config.allowed_lateness
            })
            .map(|(key, _)| key.clone())
            .collect();

        // Trigger windows and emit results
        for state_key in windows_to_close {
            if let Some(state) = states.get_mut(&state_key) {
                if !state.is_closed && state.event_count > 0 {
                    // Get aggregation result
                    match self.agg_fn.get_result(&state.accumulator) {
                        Ok(value) => {
                            let (window, key) = state_key;
                            
                            // Create result event
                            let mut result_json = serde_json::Map::new();
                            match &value {
                                EventValue::Json(json_val) => {
                                    if let Some(obj) = json_val.as_object() {
                                        for (k, v) in obj {
                                            result_json.insert(k.clone(), v.clone());
                                        }
                                    }
                                }
                                EventValue::Int(i) => {
                                    result_json.insert("value".to_string(), json!(*i));
                                }
                                EventValue::Float(f) => {
                                    result_json.insert("value".to_string(), json!(*f));
                                }
                            EventValue::String(s) => {
                                result_json.insert("value".to_string(), json!(s.to_string()));
                            }
                                EventValue::Bool(b) => {
                                    result_json.insert("value".to_string(), json!(*b));
                                }
                                EventValue::Bytes(_) => {
                                    // Skip bytes in JSON
                                }
                                EventValue::Null => {
                                    // Skip null values
                                }
                            }
                            
                            result_json.insert("window_start".to_string(), json!(window.start));
                            result_json.insert("window_end".to_string(), json!(window.end));
                            result_json.insert("event_count".to_string(), json!(state.event_count));

                            let result_event = Event::new(
                                key,
                                EventValue::Json(json!(result_json)),
                                window.end, // Use window end as event timestamp
                            );

                            triggered.push(result_event);
                            state.is_closed = true;
                        }
                        Err(e) => {
                            warn!("Failed to get aggregation result: {}", e);
                        }
                    }
                } else {
                    // Window with no events, just mark as closed
                    state.is_closed = true;
                }
            }
        }

        // Add triggered results
        if !triggered.is_empty() {
            let mut results = self.triggered_results.write().await;
            results.extend(triggered);
        }
    }

    /// Get triggered window results (and clear them)
    pub async fn get_triggered_results(&self) -> Vec<Event> {
        let mut results = self.triggered_results.write().await;
        std::mem::take(&mut *results)
    }

    /// Get current watermark
    pub async fn current_watermark(&self) -> Watermark {
        *self.watermark.read().await
    }

    /// Force trigger all open windows (for final results)
    pub async fn trigger_all_windows(&self) -> Result<Vec<Event>> {
        let mut states = self.window_states.write().await;
        let mut results = Vec::new();

        for (state_key, state) in states.iter_mut() {
            if !state.is_closed && state.event_count > 0 {
                match self.agg_fn.get_result(&state.accumulator) {
                    Ok(value) => {
                        let (window, key) = state_key.clone();
                        
                        let mut result_json = serde_json::Map::new();
                        match &value {
                            EventValue::Json(json_val) => {
                                if let Some(obj) = json_val.as_object() {
                                    for (k, v) in obj {
                                        result_json.insert(k.clone(), v.clone());
                                    }
                                }
                            }
                            EventValue::Int(i) => {
                                result_json.insert("value".to_string(), json!(*i));
                            }
                            EventValue::Float(f) => {
                                result_json.insert("value".to_string(), json!(*f));
                            }
                            EventValue::String(s) => {
                                result_json.insert("value".to_string(), json!(s.to_string()));
                            }
                            EventValue::Bool(b) => {
                                result_json.insert("value".to_string(), json!(*b));
                            }
                            EventValue::Bytes(_) => {}
                            EventValue::Null => {}
                        }
                        
                        result_json.insert("window_start".to_string(), json!(window.start));
                        result_json.insert("window_end".to_string(), json!(window.end));
                        result_json.insert("event_count".to_string(), json!(state.event_count));

                        let result_event = Event::new(
                            key,
                            EventValue::Json(json!(result_json)),
                            window.end,
                        );

                        results.push(result_event);
                    }
                    Err(e) => {
                        warn!("Failed to get aggregation result: {}", e);
                    }
                }
                state.is_closed = true;
            }
        }

        Ok(results)
    }
}

/// Field-aware wrapper for aggregation functions
/// Extracts field value from event before passing to aggregation
pub struct FieldAwareAgg<A: AggregateFunction> {
    inner: A,
    field: Option<String>,
}

impl<A: AggregateFunction> FieldAwareAgg<A> {
    pub fn new(inner: A, field: Option<String>) -> Self {
        Self { inner, field }
    }

    fn extract_field_value(&self, event: &Event) -> Option<EventValue> {
        if let Some(ref field) = self.field {
            if let EventValue::Json(json) = &event.value {
                if let Some(v) = json.get(field) {
                    // Convert JSON value to EventValue
                    if let Some(s) = v.as_str() {
                        return Some(EventValue::String(s.to_string().into()));
                    } else if let Some(i) = v.as_i64() {
                        return Some(EventValue::Int(i));
                    } else if let Some(f) = v.as_f64() {
                        return Some(EventValue::Float(f));
                    } else if let Some(b) = v.as_bool() {
                        return Some(EventValue::Bool(b));
                    }
                }
            }
            None
        } else {
            // No field specified, use entire event value
            Some(event.value.clone())
        }
    }
}

impl<A: AggregateFunction> AggregateFunction for FieldAwareAgg<A> {
    type Accumulator = A::Accumulator;

    fn create_accumulator(&self) -> Self::Accumulator {
        self.inner.create_accumulator()
    }

    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()> {
        // Extract field value if needed
        if let Some(field_value) = self.extract_field_value(event) {
            // Create a temporary event with the extracted value
            let temp_event = Event::new(event.key.clone(), field_value, event.timestamp);
            self.inner.add(acc, &temp_event)
        } else {
            // Field not found, skip
            Ok(())
        }
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        self.inner.get_result(acc)
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        self.inner.merge(acc1, acc2)
    }
}

/// Enum wrapper for different aggregation functions
/// This allows us to store different aggregation types in the same structure
pub enum AggregationFunction {
    Count(FieldAwareAgg<crate::operators::Count>),
    Sum(FieldAwareAgg<crate::operators::Sum>),
    Avg(FieldAwareAgg<crate::operators::Avg>),
    Min(FieldAwareAgg<crate::operators::Min>),
    Max(FieldAwareAgg<crate::operators::Max>),
}

impl AggregationFunction {
    pub fn process_event(&self, _event: &Event, _window: &Window, _key: &EventKey) -> Result<()> {
        // This is a placeholder - actual implementation would need to maintain
        // window state per aggregation type
        match self {
            AggregationFunction::Count(_) => Ok(()),
            AggregationFunction::Sum(_) => Ok(()),
            AggregationFunction::Avg(_) => Ok(()),
            AggregationFunction::Min(_) => Ok(()),
            AggregationFunction::Max(_) => Ok(()),
        }
    }
}

/// Helper to create aggregation function from AST
pub fn create_aggregation_function(agg: &Aggregation) -> Result<AggregationFunction> {
    use crate::operators::{Avg, Count, Max, Min, Sum};
    
    match agg.function.to_uppercase().as_str() {
        "COUNT" => {
            let count = Count::new();
            Ok(AggregationFunction::Count(FieldAwareAgg::new(count, agg.field.clone())))
        }
        "SUM" => {
            let sum = Sum::new();
            Ok(AggregationFunction::Sum(FieldAwareAgg::new(sum, agg.field.clone())))
        }
        "AVG" => {
            let avg = Avg::new();
            Ok(AggregationFunction::Avg(FieldAwareAgg::new(avg, agg.field.clone())))
        }
        "MIN" => {
            let min = Min::new();
            Ok(AggregationFunction::Min(FieldAwareAgg::new(min, agg.field.clone())))
        }
        "MAX" => {
            let max = Max::new();
            Ok(AggregationFunction::Max(FieldAwareAgg::new(max, agg.field.clone())))
        }
        _ => {
            Err(crate::error::StreamError::ProcessingError(
                format!("Unknown aggregation function: {}", agg.function)
            ).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::{Count, TumblingWindow};
    use std::time::Duration;

    #[tokio::test]
    async fn test_streaming_windowed_count() {
        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let agg_fn = Count::new();
        let config = StreamingWindowConfig::default();
        
        let aggregator = StreamingWindowedAggregator::new(assigner, agg_fn, config);

        // Process events in first window
        let event1 = Event::new(
            EventKey::from_str("key1"),
            EventValue::from_int(1),
            1000, // timestamp
        );
        aggregator.process_event(&event1).await.unwrap();

        let event2 = Event::new(
            EventKey::from_str("key1"),
            EventValue::from_int(2),
            2000,
        );
        aggregator.process_event(&event2).await.unwrap();

        // Advance watermark past window end to trigger
        let event3 = Event::new(
            EventKey::from_str("key1"),
            EventValue::from_int(3),
            61000, // Next window, triggers previous window
        );
        aggregator.process_event(&event3).await.unwrap();

        // Get triggered results
        let results = aggregator.get_triggered_results().await;
        assert!(!results.is_empty());
    }
}

