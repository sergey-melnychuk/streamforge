//! Join operators for combining multiple streams
//!
//! Joins combine events from two streams based on matching keys.
//! This module supports various join types and temporal constraints.

use crate::core::{Event, EventKey, EventValue, Timestamp};
use std::collections::HashMap;
use std::time::Duration;

/// Join type determines which events are emitted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Only emit when both sides have matching keys
    Inner,
    /// Emit all left events, with nulls when right side doesn't match
    Left,
    /// Emit all right events, with nulls when left side doesn't match
    Right,
    /// Emit all events from both sides
    Outer,
}

/// Temporal join constraint limits how far apart joined events can be
#[derive(Debug, Clone, Copy)]
pub struct TemporalConstraint {
    /// Maximum time difference between left and right events
    pub max_time_diff: Duration,
}

impl TemporalConstraint {
    pub fn of(max_time_diff: Duration) -> Self {
        Self { max_time_diff }
    }

    /// Check if two timestamps are within the constraint
    pub fn within_bound(&self, left_ts: Timestamp, right_ts: Timestamp) -> bool {
        let diff = (left_ts - right_ts).abs();
        diff <= self.max_time_diff.as_millis() as i64
    }
}

/// Result of a join operation
#[derive(Debug, Clone)]
pub struct JoinedEvent {
    /// The common key
    pub key: EventKey,
    /// Value from the left side (None for right-only in outer join)
    pub left_value: Option<EventValue>,
    /// Value from the right side (None for left-only in outer join)
    pub right_value: Option<EventValue>,
    /// Timestamp of the joined event (max of both sides)
    pub timestamp: Timestamp,
}

impl JoinedEvent {
    pub fn new(
        key: EventKey,
        left_value: Option<EventValue>,
        right_value: Option<EventValue>,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            key,
            left_value,
            right_value,
            timestamp,
        }
    }

    /// Convert to a regular event with combined values
    pub fn to_event<F>(self, combiner: F) -> Event
    where
        F: Fn(Option<EventValue>, Option<EventValue>) -> EventValue,
    {
        let value = combiner(self.left_value, self.right_value);
        Event::new(self.key, value, self.timestamp)
    }

    /// Convert to event with tuple value (JSON string)
    pub fn to_event_tuple(self) -> Event {
        // Create a simple string representation
        let left_str = self.left_value.as_ref()
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "null".to_string());
        let right_str = self.right_value.as_ref()
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "null".to_string());

        let tuple_str = format!("({}, {})", left_str, right_str);
        Event::new(self.key, EventValue::from_str(tuple_str), self.timestamp)
    }
}

/// Join state maintains buffered events for matching
pub struct JoinState {
    /// Events from the left stream, grouped by key
    left: HashMap<EventKey, Vec<Event>>,
    /// Events from the right stream, grouped by key
    right: HashMap<EventKey, Vec<Event>>,
}

impl JoinState {
    pub fn new() -> Self {
        Self {
            left: HashMap::new(),
            right: HashMap::new(),
        }
    }

    /// Add an event from the left stream
    pub fn add_left(&mut self, event: Event) {
        self.left
            .entry(event.key.clone())
            .or_default()
            .push(event);
    }

    /// Add an event from the right stream
    pub fn add_right(&mut self, event: Event) {
        self.right
            .entry(event.key.clone())
            .or_default()
            .push(event);
    }

    /// Perform join based on join type and temporal constraint
    pub fn join(
        &self,
        join_type: JoinType,
        constraint: Option<TemporalConstraint>,
    ) -> Vec<JoinedEvent> {
        let mut results = Vec::new();

        match join_type {
            JoinType::Inner => {
                for (key, left_events) in &self.left {
                    if let Some(right_events) = self.right.get(key) {
                        self.join_matching(key, left_events, right_events, constraint, &mut results);
                    }
                }
            }
            JoinType::Left => {
                for (key, left_events) in &self.left {
                    if let Some(right_events) = self.right.get(key) {
                        self.join_matching(key, left_events, right_events, constraint, &mut results);
                    } else {
                        // Left side with no match
                        for left_event in left_events {
                            results.push(JoinedEvent::new(
                                key.clone(),
                                Some(left_event.value.clone()),
                                None,
                                left_event.timestamp,
                            ));
                        }
                    }
                }
            }
            JoinType::Right => {
                for (key, right_events) in &self.right {
                    if let Some(left_events) = self.left.get(key) {
                        self.join_matching(key, left_events, right_events, constraint, &mut results);
                    } else {
                        // Right side with no match
                        for right_event in right_events {
                            results.push(JoinedEvent::new(
                                key.clone(),
                                None,
                                Some(right_event.value.clone()),
                                right_event.timestamp,
                            ));
                        }
                    }
                }
            }
            JoinType::Outer => {
                // All keys from both sides
                let mut all_keys: std::collections::HashSet<_> = self.left.keys().cloned().collect();
                all_keys.extend(self.right.keys().cloned());

                for key in all_keys {
                    let left_events = self.left.get(&key);
                    let right_events = self.right.get(&key);

                    match (left_events, right_events) {
                        (Some(left), Some(right)) => {
                            self.join_matching(&key, left, right, constraint, &mut results);
                        }
                        (Some(left), None) => {
                            for left_event in left {
                                results.push(JoinedEvent::new(
                                    key.clone(),
                                    Some(left_event.value.clone()),
                                    None,
                                    left_event.timestamp,
                                ));
                            }
                        }
                        (None, Some(right)) => {
                            for right_event in right {
                                results.push(JoinedEvent::new(
                                    key.clone(),
                                    None,
                                    Some(right_event.value.clone()),
                                    right_event.timestamp,
                                ));
                            }
                        }
                        (None, None) => unreachable!(),
                    }
                }
            }
        }

        results
    }

    /// Join matching events from left and right
    fn join_matching(
        &self,
        key: &EventKey,
        left_events: &[Event],
        right_events: &[Event],
        constraint: Option<TemporalConstraint>,
        results: &mut Vec<JoinedEvent>,
    ) {
        for left_event in left_events {
            for right_event in right_events {
                // Check temporal constraint if specified
                if let Some(tc) = constraint {
                    if !tc.within_bound(left_event.timestamp, right_event.timestamp) {
                        continue;
                    }
                }

                results.push(JoinedEvent::new(
                    key.clone(),
                    Some(left_event.value.clone()),
                    Some(right_event.value.clone()),
                    left_event.timestamp.max(right_event.timestamp),
                ));
            }
        }
    }

    /// Clean up old events based on watermark (for stateful joins)
    pub fn cleanup_before(&mut self, watermark: Timestamp) {
        self.left.retain(|_, events| {
            events.retain(|e| e.timestamp >= watermark);
            !events.is_empty()
        });

        self.right.retain(|_, events| {
            events.retain(|e| e.timestamp >= watermark);
            !events.is_empty()
        });
    }
}

impl Default for JoinState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_event(key: &str, value: i64, timestamp: Timestamp) -> Event {
        Event::new(
            EventKey::from_str(key),
            EventValue::from_int(value),
            timestamp,
        )
    }

    #[test]
    fn test_inner_join() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("b", 2, 1000));
        state.add_right(create_event("a", 10, 1000));
        state.add_right(create_event("c", 30, 1000));

        let results = state.join(JoinType::Inner, None);

        // Only "a" has matches on both sides
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, EventKey::from_str("a"));
        assert_eq!(results[0].left_value.as_ref().unwrap().as_int(), Some(1));
        assert_eq!(results[0].right_value.as_ref().unwrap().as_int(), Some(10));
    }

    #[test]
    fn test_left_join() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("b", 2, 1000));
        state.add_right(create_event("a", 10, 1000));

        let results = state.join(JoinType::Left, None);

        // Both left events appear (a matched, b unmatched)
        assert_eq!(results.len(), 2);

        let a_result = results.iter().find(|r| r.key == EventKey::from_str("a")).unwrap();
        assert!(a_result.left_value.is_some());
        assert!(a_result.right_value.is_some());

        let b_result = results.iter().find(|r| r.key == EventKey::from_str("b")).unwrap();
        assert!(b_result.left_value.is_some());
        assert!(b_result.right_value.is_none());
    }

    #[test]
    fn test_temporal_constraint() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("a", 2, 5000));
        state.add_right(create_event("a", 10, 1500)); // Within 1s of first
        state.add_right(create_event("a", 20, 10000)); // Not within 1s of either

        let constraint = TemporalConstraint::of(Duration::from_secs(1));
        let results = state.join(JoinType::Inner, Some(constraint));

        // Only the first pair should match (1000, 1500) within 1s
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].left_value.as_ref().unwrap().as_int(), Some(1));
        assert_eq!(results[0].right_value.as_ref().unwrap().as_int(), Some(10));
    }

    #[test]
    fn test_outer_join() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("b", 2, 1000));
        state.add_right(create_event("a", 10, 1000));
        state.add_right(create_event("c", 30, 1000));

        let results = state.join(JoinType::Outer, None);

        // Should have: matched(a), left-only(b), right-only(c)
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_multiple_matches() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("a", 2, 2000));
        state.add_right(create_event("a", 10, 1000));
        state.add_right(create_event("a", 20, 2000));

        let results = state.join(JoinType::Inner, None);

        // Cartesian product: 2 left × 2 right = 4 results
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_cleanup() {
        let mut state = JoinState::new();

        state.add_left(create_event("a", 1, 1000));
        state.add_left(create_event("a", 2, 5000));
        state.add_right(create_event("a", 10, 2000));
        state.add_right(create_event("a", 20, 6000));

        // Clean up events before timestamp 3000
        state.cleanup_before(3000);

        let results = state.join(JoinType::Inner, None);

        // Should only have events from 5000 and 6000
        assert_eq!(results.len(), 1);
    }
}
