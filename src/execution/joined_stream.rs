//! Joined stream operations for combining two streams

use crate::core::{Event, EventValue};
use crate::error::Result;
use crate::operators::join::{JoinState, JoinType, JoinedEvent, TemporalConstraint};

/// A joined stream represents the result of joining two event streams
pub struct JoinedStream {
    results: Vec<JoinedEvent>,
}

impl JoinedStream {
    pub fn new(results: Vec<JoinedEvent>) -> Self {
        Self { results }
    }

    /// Collect all joined events
    pub async fn collect(self) -> Result<Vec<JoinedEvent>> {
        Ok(self.results)
    }

    /// Convert joined events to regular events using a combiner function
    pub async fn combine<F>(self, combiner: F) -> Result<Vec<Event>>
    where
        F: Fn(Option<EventValue>, Option<EventValue>) -> EventValue,
    {
        Ok(self
            .results
            .into_iter()
            .map(|je| je.to_event(&combiner))
            .collect())
    }

    /// Convert to events with tuple values (JSON format)
    pub async fn as_tuples(self) -> Result<Vec<Event>> {
        Ok(self
            .results
            .into_iter()
            .map(|je| je.to_event_tuple())
            .collect())
    }

    /// Sum the numeric values from both sides
    pub async fn sum_values(self) -> Result<Vec<Event>> {
        self.combine(|left, right| {
            let left_val = left.and_then(|v| v.as_float()).unwrap_or(0.0);
            let right_val = right.and_then(|v| v.as_float()).unwrap_or(0.0);
            EventValue::Float(left_val + right_val)
        })
        .await
    }

    /// Take the left value, or right if left is None
    pub async fn coalesce_left(self) -> Result<Vec<Event>> {
        self.combine(|left, right| left.or(right).unwrap_or(EventValue::Null))
            .await
    }

    /// Take the right value, or left if right is None
    pub async fn coalesce_right(self) -> Result<Vec<Event>> {
        self.combine(|left, right| right.or(left).unwrap_or(EventValue::Null))
            .await
    }

    /// Count the number of joined events
    pub async fn count(self) -> Result<usize> {
        Ok(self.results.len())
    }
}

/// Builder for configuring and executing joins
pub struct JoinBuilder {
    left_events: Vec<Event>,
    right_events: Vec<Event>,
    join_type: JoinType,
    temporal_constraint: Option<TemporalConstraint>,
}

impl JoinBuilder {
    pub fn new(left_events: Vec<Event>, right_events: Vec<Event>) -> Self {
        Self {
            left_events,
            right_events,
            join_type: JoinType::Inner,
            temporal_constraint: None,
        }
    }

    /// Set the join type (Inner, Left, Right, Outer)
    pub fn join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Add a temporal constraint (maximum time difference)
    pub fn with_temporal_constraint(mut self, constraint: TemporalConstraint) -> Self {
        self.temporal_constraint = Some(constraint);
        self
    }

    /// Execute the join
    pub async fn execute(self) -> Result<JoinedStream> {
        let mut state = JoinState::new();

        // Add all left events
        for event in self.left_events {
            state.add_left(event);
        }

        // Add all right events
        for event in self.right_events {
            state.add_right(event);
        }

        // Perform the join
        let results = state.join(self.join_type, self.temporal_constraint);

        Ok(JoinedStream::new(results))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EventKey;
    use std::time::Duration;

    fn create_event(key: &str, value: i64, timestamp: i64) -> Event {
        Event::new(
            EventKey::from_str(key),
            EventValue::from_int(value),
            timestamp,
        )
    }

    #[tokio::test]
    async fn test_inner_join_builder() {
        let left = vec![
            create_event("a", 1, 1000),
            create_event("b", 2, 1000),
        ];

        let right = vec![
            create_event("a", 10, 1000),
            create_event("c", 30, 1000),
        ];

        let joined = JoinBuilder::new(left, right)
            .join_type(JoinType::Inner)
            .execute()
            .await
            .unwrap();

        let results = joined.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, EventKey::from_str("a"));
    }

    #[tokio::test]
    async fn test_temporal_join() {
        let left = vec![
            create_event("a", 1, 1000),
            create_event("a", 2, 5000),
        ];

        let right = vec![
            create_event("a", 10, 1500),
            create_event("a", 20, 10000),
        ];

        let joined = JoinBuilder::new(left, right)
            .with_temporal_constraint(TemporalConstraint::of(Duration::from_secs(1)))
            .execute()
            .await
            .unwrap();

        let results = joined.collect().await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_sum_values() {
        let left = vec![create_event("a", 5, 1000)];
        let right = vec![create_event("a", 10, 1000)];

        let joined = JoinBuilder::new(left, right)
            .execute()
            .await
            .unwrap();

        let events = joined.sum_values().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].value.as_float(), Some(15.0));
    }

    #[tokio::test]
    async fn test_left_join() {
        let left = vec![
            create_event("a", 1, 1000),
            create_event("b", 2, 1000),
        ];

        let right = vec![create_event("a", 10, 1000)];

        let joined = JoinBuilder::new(left, right)
            .join_type(JoinType::Left)
            .execute()
            .await
            .unwrap();

        let results = joined.collect().await.unwrap();
        assert_eq!(results.len(), 2); // Both left events appear
    }
}
