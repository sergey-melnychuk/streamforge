//! Windowed stream operations for time-based aggregations

use crate::core::{Event, EventKey, EventValue};
use crate::error::Result;
use crate::operators::{AggregateFunction, Window, WindowAssigner};
use std::collections::HashMap;
use std::sync::Arc;

/// A windowed stream groups events into windows for aggregation
pub struct WindowedStream<W>
where
    W: WindowAssigner,
{
    events: Vec<Event>,
    assigner: Arc<W>,
}

impl<W> WindowedStream<W>
where
    W: WindowAssigner + 'static,
{
    pub fn new(events: Vec<Event>, assigner: W) -> Self {
        Self {
            events,
            assigner: Arc::new(assigner),
        }
    }

    /// Apply an aggregation function to each window
    pub async fn aggregate<A>(self, agg_fn: A) -> Result<Vec<(Window, EventKey, EventValue)>>
    where
        A: AggregateFunction,
    {
        // Group events by (window, key)
        let mut window_states: HashMap<(Window, EventKey), A::Accumulator> = HashMap::new();

        for event in self.events {
            let windows = self.assigner.assign_windows(&event);

            for window in windows {
                let key = event.key.clone();
                let state_key = (window, key);

                let acc = window_states
                    .entry(state_key)
                    .or_insert_with(|| agg_fn.create_accumulator());

                agg_fn.add(acc, &event)?;
            }
        }

        // Collect results
        let mut results = Vec::new();
        for ((window, key), acc) in window_states {
            let value = agg_fn.get_result(&acc)?;
            results.push((window, key, value));
        }

        // Sort by window start time
        results.sort_by_key(|(w, _, _)| w.start);

        Ok(results)
    }

    /// Apply aggregation and return events
    pub async fn aggregate_to_events<A>(self, agg_fn: A) -> Result<Vec<Event>>
    where
        A: AggregateFunction,
    {
        let results = self.aggregate(agg_fn).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                Event::new(key, value, window.end)
                    .with_header("window_start", window.start.to_string())
                    .with_header("window_end", window.end.to_string())
            })
            .collect())
    }

    /// Count events in each window
    pub async fn count(self) -> Result<Vec<(Window, EventKey, i64)>> {
        use crate::operators::Count;

        let results = self.aggregate(Count::new()).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                let count = value.as_int().unwrap_or(0);
                (window, key, count)
            })
            .collect())
    }

    /// Sum values in each window
    pub async fn sum(self) -> Result<Vec<(Window, EventKey, f64)>> {
        use crate::operators::Sum;

        let results = self.aggregate(Sum::new()).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                let sum = value.as_float().unwrap_or(0.0);
                (window, key, sum)
            })
            .collect())
    }

    /// Average values in each window
    pub async fn avg(self) -> Result<Vec<(Window, EventKey, Option<f64>)>> {
        use crate::operators::Avg;

        let results = self.aggregate(Avg::new()).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                let avg = value.as_float();
                (window, key, avg)
            })
            .collect())
    }

    /// Find minimum value in each window
    pub async fn min(self) -> Result<Vec<(Window, EventKey, Option<f64>)>> {
        use crate::operators::Min;

        let results = self.aggregate(Min::new()).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                let min = value.as_float();
                (window, key, min)
            })
            .collect())
    }

    /// Find maximum value in each window
    pub async fn max(self) -> Result<Vec<(Window, EventKey, Option<f64>)>> {
        use crate::operators::Max;

        let results = self.aggregate(Max::new()).await?;

        Ok(results
            .into_iter()
            .map(|(window, key, value)| {
                let max = value.as_float();
                (window, key, max)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::TumblingWindow;
    use std::time::Duration;

    fn create_event(key: &str, value: i64, timestamp: i64) -> Event {
        Event::new(
            EventKey::from_str(key),
            EventValue::from_int(value),
            timestamp,
        )
    }

    #[tokio::test]
    async fn test_windowed_count() {
        let events = vec![
            create_event("a", 1, 1000),
            create_event("a", 2, 2000),
            create_event("b", 3, 3000),
            create_event("a", 4, 61000), // Next window
            create_event("b", 5, 62000), // Next window
        ];

        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let windowed = WindowedStream::new(events, assigner);

        let results = windowed.count().await.unwrap();

        // Should have 3 results: (window1, key_a, 2), (window1, key_b, 1), (window2, key_a, 1), (window2, key_b, 1)
        assert!(results.len() >= 3);

        // Check first window counts
        let window1_a = results.iter().find(|(w, k, _)| {
            w.start == 0 && matches!(k, EventKey::String(s) if s.as_ref() == "a")
        });
        assert!(window1_a.is_some());
        assert_eq!(window1_a.unwrap().2, 2);
    }

    #[tokio::test]
    async fn test_windowed_sum() {
        let events = vec![
            create_event("a", 10, 1000),
            create_event("a", 20, 2000),
            create_event("a", 30, 3000),
        ];

        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let windowed = WindowedStream::new(events, assigner);

        let results = windowed.sum().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2, 60.0);
    }

    #[tokio::test]
    async fn test_windowed_avg() {
        let events = vec![
            create_event("a", 10, 1000),
            create_event("a", 20, 2000),
            create_event("a", 30, 3000),
        ];

        let assigner = TumblingWindow::of(Duration::from_secs(60));
        let windowed = WindowedStream::new(events, assigner);

        let results = windowed.avg().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2, Some(20.0));
    }

    #[tokio::test]
    async fn test_windowed_min_max() {
        let events = vec![
            create_event("a", 30, 1000),
            create_event("a", 10, 2000),
            create_event("a", 20, 3000),
        ];

        let assigner = TumblingWindow::of(Duration::from_secs(60));

        // Test min
        let windowed_min = WindowedStream::new(events.clone(), assigner.clone());
        let min_results = windowed_min.min().await.unwrap();
        assert_eq!(min_results.len(), 1);
        assert_eq!(min_results[0].2, Some(10.0));

        // Test max
        let windowed_max = WindowedStream::new(events, assigner);
        let max_results = windowed_max.max().await.unwrap();
        assert_eq!(max_results.len(), 1);
        assert_eq!(max_results[0].2, Some(30.0));
    }
}
