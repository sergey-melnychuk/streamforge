//! Aggregation functions for windowed computations
//!
//! Aggregation functions combine multiple events into a single result.

use crate::core::{Event, EventValue};
use crate::error::Result;

/// Trait for aggregation functions
///
/// Aggregations maintain state and incrementally compute results
/// as events arrive.
pub trait AggregateFunction: Send + Sync {
    /// The accumulator type used to maintain state
    type Accumulator: Clone + Send;

    /// Create a new empty accumulator
    fn create_accumulator(&self) -> Self::Accumulator;

    /// Add an event to the accumulator
    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()>;

    /// Get the current result from the accumulator
    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue>;

    /// Merge two accumulators (for parallel processing)
    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator;
}

/// Sum aggregation
#[derive(Debug, Clone)]
pub struct Sum;

impl Sum {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for Sum {
    type Accumulator = f64;

    fn create_accumulator(&self) -> Self::Accumulator {
        0.0
    }

    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()> {
        if let Some(val) = event.value.as_float() {
            *acc += val;
        } else if let Some(val) = event.value.as_int() {
            *acc += val as f64;
        }
        Ok(())
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        Ok(EventValue::Float(*acc))
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        acc1 + acc2
    }
}

/// Count aggregation
#[derive(Debug, Clone)]
pub struct Count;

impl Count {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for Count {
    type Accumulator = i64;

    fn create_accumulator(&self) -> Self::Accumulator {
        0
    }

    fn add(&self, acc: &mut Self::Accumulator, _event: &Event) -> Result<()> {
        *acc += 1;
        Ok(())
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        Ok(EventValue::Int(*acc))
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        acc1 + acc2
    }
}

/// Average aggregation
#[derive(Debug, Clone)]
pub struct Avg;

#[derive(Debug, Clone)]
pub struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl Avg {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for Avg {
    type Accumulator = AvgAccumulator;

    fn create_accumulator(&self) -> Self::Accumulator {
        AvgAccumulator { sum: 0.0, count: 0 }
    }

    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()> {
        if let Some(val) = event.value.as_float() {
            acc.sum += val;
            acc.count += 1;
        } else if let Some(val) = event.value.as_int() {
            acc.sum += val as f64;
            acc.count += 1;
        }
        Ok(())
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        if acc.count == 0 {
            Ok(EventValue::Null)
        } else {
            Ok(EventValue::Float(acc.sum / acc.count as f64))
        }
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        AvgAccumulator {
            sum: acc1.sum + acc2.sum,
            count: acc1.count + acc2.count,
        }
    }
}

/// Minimum aggregation
#[derive(Debug, Clone)]
pub struct Min;

impl Min {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for Min {
    type Accumulator = Option<f64>;

    fn create_accumulator(&self) -> Self::Accumulator {
        None
    }

    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()> {
        let val = if let Some(v) = event.value.as_float() {
            Some(v)
        } else {
            event.value.as_int().map(|v| v as f64)
        };

        if let Some(v) = val {
            *acc = Some(acc.map_or(v, |current| current.min(v)));
        }
        Ok(())
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        Ok(acc.map(EventValue::Float).unwrap_or(EventValue::Null))
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        match (acc1, acc2) {
            (Some(v1), Some(v2)) => Some(v1.min(*v2)),
            (Some(v1), None) => Some(*v1),
            (None, Some(v2)) => Some(*v2),
            (None, None) => None,
        }
    }
}

/// Maximum aggregation
#[derive(Debug, Clone)]
pub struct Max;

impl Max {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for Max {
    type Accumulator = Option<f64>;

    fn create_accumulator(&self) -> Self::Accumulator {
        None
    }

    fn add(&self, acc: &mut Self::Accumulator, event: &Event) -> Result<()> {
        let val = if let Some(v) = event.value.as_float() {
            Some(v)
        } else {
            event.value.as_int().map(|v| v as f64)
        };

        if let Some(v) = val {
            *acc = Some(acc.map_or(v, |current| current.max(v)));
        }
        Ok(())
    }

    fn get_result(&self, acc: &Self::Accumulator) -> Result<EventValue> {
        Ok(acc.map(EventValue::Float).unwrap_or(EventValue::Null))
    }

    fn merge(&self, acc1: &Self::Accumulator, acc2: &Self::Accumulator) -> Self::Accumulator {
        match (acc1, acc2) {
            (Some(v1), Some(v2)) => Some(v1.max(*v2)),
            (Some(v1), None) => Some(*v1),
            (None, Some(v2)) => Some(*v2),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{EventKey, Timestamp};

    fn create_event(value: i64, timestamp: Timestamp) -> Event {
        Event::new(EventKey::None, EventValue::from_int(value), timestamp)
    }

    #[test]
    fn test_sum_aggregation() {
        let sum = Sum::new();
        let mut acc = sum.create_accumulator();

        sum.add(&mut acc, &create_event(10, 1000)).unwrap();
        sum.add(&mut acc, &create_event(20, 2000)).unwrap();
        sum.add(&mut acc, &create_event(30, 3000)).unwrap();

        let result = sum.get_result(&acc).unwrap();
        assert_eq!(result.as_float(), Some(60.0));
    }

    #[test]
    fn test_count_aggregation() {
        let count = Count::new();
        let mut acc = count.create_accumulator();

        count.add(&mut acc, &create_event(10, 1000)).unwrap();
        count.add(&mut acc, &create_event(20, 2000)).unwrap();
        count.add(&mut acc, &create_event(30, 3000)).unwrap();

        let result = count.get_result(&acc).unwrap();
        assert_eq!(result.as_int(), Some(3));
    }

    #[test]
    fn test_avg_aggregation() {
        let avg = Avg::new();
        let mut acc = avg.create_accumulator();

        avg.add(&mut acc, &create_event(10, 1000)).unwrap();
        avg.add(&mut acc, &create_event(20, 2000)).unwrap();
        avg.add(&mut acc, &create_event(30, 3000)).unwrap();

        let result = avg.get_result(&acc).unwrap();
        assert_eq!(result.as_float(), Some(20.0));
    }

    #[test]
    fn test_avg_empty() {
        let avg = Avg::new();
        let acc = avg.create_accumulator();

        let result = avg.get_result(&acc).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_min_aggregation() {
        let min = Min::new();
        let mut acc = min.create_accumulator();

        min.add(&mut acc, &create_event(30, 1000)).unwrap();
        min.add(&mut acc, &create_event(10, 2000)).unwrap();
        min.add(&mut acc, &create_event(20, 3000)).unwrap();

        let result = min.get_result(&acc).unwrap();
        assert_eq!(result.as_float(), Some(10.0));
    }

    #[test]
    fn test_max_aggregation() {
        let max = Max::new();
        let mut acc = max.create_accumulator();

        max.add(&mut acc, &create_event(10, 1000)).unwrap();
        max.add(&mut acc, &create_event(30, 2000)).unwrap();
        max.add(&mut acc, &create_event(20, 3000)).unwrap();

        let result = max.get_result(&acc).unwrap();
        assert_eq!(result.as_float(), Some(30.0));
    }

    #[test]
    fn test_merge_sum() {
        let sum = Sum::new();
        let mut acc1 = sum.create_accumulator();
        let mut acc2 = sum.create_accumulator();

        sum.add(&mut acc1, &create_event(10, 1000)).unwrap();
        sum.add(&mut acc2, &create_event(20, 2000)).unwrap();

        let merged = sum.merge(&acc1, &acc2);
        let result = sum.get_result(&merged).unwrap();
        assert_eq!(result.as_float(), Some(30.0));
    }

    #[test]
    fn test_merge_count() {
        let count = Count::new();
        let mut acc1 = count.create_accumulator();
        let mut acc2 = count.create_accumulator();

        count.add(&mut acc1, &create_event(10, 1000)).unwrap();
        count.add(&mut acc1, &create_event(20, 2000)).unwrap();
        count.add(&mut acc2, &create_event(30, 3000)).unwrap();

        let merged = count.merge(&acc1, &acc2);
        let result = count.get_result(&merged).unwrap();
        assert_eq!(result.as_int(), Some(3));
    }

    #[test]
    fn test_merge_avg() {
        let avg = Avg::new();
        let mut acc1 = avg.create_accumulator();
        let mut acc2 = avg.create_accumulator();

        avg.add(&mut acc1, &create_event(10, 1000)).unwrap();
        avg.add(&mut acc1, &create_event(20, 2000)).unwrap();
        avg.add(&mut acc2, &create_event(30, 3000)).unwrap();

        let merged = avg.merge(&acc1, &acc2);
        let result = avg.get_result(&merged).unwrap();
        assert_eq!(result.as_float(), Some(20.0));
    }
}
