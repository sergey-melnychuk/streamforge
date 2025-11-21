//! Stream operators for transforming event streams
//!
//! Operators are composable transformations that can be applied to streams.
//! Each operator implements the `StreamOperator` trait.

pub mod aggregate;
pub mod filter;
pub mod flatmap;
pub mod join;
pub mod map;
pub mod stateful;
pub mod window;

pub use aggregate::{AggregateFunction, Avg, Count, Max, Min, Sum};
pub use filter::FilterOp;
pub use flatmap::FlatMapOp;
pub use join::{JoinState, JoinType, JoinedEvent, TemporalConstraint};
pub use map::MapOp;
pub use stateful::{CountOperator, OperatorState, StatefulOperator};
pub use window::{
    SessionWindow, SlidingWindow, TumblingWindow, Window, WindowAssigner, WindowType,
};

use crate::core::Event;
use crate::error::Result;

/// Trait for stream operators
///
/// Operators transform events as they flow through the stream.
/// They can be chained together to build complex processing pipelines.
pub trait StreamOperator: Send + Sync {
    /// Process a single event
    ///
    /// Returns:
    /// - `Ok(Some(event))` if the event should be forwarded
    /// - `Ok(None)` if the event should be filtered out
    /// - `Err(e)` if an error occurred
    fn process(&mut self, event: Event) -> Result<Option<Event>>;

    /// Process a batch of events for better performance
    ///
    /// Default implementation processes events one by one.
    /// Operators can override this for batch optimizations.
    fn process_batch(&mut self, events: Vec<Event>) -> Result<Vec<Event>> {
        let mut results = Vec::with_capacity(events.len());
        for event in events {
            if let Some(result) = self.process(event)? {
                results.push(result);
            }
        }
        Ok(results)
    }
}

/// Trait for operators that can produce multiple events from a single input
pub trait FlatMapOperator: Send + Sync {
    /// Process a single event and return zero or more output events
    fn process_flat(&mut self, event: Event) -> Result<Vec<Event>>;
}
