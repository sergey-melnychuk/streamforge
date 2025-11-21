//! Core abstractions for stream processing
//!
//! This module contains the fundamental building blocks:
//! - Event model
//! - Partitioning logic
//! - Time and watermark handling

pub mod event;
pub mod partition;
pub mod time;
pub mod watermark;
pub mod watermark_generator;

pub use event::{Event, EventKey, EventValue, Timestamp};
pub use partition::{Partition, PartitionKey, Partitioner};
pub use time::{EventTime, ProcessingTime, TimeCharacteristic};
pub use watermark::Watermark;
pub use watermark_generator::{
    BoundedOutOfOrderStrategy, PartitionedWatermarkTracker, PeriodicWatermarkGenerator,
    WatermarkStrategy,
};
