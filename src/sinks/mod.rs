//! Stream sinks for writing data to external systems
//!
//! Sinks are the exit points for data from StreamForge streams.

pub mod file;
pub mod idempotent;
pub mod metrics;

pub use file::FileSink;
pub use idempotent::IdempotentSink;
pub use metrics::{MetricsSink, MetricsSinkConfig};

use crate::core::Event;
use async_trait::async_trait;

/// Trait for stream sinks
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write an event to the sink
    async fn write(&mut self, event: Event) -> Result<(), SinkError>;

    /// Flush any buffered data
    async fn flush(&mut self) -> Result<(), SinkError>;

    /// Close the sink
    async fn close(&mut self) -> Result<(), SinkError>;
}

/// Sink error
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Sink error: {0}")]
    Other(String),
}
