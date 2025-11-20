//! StreamForge - Ultra-fast, distributed stream processing engine
//!
//! StreamForge is a high-performance stream processing engine built in Rust,
//! designed for:
//! - Ultra-fast processing (sub-millisecond latency)
//! - Zero-infrastructure distributed setup
//! - Advanced stream operations (windowing, joins, aggregations)
//! - Low-overhead persistence
//!
//! # Example
//!
//! ```no_run
//! use streamforge::prelude::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let stream = Stream::from_values(vec![1, 2, 3, 4, 5]);
//!
//! let result = stream
//!     .filter(|e| e.value.as_int().unwrap_or(0) % 2 == 0)
//!     .map(|e| {
//!         let val = e.value.as_int().unwrap_or(0) * 2;
//!         e.with_value_changed(val.into())
//!     })
//!     .collect()
//!     .await?;
//!
//! assert_eq!(result.len(), 2); // Two even numbers: 2 and 4
//! # Ok(())
//! # }
//! ```

pub mod core;
pub mod operators;
pub mod execution;
pub mod query;
pub mod sinks;
pub mod sources;
pub mod state;
pub mod storage;
pub mod metrics;
pub mod config;
pub mod distributed;
pub mod network;
pub mod tracing;

/// Re-exports of commonly used types and traits
pub mod prelude {
    pub use crate::core::{Event, EventKey, EventValue, Timestamp};
    pub use crate::operators::{FilterOp, MapOp, StreamOperator};
    pub use crate::execution::Stream;
}

/// Error types for the StreamForge library
pub mod error {
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum StreamError {
        #[error("Stream processing error: {0}")]
        ProcessingError(String),

        #[error("Serialization error: {0}")]
        SerializationError(String),

        #[error("State error: {0}")]
        StateError(String),

        #[error("Network error: {0}")]
        NetworkError(String),

        #[error("Configuration error: {0}")]
        ConfigError(String),

        #[error("Unknown error: {0}")]
        Unknown(String),
    }

    pub type Result<T> = std::result::Result<T, StreamError>;
}
