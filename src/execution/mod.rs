//! Execution engine for stream processing
//!
//! This module provides the core execution model for processing streams.

pub mod backpressure;
pub mod distributed;
pub mod exactly_once;
pub mod joined_stream;
pub mod pipeline;
pub mod shuffle;
pub mod stream;
pub mod watermarked_stream;
pub mod windowed_stream;

pub use backpressure::{
    BackpressureChannel, BackpressureDetector, BackpressureLevel, FlowController,
};
pub use distributed::{DistributedContext, DistributedExecutor};
pub use exactly_once::{
    CheckpointCoordinator, IdempotentStateTracker, Transaction, TransactionId, TransactionStatus,
    TwoPhaseCommitCoordinator,
};
pub use joined_stream::{JoinBuilder, JoinedStream};
pub use pipeline::Pipeline;
pub use shuffle::{JoinShuffleCoordinator, ShuffleManager};
pub use stream::Stream;
pub use watermarked_stream::{
    LateDataConfig, LateDataPolicy, WatermarkEmitter, WatermarkedWindowedStream,
};
pub use windowed_stream::WindowedStream;
