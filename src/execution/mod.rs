//! Execution engine for stream processing
//!
//! This module provides the core execution model for processing streams.

pub mod stream;
pub mod pipeline;
pub mod windowed_stream;
pub mod joined_stream;
pub mod distributed;
pub mod backpressure;
pub mod watermarked_stream;
pub mod shuffle;
pub mod exactly_once;

pub use stream::Stream;
pub use pipeline::Pipeline;
pub use windowed_stream::WindowedStream;
pub use joined_stream::{JoinBuilder, JoinedStream};
pub use distributed::{DistributedContext, DistributedExecutor};
pub use backpressure::{
    BackpressureDetector, BackpressureLevel, BackpressureChannel, FlowController,
};
pub use watermarked_stream::{
    WatermarkedWindowedStream, WatermarkEmitter, LateDataConfig, LateDataPolicy,
};
pub use shuffle::{ShuffleManager, JoinShuffleCoordinator};
pub use exactly_once::{
    TransactionId, Transaction, TransactionStatus,
    TwoPhaseCommitCoordinator, IdempotentStateTracker, CheckpointCoordinator,
};
