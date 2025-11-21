//! Storage layer for persistence
//!
//! Provides append-only log, indexing, and compaction for durable storage

pub mod compaction;
pub mod index;
pub mod log;

pub use compaction::LogCompactor;
pub use index::LogIndex;
pub use log::{AppendOnlyLog, LogError, LogResult};
