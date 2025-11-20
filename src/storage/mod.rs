//! Storage layer for persistence
//!
//! Provides append-only log, indexing, and compaction for durable storage

pub mod log;
pub mod index;
pub mod compaction;

pub use log::AppendOnlyLog;
pub use index::LogIndex;
pub use compaction::LogCompactor;

