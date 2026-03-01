//! State management for stateful stream operations
//!
//! Provides state backends for storing operator state with:
//! - In-memory state backend (fast, non-persistent)
//! - State backend abstraction for pluggable storage
//! - Snapshot and restore capabilities
//!
//! Future phases will implement:
//! - RocksDB state backend
//! - Checkpointing and recovery
//! - TTL management

pub mod backend;
pub mod checkpoint;
pub mod idempotent;
pub mod memory;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
pub mod ttl;

pub use backend::{StateBackend, StateError, StateResult};
pub use checkpoint::{CheckpointError, CheckpointManager, CheckpointMetadata, CheckpointResult};
pub use idempotent::IdempotentStateBackend;
pub use memory::MemoryStateBackend;
#[cfg(feature = "rocksdb")]
pub use rocksdb::RocksDBStateBackend;
pub use ttl::{CleanupPolicy, TtlConfig, TtlStateBackend};
