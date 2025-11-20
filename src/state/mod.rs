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
pub mod memory;
pub mod rocksdb;
pub mod checkpoint;
pub mod idempotent;
pub mod ttl;

pub use backend::{StateBackend, StateError, StateResult};
pub use memory::MemoryStateBackend;
pub use rocksdb::RocksDBStateBackend;
pub use checkpoint::{CheckpointManager, CheckpointMetadata, CheckpointError, CheckpointResult};
pub use idempotent::IdempotentStateBackend;
pub use ttl::{TtlStateBackend, TtlConfig, CleanupPolicy};
