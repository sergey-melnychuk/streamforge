//! State backend abstraction
//!
//! Defines the trait for state storage backends

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;

/// Error type for state operations
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("State not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("State error: {0}")]
    Other(String),
}

/// Result type for state operations
pub type StateResult<T> = Result<T, StateError>;

/// Trait for state storage backends
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &[u8]) -> StateResult<Option<Bytes>>;

    /// Put a key-value pair
    async fn put(&self, key: &[u8], value: Bytes) -> StateResult<()>;

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> StateResult<()>;

    /// Check if a key exists
    async fn exists(&self, key: &[u8]) -> StateResult<bool>;

    /// Get all keys with a given prefix
    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Bytes>>;

    /// Clear all state
    async fn clear(&self) -> StateResult<()>;

    /// Create a snapshot of the current state
    async fn snapshot(&self) -> StateResult<HashMap<Bytes, Bytes>>;

    /// Restore state from a snapshot
    async fn restore(&self, snapshot: HashMap<Bytes, Bytes>) -> StateResult<()>;
}
