//! In-memory state backend
//!
//! Fast, non-persistent state storage using concurrent hash maps

use crate::state::backend::{StateBackend, StateError, StateResult};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;

/// In-memory state backend using DashMap for concurrent access
pub struct MemoryStateBackend {
    state: DashMap<Bytes, Bytes>,
}

impl MemoryStateBackend {
    /// Create a new in-memory state backend
    pub fn new() -> Self {
        Self {
            state: DashMap::new(),
        }
    }
}

impl Default for MemoryStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackend for MemoryStateBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Bytes>> {
        let key = Bytes::copy_from_slice(key);
        Ok(self.state.get(&key).map(|v| v.value().clone()))
    }

    async fn put(&self, key: &[u8], value: Bytes) -> StateResult<()> {
        let key = Bytes::copy_from_slice(key);
        self.state.insert(key, value);
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        let key = Bytes::copy_from_slice(key);
        self.state.remove(&key);
        Ok(())
    }

    async fn exists(&self, key: &[u8]) -> StateResult<bool> {
        let key = Bytes::copy_from_slice(key);
        Ok(self.state.contains_key(&key))
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Bytes>> {
        let prefix = Bytes::copy_from_slice(prefix);
        let keys: Vec<Bytes> = self
            .state
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().clone())
            .collect();
        Ok(keys)
    }

    async fn clear(&self) -> StateResult<()> {
        self.state.clear();
        Ok(())
    }

    async fn snapshot(&self) -> StateResult<HashMap<Bytes, Bytes>> {
        let snapshot: HashMap<Bytes, Bytes> = self
            .state
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        Ok(snapshot)
    }

    async fn restore(&self, snapshot: HashMap<Bytes, Bytes>) -> StateResult<()> {
        self.state.clear();
        for (key, value) in snapshot {
            self.state.insert(key, value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_backend_basic() {
        let backend = MemoryStateBackend::new();

        // Put and get
        backend
            .put(b"key1", Bytes::from("value1"))
            .await
            .unwrap();
        let value = backend.get(b"key1").await.unwrap();
        assert_eq!(value, Some(Bytes::from("value1")));

        // Exists
        assert!(backend.exists(b"key1").await.unwrap());
        assert!(!backend.exists(b"key2").await.unwrap());

        // Delete
        backend.delete(b"key1").await.unwrap();
        assert!(!backend.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_backend_list_keys() {
        let backend = MemoryStateBackend::new();

        backend.put(b"prefix:key1", Bytes::from("v1")).await.unwrap();
        backend.put(b"prefix:key2", Bytes::from("v2")).await.unwrap();
        backend.put(b"other:key1", Bytes::from("v3")).await.unwrap();

        let keys = backend.list_keys(b"prefix:").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Bytes::from("prefix:key1")));
        assert!(keys.contains(&Bytes::from("prefix:key2")));
    }

    #[tokio::test]
    async fn test_memory_backend_snapshot() {
        let backend = MemoryStateBackend::new();

        backend.put(b"key1", Bytes::from("value1")).await.unwrap();
        backend.put(b"key2", Bytes::from("value2")).await.unwrap();

        let snapshot = backend.snapshot().await.unwrap();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(&Bytes::from("key1")), Some(&Bytes::from("value1")));
        assert_eq!(snapshot.get(&Bytes::from("key2")), Some(&Bytes::from("value2")));

        // Clear and restore
        backend.clear().await.unwrap();
        assert_eq!(backend.get(b"key1").await.unwrap(), None);

        backend.restore(snapshot).await.unwrap();
        assert_eq!(backend.get(b"key1").await.unwrap(), Some(Bytes::from("value1")));
        assert_eq!(backend.get(b"key2").await.unwrap(), Some(Bytes::from("value2")));
    }
}

