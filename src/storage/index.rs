//! Index for append-only log
//!
//! Provides fast lookups by key or offset

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Index entry mapping key to log offset
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Log offset where entry starts
    pub offset: u64,
    /// Entry length in bytes
    pub length: u64,
    /// Timestamp when entry was written
    pub timestamp: u64,
}

/// Index for fast log lookups
pub struct LogIndex {
    /// Key -> IndexEntry mapping
    key_index: Arc<RwLock<HashMap<Bytes, IndexEntry>>>,
    /// Offset -> Key mapping (for reverse lookup)
    offset_index: Arc<RwLock<HashMap<u64, Bytes>>>,
}

impl LogIndex {
    /// Create a new log index
    pub fn new() -> Self {
        Self {
            key_index: Arc::new(RwLock::new(HashMap::new())),
            offset_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add an entry to the index
    pub async fn add(&self, key: Bytes, offset: u64, length: u64) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entry = IndexEntry {
            offset,
            length,
            timestamp,
        };

        let mut key_idx = self.key_index.write().await;
        let mut offset_idx = self.offset_index.write().await;

        key_idx.insert(key.clone(), entry);
        offset_idx.insert(offset, key);

        debug!("Indexed entry at offset {} (length: {})", offset, length);
    }

    /// Get index entry by key
    pub async fn get_by_key(&self, key: &[u8]) -> Option<IndexEntry> {
        let key_idx = self.key_index.read().await;
        key_idx.get(key).cloned()
    }

    /// Get key by offset
    pub async fn get_key_by_offset(&self, offset: u64) -> Option<Bytes> {
        let offset_idx = self.offset_index.read().await;
        offset_idx.get(&offset).cloned()
    }

    /// Remove an entry from the index
    pub async fn remove(&self, key: &[u8]) -> Option<IndexEntry> {
        let mut key_idx = self.key_index.write().await;
        let mut offset_idx = self.offset_index.write().await;

        if let Some(entry) = key_idx.remove(key) {
            offset_idx.remove(&entry.offset);
            Some(entry)
        } else {
            None
        }
    }

    /// List all keys with a given prefix
    pub async fn list_keys(&self, prefix: &[u8]) -> Vec<Bytes> {
        let key_idx = self.key_index.read().await;
        key_idx
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Get the number of indexed entries
    pub async fn len(&self) -> usize {
        let key_idx = self.key_index.read().await;
        key_idx.len()
    }

    /// Check if the index is empty
    pub async fn is_empty(&self) -> bool {
        let key_idx = self.key_index.read().await;
        key_idx.is_empty()
    }

    /// Clear the index
    pub async fn clear(&self) {
        let mut key_idx = self.key_index.write().await;
        let mut offset_idx = self.offset_index.write().await;
        key_idx.clear();
        offset_idx.clear();
    }
}

impl Default for LogIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_index_add_and_get() {
        let index = LogIndex::new();

        let key = Bytes::from("test_key");
        index.add(key.clone(), 100, 50).await;

        let entry = index.get_by_key(b"test_key").await;
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.offset, 100);
        assert_eq!(entry.length, 50);
    }

    #[tokio::test]
    async fn test_index_list_keys() {
        let index = LogIndex::new();

        index.add(Bytes::from("prefix:key1"), 100, 50).await;
        index.add(Bytes::from("prefix:key2"), 200, 50).await;
        index.add(Bytes::from("other:key1"), 300, 50).await;

        let keys = index.list_keys(b"prefix:").await;
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Bytes::from("prefix:key1")));
        assert!(keys.contains(&Bytes::from("prefix:key2")));
    }

    #[tokio::test]
    async fn test_index_remove() {
        let index = LogIndex::new();

        let key = Bytes::from("test_key");
        index.add(key.clone(), 100, 50).await;
        assert_eq!(index.len().await, 1);

        index.remove(b"test_key").await;
        assert_eq!(index.len().await, 0);
        assert!(index.get_by_key(b"test_key").await.is_none());
    }
}
