//! Time-to-live (TTL) and cleanup for state backends
//!
//! Provides TTL support, automatic cleanup, and state size management

use crate::state::backend::{StateBackend, StateResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// TTL configuration
#[derive(Debug, Clone, Copy)]
pub struct TtlConfig {
    /// Default TTL for entries (None = no expiration)
    pub default_ttl: Option<Duration>,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Maximum state size in bytes (None = unlimited)
    pub max_size_bytes: Option<usize>,
    /// Cleanup policy
    pub cleanup_policy: CleanupPolicy,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            default_ttl: None,
            cleanup_interval: Duration::from_secs(60),
            max_size_bytes: None,
            cleanup_policy: CleanupPolicy::TimeBased,
        }
    }
}

/// Cleanup policy for state entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    /// Time-based: remove expired entries
    TimeBased,
    /// LRU: remove least recently used entries
    Lru,
    /// Size-based: remove entries when size limit reached
    SizeBased,
    /// Combined: use multiple policies
    Combined,
}

/// State entry with metadata
#[derive(Debug, Clone)]
struct StateEntry {
    /// Entry value
    value: Bytes,
    /// Expiration timestamp (milliseconds since epoch)
    expires_at: Option<u64>,
    /// Last access timestamp (milliseconds since epoch)
    last_accessed: u64,
    /// Creation timestamp (milliseconds since epoch)
    created_at: u64,
}

impl StateEntry {
    fn new(value: Bytes, ttl: Option<Duration>) -> Self {
        let now = current_timestamp_ms();
        let expires_at = ttl.map(|ttl| now + ttl.as_millis() as u64);

        Self {
            value,
            expires_at,
            last_accessed: now,
            created_at: now,
        }
    }

    fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            current_timestamp_ms() >= expires_at
        } else {
            false
        }
    }

    fn update_access(&mut self) {
        self.last_accessed = current_timestamp_ms();
    }
}

/// TTL-enabled state backend wrapper
pub struct TtlStateBackend {
    /// Underlying state backend
    inner: Arc<dyn StateBackend>,
    /// State entries with metadata
    entries: Arc<RwLock<HashMap<Bytes, StateEntry>>>,
    /// TTL configuration
    config: TtlConfig,
    /// Current state size in bytes
    current_size: Arc<RwLock<usize>>,
}

impl TtlStateBackend {
    /// Create a new TTL state backend wrapper
    pub fn new(inner: Arc<dyn StateBackend>, config: TtlConfig) -> Self {
        let backend = Self {
            inner,
            entries: Arc::new(RwLock::new(HashMap::new())),
            config,
            current_size: Arc::new(RwLock::new(0)),
        };

        // Start background cleanup task
        backend.start_cleanup_task();

        backend
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let entries = Arc::clone(&self.entries);
        let inner = Arc::clone(&self.inner);
        let config = self.config;
        let current_size = Arc::clone(&self.current_size);
        let mut interval = interval(config.cleanup_interval);

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                Self::cleanup_expired(&entries, &inner, &current_size, config).await;
            }
        });
    }

    /// Cleanup expired entries
    async fn cleanup_expired(
        entries: &Arc<RwLock<HashMap<Bytes, StateEntry>>>,
        inner: &Arc<dyn StateBackend>,
        current_size: &Arc<RwLock<usize>>,
        config: TtlConfig,
    ) {
        let mut entries_guard = entries.write().await;
        let mut to_remove = Vec::new();
        let mut size_to_remove = 0;

        // Find expired entries
        for (key, entry) in entries_guard.iter() {
            if entry.is_expired() {
                to_remove.push(key.clone());
                size_to_remove += entry.value.len();
            }
        }

        // Remove expired entries
        for key in &to_remove {
            entries_guard.remove(key);
            if let Err(e) = inner.delete(key).await {
                warn!("Failed to delete expired key from inner backend: {}", e);
            }
        }

        // Update size
        if size_to_remove > 0 {
            let mut size = current_size.write().await;
            *size = size.saturating_sub(size_to_remove);
        }

        if !to_remove.is_empty() {
            debug!("Cleaned up {} expired entries", to_remove.len());
        }

        // Apply size-based cleanup if needed
        if let Some(max_size) = config.max_size_bytes {
            let size = *current_size.read().await;
            if size > max_size {
                Self::cleanup_by_size(
                    &mut entries_guard,
                    inner,
                    current_size,
                    size - max_size,
                    config.cleanup_policy,
                )
                .await;
            }
        }
    }

    /// Cleanup entries by size using the specified policy
    async fn cleanup_by_size(
        entries: &mut HashMap<Bytes, StateEntry>,
        inner: &Arc<dyn StateBackend>,
        current_size: &Arc<RwLock<usize>>,
        bytes_to_free: usize,
        policy: CleanupPolicy,
    ) {
        let mut to_remove = Vec::new();
        let mut freed_bytes = 0;

        match policy {
            CleanupPolicy::Lru | CleanupPolicy::Combined => {
                // Sort by last access time (oldest first)
                let mut sorted: Vec<_> = entries.iter().collect();
                sorted.sort_by_key(|(_, entry)| entry.last_accessed);

                for (key, entry) in sorted {
                    if freed_bytes >= bytes_to_free {
                        break;
                    }
                    to_remove.push(key.clone());
                    freed_bytes += entry.value.len();
                }
            }
            CleanupPolicy::TimeBased | CleanupPolicy::SizeBased => {
                // Remove oldest entries (by creation time)
                let mut sorted: Vec<_> = entries.iter().collect();
                sorted.sort_by_key(|(_, entry)| entry.created_at);

                for (key, entry) in sorted {
                    if freed_bytes >= bytes_to_free {
                        break;
                    }
                    to_remove.push(key.clone());
                    freed_bytes += entry.value.len();
                }
            }
        }

        // Remove selected entries
        for key in &to_remove {
            if let Some(_entry) = entries.remove(key) {
                if let Err(e) = inner.delete(key).await {
                    warn!("Failed to delete key from inner backend: {}", e);
                }
            }
        }

        // Update size
        if freed_bytes > 0 {
            let mut size = current_size.write().await;
            *size = size.saturating_sub(freed_bytes);
            info!("Freed {} bytes using {:?} policy", freed_bytes, policy);
        }
    }

    /// Get current state size in bytes
    pub async fn get_size(&self) -> usize {
        *self.current_size.read().await
    }

    /// Get number of entries
    pub async fn get_entry_count(&self) -> usize {
        let entries = self.entries.read().await;
        entries.len()
    }
}

#[async_trait]
impl StateBackend for TtlStateBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Bytes>> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.get_mut(&key_bytes) {
            // Check if expired
            if entry.is_expired() {
                entries.remove(&key_bytes);
                self.inner.delete(key).await?;
                return Ok(None);
            }

            // Update access time
            entry.update_access();

            // Get from inner backend
            self.inner.get(key).await
        } else {
            // Not in cache, try inner backend
            self.inner.get(key).await
        }
    }

    async fn put(&self, key: &[u8], value: Bytes) -> StateResult<()> {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_size = value.len();

        // Put in inner backend
        self.inner.put(key, value.clone()).await?;

        // Update cache
        let mut entries = self.entries.write().await;
        let old_entry = entries.insert(
            key_bytes.clone(),
            StateEntry::new(value, self.config.default_ttl),
        );

        // Update size
        let mut size = self.current_size.write().await;
        if let Some(old) = old_entry {
            *size = size.saturating_sub(old.value.len());
        }
        *size += value_size;

        // Check size limit
        if let Some(max_size) = self.config.max_size_bytes {
            let current_size_value = *size;
            if current_size_value > max_size {
                drop(size);
                drop(entries);
                // Trigger immediate cleanup
                let entries = Arc::clone(&self.entries);
                let inner = Arc::clone(&self.inner);
                let current_size = Arc::clone(&self.current_size);
                let mut entries_guard = entries.write().await;
                let size_to_free = current_size_value - max_size;
                Self::cleanup_by_size(
                    &mut entries_guard,
                    &inner,
                    &current_size,
                    size_to_free,
                    self.config.cleanup_policy,
                )
                .await;
            }
        }

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.remove(&key_bytes) {
            let mut size = self.current_size.write().await;
            *size = size.saturating_sub(entry.value.len());
        }

        self.inner.delete(key).await
    }

    async fn exists(&self, key: &[u8]) -> StateResult<bool> {
        let key_bytes = Bytes::copy_from_slice(key);
        let mut entries = self.entries.write().await;

        if let Some(entry) = entries.get_mut(&key_bytes) {
            if entry.is_expired() {
                entries.remove(&key_bytes);
                self.inner.delete(key).await?;
                return Ok(false);
            }
            entry.update_access();
        }

        self.inner.exists(key).await
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Bytes>> {
        // Clean expired entries first
        let mut entries = self.entries.write().await;
        let expired_keys: Vec<Bytes> = entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in &expired_keys {
            entries.remove(key);
            let _ = self.inner.delete(key).await;
        }

        drop(entries);
        self.inner.list_keys(prefix).await
    }

    async fn clear(&self) -> StateResult<()> {
        let mut entries = self.entries.write().await;
        entries.clear();
        let mut size = self.current_size.write().await;
        *size = 0;
        self.inner.clear().await
    }

    async fn snapshot(&self) -> StateResult<HashMap<Bytes, Bytes>> {
        // Clean expired entries before snapshot
        let mut entries = self.entries.write().await;
        let expired_keys: Vec<Bytes> = entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in &expired_keys {
            entries.remove(key);
            let _ = self.inner.delete(key).await;
        }

        drop(entries);
        self.inner.snapshot().await
    }

    async fn restore(&self, snapshot: HashMap<Bytes, Bytes>) -> StateResult<()> {
        let mut entries = self.entries.write().await;
        entries.clear();
        let mut size = self.current_size.write().await;
        *size = 0;

        // Restore to inner backend
        self.inner.restore(snapshot.clone()).await?;

        // Rebuild cache
        for (key, value) in snapshot {
            entries.insert(
                key.clone(),
                StateEntry::new(value.clone(), self.config.default_ttl),
            );
            *size += value.len();
        }

        Ok(())
    }
}

/// Helper function to get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MemoryStateBackend;

    #[tokio::test]
    async fn test_ttl_expiration() {
        let inner = Arc::new(MemoryStateBackend::new());
        let config = TtlConfig {
            default_ttl: Some(Duration::from_millis(100)),
            cleanup_interval: Duration::from_millis(50),
            max_size_bytes: None,
            cleanup_policy: CleanupPolicy::TimeBased,
        };

        let backend = TtlStateBackend::new(inner, config);

        // Put an entry
        backend.put(b"key1", Bytes::from("value1")).await.unwrap();
        assert!(backend.exists(b"key1").await.unwrap());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Entry should be expired
        assert!(!backend.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_ttl_size_limit() {
        let inner = Arc::new(MemoryStateBackend::new());
        let config = TtlConfig {
            default_ttl: None,
            cleanup_interval: Duration::from_secs(60), // Long interval, but cleanup happens immediately on put
            max_size_bytes: Some(100),
            cleanup_policy: CleanupPolicy::Lru,
        };

        let backend = TtlStateBackend::new(inner, config);

        // Add entries that exceed size limit
        // First entry: 50 bytes (total: 50)
        backend
            .put(b"key1", Bytes::from(vec![0u8; 50]))
            .await
            .unwrap();
        // Second entry: 50 bytes (total: 100, at limit)
        backend
            .put(b"key2", Bytes::from(vec![0u8; 50]))
            .await
            .unwrap();
        // Third entry: 50 bytes (total: 150, exceeds limit - should trigger cleanup)
        backend
            .put(b"key3", Bytes::from(vec![0u8; 50]))
            .await
            .unwrap();

        // Give a small moment for async cleanup to complete
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Size should be managed (should be <= 100 after cleanup)
        let size = backend.get_size().await;
        assert!(size <= 100, "Size {} exceeds limit of 100", size);

        // Verify that at least one entry was removed
        let entry_count = backend.get_entry_count().await;
        assert!(
            entry_count <= 2,
            "Expected at most 2 entries after cleanup, got {}",
            entry_count
        );
    }
}
