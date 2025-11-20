//! RocksDB state backend
//!
//! Persistent state storage using RocksDB

use crate::state::backend::{StateBackend, StateError, StateResult};
use async_trait::async_trait;
use bytes::Bytes;
use rocksdb::{DB, Options, WriteOptions};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::task;
use tracing::{debug, info};

/// RocksDB state backend for persistent storage
pub struct RocksDBStateBackend {
    db: Arc<DB>,
}

impl RocksDBStateBackend {
    /// Create a new RocksDB state backend
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StateError> {
        let path = path.as_ref();
        
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open(&opts, path)
            .map_err(|e| StateError::Other(format!("Failed to open RocksDB: {}", e)))?;

        info!("Opened RocksDB at {}", path.display());

        Ok(Self {
            db: Arc::new(db),
        })
    }

    /// Create with custom options
    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: Options,
    ) -> Result<Self, StateError> {
        let path = path.as_ref();
        
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = DB::open(&options, path)
            .map_err(|e| StateError::Other(format!("Failed to open RocksDB: {}", e)))?;

        info!("Opened RocksDB at {} with custom options", path.display());

        Ok(Self {
            db: Arc::new(db),
        })
    }
}

#[async_trait]
impl StateBackend for RocksDBStateBackend {
    async fn get(&self, key: &[u8]) -> StateResult<Option<Bytes>> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        task::spawn_blocking(move || {
            match db.get(&key) {
                Ok(Some(value)) => Ok(Some(Bytes::from(value))),
                Ok(None) => Ok(None),
                Err(e) => Err(StateError::Other(format!("RocksDB get error: {}", e))),
            }
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn put(&self, key: &[u8], value: Bytes) -> StateResult<()> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        let value = value.to_vec();

        task::spawn_blocking(move || {
            db.put(&key, &value)
                .map_err(|e| StateError::Other(format!("RocksDB put error: {}", e)))
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn delete(&self, key: &[u8]) -> StateResult<()> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        task::spawn_blocking(move || {
            db.delete(&key)
                .map_err(|e| StateError::Other(format!("RocksDB delete error: {}", e)))
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn exists(&self, key: &[u8]) -> StateResult<bool> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        task::spawn_blocking(move || {
            match db.get(&key) {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(StateError::Other(format!("RocksDB exists error: {}", e))),
            }
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn list_keys(&self, prefix: &[u8]) -> StateResult<Vec<Bytes>> {
        let db = Arc::clone(&self.db);
        let prefix = prefix.to_vec();

        task::spawn_blocking(move || {
            let mut keys = Vec::new();
            let iter = db.iterator(rocksdb::IteratorMode::Start);

            for item in iter {
                match item {
                    Ok((key, _)) => {
                        if key.starts_with(&prefix) {
                            keys.push(Bytes::from(key));
                        }
                    }
                    Err(e) => {
                        return Err(StateError::Other(format!("RocksDB iterator error: {}", e)));
                    }
                }
            }

            Ok(keys)
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn clear(&self) -> StateResult<()> {
        let db = Arc::clone(&self.db);

        task::spawn_blocking(move || {
            // Delete all keys by iterating and deleting
            let iter = db.iterator(rocksdb::IteratorMode::Start);
            let mut keys_to_delete = Vec::new();

            for item in iter {
                match item {
                    Ok((key, _)) => keys_to_delete.push(key),
                    Err(e) => {
                        return Err(StateError::Other(format!("RocksDB iterator error: {}", e)));
                    }
                }
            }

            for key in keys_to_delete {
                db.delete(&key)
                    .map_err(|e| StateError::Other(format!("RocksDB delete error: {}", e)))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn snapshot(&self) -> StateResult<HashMap<Bytes, Bytes>> {
        let db = Arc::clone(&self.db);

        task::spawn_blocking(move || {
            let mut snapshot = HashMap::new();
            let iter = db.iterator(rocksdb::IteratorMode::Start);

            for item in iter {
                match item {
                    Ok((key, value)) => {
                        snapshot.insert(Bytes::from(key), Bytes::from(value));
                    }
                    Err(e) => {
                        return Err(StateError::Other(format!("RocksDB iterator error: {}", e)));
                    }
                }
            }

            Ok(snapshot)
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }

    async fn restore(&self, snapshot: HashMap<Bytes, Bytes>) -> StateResult<()> {
        let db = Arc::clone(&self.db);

        // Clear existing data
        self.clear().await?;

        // Restore snapshot
        let snapshot = snapshot;
        task::spawn_blocking(move || {
            let mut write_opts = WriteOptions::default();
            write_opts.set_sync(true);

            for (key, value) in snapshot {
                db.put_opt(&key, &value, &write_opts)
                    .map_err(|e| StateError::Other(format!("RocksDB put error: {}", e)))?;
            }

            Ok(())
        })
        .await
        .map_err(|e| StateError::Other(format!("Task join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_rocksdb_backend_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let backend = RocksDBStateBackend::open(&db_path).unwrap();

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
    async fn test_rocksdb_backend_list_keys() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let backend = RocksDBStateBackend::open(&db_path).unwrap();

        backend.put(b"prefix:key1", Bytes::from("v1")).await.unwrap();
        backend.put(b"prefix:key2", Bytes::from("v2")).await.unwrap();
        backend.put(b"other:key1", Bytes::from("v3")).await.unwrap();

        let keys = backend.list_keys(b"prefix:").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&Bytes::from("prefix:key1")));
        assert!(keys.contains(&Bytes::from("prefix:key2")));
    }

    #[tokio::test]
    async fn test_rocksdb_backend_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let backend = RocksDBStateBackend::open(&db_path).unwrap();

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

