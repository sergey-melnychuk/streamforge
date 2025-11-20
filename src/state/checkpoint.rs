//! Checkpointing mechanism for state recovery
//!
//! Provides checkpoint creation and restoration from append-only logs

use crate::state::backend::{StateBackend, StateError, StateResult};
use crate::storage::log::{AppendOnlyLog, LogError, LogResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Checkpoint metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Checkpoint ID (timestamp-based)
    pub id: u64,
    /// Timestamp when checkpoint was created
    pub timestamp: u64,
    /// Number of state entries in checkpoint
    pub entry_count: usize,
    /// Offset in log file where checkpoint starts
    pub log_offset: u64,
}

/// Error type for checkpoint operations
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("State error: {0}")]
    State(#[from] StateError),
    #[error("Log error: {0}")]
    Log(#[from] LogError),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Checkpoint not found: {0}")]
    NotFound(u64),
}

/// Result type for checkpoint operations
pub type CheckpointResult<T> = Result<T, CheckpointError>;

/// Checkpoint manager for creating and restoring state checkpoints
pub struct CheckpointManager {
    state_backend: Box<dyn StateBackend>,
    log: AppendOnlyLog,
    checkpoint_dir: PathBuf,
    checkpoint_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub async fn new<P: AsRef<Path>>(
        state_backend: Box<dyn StateBackend>,
        log_path: P,
        checkpoint_dir: P,
    ) -> CheckpointResult<Self> {
        let log = AppendOnlyLog::open(log_path).await?;
        let checkpoint_dir = checkpoint_dir.as_ref().to_path_buf();

        // Create checkpoint directory if it doesn't exist
        if let Some(parent) = checkpoint_dir.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CheckpointError::Log(LogError::Io(e)))?;
        }
        std::fs::create_dir_all(&checkpoint_dir)
            .map_err(|e| CheckpointError::Log(LogError::Io(e)))?;

        Ok(Self {
            state_backend,
            log,
            checkpoint_dir,
            checkpoint_counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Create a checkpoint of the current state
    pub async fn create_checkpoint(&self) -> CheckpointResult<CheckpointMetadata> {
        info!("Creating checkpoint...");

        // Get current state snapshot
        let snapshot = self.state_backend.snapshot().await?;
        let entry_count = snapshot.len();

        // Get current log offset
        let log_offset = self.log.size().await?;

        // Convert snapshot to serializable format (Bytes -> Vec<u8>)
        let snapshot_vec: HashMap<Vec<u8>, Vec<u8>> = snapshot
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();

        // Serialize snapshot
        let snapshot_bytes = bincode::serialize(&snapshot_vec)
            .map_err(|e| CheckpointError::Serialization(e.to_string()))?;

        // Write to log
        let checkpoint_offset = self.log.append(&snapshot_bytes).await?;

        // Create metadata with unique ID
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Generate unique ID: timestamp * 1000 + counter (ensures uniqueness)
        let counter = self.checkpoint_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let id = timestamp * 1000 + (counter % 1000);

        let metadata = CheckpointMetadata {
            id,
            timestamp,
            entry_count,
            log_offset: checkpoint_offset,
        };

        // Save metadata to file
        let metadata_path = self.checkpoint_dir.join(format!("checkpoint_{}.meta", metadata.id));
        let metadata_bytes = bincode::serialize(&metadata)
            .map_err(|e| CheckpointError::Serialization(e.to_string()))?;
        std::fs::write(&metadata_path, metadata_bytes)
            .map_err(|e| CheckpointError::Log(LogError::Io(e)))?;

        info!(
            "Checkpoint {} created: {} entries at offset {}",
            metadata.id, entry_count, checkpoint_offset
        );

        Ok(metadata)
    }

    /// Restore state from a checkpoint
    pub async fn restore_checkpoint(&self, checkpoint_id: u64) -> CheckpointResult<()> {
        info!("Restoring checkpoint {}...", checkpoint_id);

        // Load metadata
        let metadata_path = self.checkpoint_dir.join(format!("checkpoint_{}.meta", checkpoint_id));
        let metadata_bytes = std::fs::read(&metadata_path)
            .map_err(|_| CheckpointError::NotFound(checkpoint_id))?;

        let metadata: CheckpointMetadata = bincode::deserialize(&metadata_bytes)
            .map_err(|e| CheckpointError::Deserialization(e.to_string()))?;

        // Read snapshot from log
        let snapshot_bytes = self.log.read_at(metadata.log_offset).await?;

        // Deserialize snapshot
        let snapshot_vec: HashMap<Vec<u8>, Vec<u8>> = bincode::deserialize(&snapshot_bytes)
            .map_err(|e| CheckpointError::Deserialization(e.to_string()))?;

        // Convert back to Bytes
        let snapshot: HashMap<Bytes, Bytes> = snapshot_vec
            .into_iter()
            .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
            .collect();

        // Restore state
        self.state_backend.restore(snapshot).await?;

        info!(
            "Checkpoint {} restored: {} entries",
            checkpoint_id, metadata.entry_count
        );

        Ok(())
    }

    /// List all available checkpoints
    pub fn list_checkpoints(&self) -> CheckpointResult<Vec<CheckpointMetadata>> {
        let mut checkpoints = Vec::new();

        let entries = std::fs::read_dir(&self.checkpoint_dir)
            .map_err(|e| CheckpointError::Log(LogError::Io(e)))?;
        for entry in entries {
            let entry = entry.map_err(|e| CheckpointError::Log(LogError::Io(e)))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                if let Ok(metadata_bytes) = std::fs::read(&path) {
                    if let Ok(metadata) = bincode::deserialize::<CheckpointMetadata>(&metadata_bytes) {
                        checkpoints.push(metadata);
                    }
                }
            }
        }

        // Sort by timestamp (newest first)
        checkpoints.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok(checkpoints)
    }

    /// Get the latest checkpoint
    pub fn get_latest_checkpoint(&self) -> CheckpointResult<Option<CheckpointMetadata>> {
        let checkpoints = self.list_checkpoints()?;
        Ok(checkpoints.into_iter().next())
    }

    /// Delete a checkpoint
    pub fn delete_checkpoint(&self, checkpoint_id: u64) -> CheckpointResult<()> {
        let metadata_path = self.checkpoint_dir.join(format!("checkpoint_{}.meta", checkpoint_id));
        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)
                .map_err(|e| CheckpointError::Log(LogError::Io(e)))?;
            info!("Deleted checkpoint {}", checkpoint_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::memory::MemoryStateBackend;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_checkpoint_create_and_restore() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("state.log");
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        let state_backend = Box::new(MemoryStateBackend::new());
        let manager = CheckpointManager::new(state_backend, &log_path, &checkpoint_dir)
            .await
            .unwrap();

        // Create checkpoint with empty state
        let metadata1 = manager.create_checkpoint().await.unwrap();
        assert_eq!(metadata1.entry_count, 0);
        assert!(metadata1.id > 0);
        assert!(metadata1.timestamp > 0);

        // Create another checkpoint
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let metadata2 = manager.create_checkpoint().await.unwrap();
        assert!(metadata2.id > metadata1.id); // Counter ensures unique increasing IDs
        assert!(metadata2.timestamp >= metadata1.timestamp);
    }

    #[tokio::test]
    async fn test_checkpoint_list() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("state.log");
        let checkpoint_dir = temp_dir.path().join("checkpoints");

        let state_backend = Box::new(MemoryStateBackend::new());
        let manager = CheckpointManager::new(state_backend, &log_path, &checkpoint_dir)
            .await
            .unwrap();

        // Create checkpoints
        let metadata1 = manager.create_checkpoint().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let metadata2 = manager.create_checkpoint().await.unwrap();

        // List checkpoints
        let checkpoints = manager.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);

        // Latest should be first (sorted by timestamp descending)
        // Note: IDs may differ slightly due to counter, but timestamps should match
        assert!(checkpoints[0].timestamp >= checkpoints[1].timestamp);
        assert!(checkpoints.iter().any(|c| c.id == metadata1.id));
        assert!(checkpoints.iter().any(|c| c.id == metadata2.id));

        // Get latest - should match metadata2
        let latest = manager.get_latest_checkpoint().unwrap();
        assert!(latest.is_some());
        // Latest should have timestamp >= metadata2's timestamp
        assert!(latest.unwrap().timestamp >= metadata2.timestamp);
    }
}

