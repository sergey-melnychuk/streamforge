//! Sink write tracking for idempotent writes
//!
//! Tracks the last written position to sinks to enable idempotent writes and transaction tracking

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::sync::RwLock;
use std::sync::Arc;

/// Sink write information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkWrite {
    /// Sink identifier (e.g., file path, database table)
    pub sink_id: String,
    /// Last written offset/position
    pub last_written: u64,
    /// Last transaction ID (for exactly-once semantics)
    pub last_transaction_id: Option<String>,
    /// Timestamp of last write
    pub last_updated: u64,
    /// Number of events written
    pub events_written: u64,
}

/// Sink tracker for managing write positions
pub struct SinkTracker {
    /// Job ID this tracker belongs to
    job_id: String,
    /// Storage directory for write tracking files
    storage_dir: PathBuf,
    /// In-memory write cache
    writes: Arc<RwLock<HashMap<String, SinkWrite>>>,
}

impl SinkTracker {
    /// Create a new sink tracker for a job
    pub async fn new(job_id: &str, storage_dir: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let storage_dir = storage_dir.join("sink_writes");
        fs::create_dir_all(&storage_dir).await?;

        let write_file = storage_dir.join(format!("{}.json", job_id));
        let writes = if write_file.exists() {
            let content = fs::read_to_string(&write_file).await?;
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self {
            job_id: job_id.to_string(),
            storage_dir,
            writes: Arc::new(RwLock::new(writes)),
        })
    }

    /// Get the last write position for a sink
    #[allow(dead_code)]
    pub async fn get_last_write(&self, sink_id: &str) -> Option<SinkWrite> {
        let writes = self.writes.read().await;
        writes.get(sink_id).cloned()
    }

    /// Update the write position for a sink
    pub async fn update_write(
        &self,
        sink_id: &str,
        position: u64,
        transaction_id: Option<String>,
        events_count: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        {
            let mut writes = self.writes.write().await;
            let write = writes.entry(sink_id.to_string()).or_insert_with(|| SinkWrite {
                sink_id: sink_id.to_string(),
                last_written: 0,
                last_transaction_id: None,
                last_updated: now,
                events_written: 0,
            });

            write.last_written = position;
            write.last_transaction_id = transaction_id;
            write.last_updated = now;
            write.events_written += events_count;
        }

        // Persist to disk
        self.persist().await?;

        Ok(())
    }

    /// Persist writes to disk
    async fn persist(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let write_file = self.storage_dir.join(format!("{}.json", self.job_id));
        let writes = self.writes.read().await;
        let content = serde_json::to_string_pretty(&*writes)?;
        fs::write(&write_file, content).await?;
        Ok(())
    }

    /// Get all write positions
    #[allow(dead_code)]
    pub async fn get_all_writes(&self) -> HashMap<String, SinkWrite> {
        self.writes.read().await.clone()
    }
}

