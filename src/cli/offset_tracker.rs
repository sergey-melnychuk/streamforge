//! Source offset tracking for resumable processing
//!
//! Tracks the last read position from sources to enable resumable processing

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::sync::RwLock;

/// Source offset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceOffset {
    /// Source identifier (e.g., file path, Kafka topic-partition)
    pub source_id: String,
    /// Offset value (line number, byte offset, Kafka offset, etc.)
    pub offset: u64,
    /// Timestamp of last update
    pub last_updated: u64,
}

/// Offset tracker for managing source read positions
pub struct OffsetTracker {
    /// Job ID this tracker belongs to
    job_id: String,
    /// Storage directory for offset files
    storage_dir: PathBuf,
    /// In-memory offset cache
    offsets: Arc<RwLock<HashMap<String, SourceOffset>>>,
}

impl OffsetTracker {
    /// Create a new offset tracker for a job
    pub async fn new(job_id: &str, storage_dir: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let storage_dir = storage_dir.join("offsets");
        fs::create_dir_all(&storage_dir).await?;

        let offset_file = storage_dir.join(format!("{}.json", job_id));
        let offsets = if offset_file.exists() {
            let content = fs::read_to_string(&offset_file).await?;
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            HashMap::new()
        };

        Ok(Self {
            job_id: job_id.to_string(),
            storage_dir,
            offsets: Arc::new(RwLock::new(offsets)),
        })
    }

    /// Get the last offset for a source
    pub async fn get_offset(&self, source_id: &str) -> Option<u64> {
        let offsets = self.offsets.read().await;
        offsets.get(source_id).map(|o| o.offset)
    }

    /// Update the offset for a source
    pub async fn update_offset(&self, source_id: &str, offset: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        {
            let mut offsets = self.offsets.write().await;
            offsets.insert(
                source_id.to_string(),
                SourceOffset {
                    source_id: source_id.to_string(),
                    offset,
                    last_updated: now,
                },
            );
        }

        // Persist to disk
        self.persist().await?;

        Ok(())
    }

    /// Persist offsets to disk
    async fn persist(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let offset_file = self.storage_dir.join(format!("{}.json", self.job_id));
        let offsets = self.offsets.read().await;
        let content = serde_json::to_string_pretty(&*offsets)?;
        fs::write(&offset_file, content).await?;
        Ok(())
    }

    /// Get all offsets
    #[allow(dead_code)]
    pub async fn get_all_offsets(&self) -> HashMap<String, SourceOffset> {
        self.offsets.read().await.clone()
    }
}

use std::sync::Arc;

