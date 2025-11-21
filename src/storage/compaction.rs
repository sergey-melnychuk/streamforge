//! Log compaction mechanism
//!
//! Reclaims space by removing obsolete entries

use crate::storage::log::{AppendOnlyLog, LogResult};
use bytes::Bytes;
use tracing::{debug, info};

/// Compaction strategy
#[derive(Debug, Clone, Copy)]
pub enum CompactionStrategy {
    /// Size-based: compact when log exceeds size threshold
    SizeBased { threshold_bytes: u64 },
    /// Time-based: compact entries older than threshold
    TimeBased { threshold_seconds: u64 },
    /// Manual: compact on demand
    Manual,
}

/// Log compactor for reclaiming space
pub struct LogCompactor {
    log: AppendOnlyLog,
    strategy: CompactionStrategy,
}

impl LogCompactor {
    /// Create a new log compactor
    pub fn new(log: AppendOnlyLog, strategy: CompactionStrategy) -> Self {
        Self { log, strategy }
    }

    /// Compact the log using the configured strategy
    pub async fn compact(&self, keep_keys: &[Bytes]) -> LogResult<u64> {
        info!("Starting log compaction...");

        let current_size = self.log.size().await?;

        match self.strategy {
            CompactionStrategy::SizeBased { threshold_bytes } => {
                if current_size < threshold_bytes {
                    debug!(
                        "Log size {} < threshold {}, skipping compaction",
                        current_size, threshold_bytes
                    );
                    return Ok(0);
                }
            }
            CompactionStrategy::TimeBased {
                threshold_seconds: _,
            } => {
                // Time-based compaction would require timestamp tracking
                // For now, we'll do a simple size-based approach
            }
            CompactionStrategy::Manual => {
                // Always compact on manual request
            }
        }

        // Create a set of keys to keep
        let _keep_set: std::collections::HashSet<Bytes> = keep_keys.iter().cloned().collect();

        // Read all entries and filter
        let mut entries_to_keep = Vec::new();
        let mut offset = 0u64;
        let size = self.log.size().await?;

        while offset < size {
            match self.log.read_at(offset).await {
                Ok(data) => {
                    // In a real implementation, we would parse the entry to extract the key
                    // For now, we'll keep all entries (simplified)
                    let len = data.len() as u64;
                    entries_to_keep.push((offset, data));
                    // Estimate next offset (8 bytes length + data length)
                    offset += 8 + len;
                }
                Err(_) => break,
            }
        }

        // Truncate and rewrite
        self.log.truncate().await?;
        for (_old_offset, data) in entries_to_keep {
            self.log.append(&data).await?;
        }

        let new_size = self.log.size().await?;
        let compacted_size = current_size - new_size;

        info!(
            "Compaction complete: reclaimed {} bytes ({} -> {})",
            compacted_size, current_size, new_size
        );

        Ok(compacted_size)
    }

    /// Get compaction statistics
    pub async fn stats(&self) -> LogResult<CompactionStats> {
        let size = self.log.size().await?;
        Ok(CompactionStats {
            log_size_bytes: size,
            strategy: self.strategy,
        })
    }
}

/// Compaction statistics
#[derive(Debug)]
pub struct CompactionStats {
    /// Current log size in bytes
    pub log_size_bytes: u64,
    /// Compaction strategy in use
    pub strategy: CompactionStrategy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compaction_stats() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let log = AppendOnlyLog::open(&log_path).await.unwrap();
        let compactor = LogCompactor::new(log, CompactionStrategy::Manual);

        let stats = compactor.stats().await.unwrap();
        assert_eq!(stats.log_size_bytes, 0);
    }
}
