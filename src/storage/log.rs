//! Append-only log implementation
//!
//! Provides durable, append-only storage for events and state

use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// Error type for log operations
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Log is closed")]
    Closed,
    #[error("Invalid log entry at offset {0}")]
    InvalidEntry(u64),
    #[error("Log error: {0}")]
    Other(String),
}

/// Result type for log operations
pub type LogResult<T> = Result<T, LogError>;

/// Append-only log for durable storage
pub struct AppendOnlyLog {
    path: PathBuf,
    file: Arc<Mutex<File>>,
    current_offset: Arc<Mutex<u64>>,
}

impl AppendOnlyLog {
    /// Create or open an append-only log at the given path
    pub async fn open<P: AsRef<Path>>(path: P) -> LogResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Get current file size
        let current_offset = file.metadata()?.len();

        info!("Opened append-only log at {} (offset: {})", path.display(), current_offset);

        Ok(Self {
            path,
            file: Arc::new(Mutex::new(file)),
            current_offset: Arc::new(Mutex::new(current_offset)),
        })
    }

    /// Append data to the log
    pub async fn append(&self, data: &[u8]) -> LogResult<u64> {
        let mut file = self.file.lock().await;
        let mut offset = self.current_offset.lock().await;

        // Write length prefix (8 bytes, big-endian)
        let len = data.len() as u64;
        file.write_all(&len.to_be_bytes())?;

        // Write data
        file.write_all(data)?;
        file.sync_all()?; // Ensure durability

        let entry_offset = *offset;
        *offset += 8 + len; // 8 bytes for length prefix + data length

        debug!("Appended {} bytes at offset {}", len, entry_offset);

        Ok(entry_offset)
    }

    /// Read an entry at the given offset
    pub async fn read_at(&self, offset: u64) -> LogResult<Bytes> {
        let mut file = self.file.lock().await;

        // Seek to offset
        file.seek(SeekFrom::Start(offset))?;

        // Read length prefix
        let mut len_bytes = [0u8; 8];
        file.read_exact(&mut len_bytes)?;
        let len = u64::from_be_bytes(len_bytes);

        // Read data
        let mut data = vec![0u8; len as usize];
        file.read_exact(&mut data)?;

        Ok(Bytes::from(data))
    }

    /// Get the current log size in bytes
    pub async fn size(&self) -> LogResult<u64> {
        let offset = self.current_offset.lock().await;
        Ok(*offset)
    }

    /// Get the path of the log file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the log (removes all entries)
    pub async fn truncate(&self) -> LogResult<()> {
        let mut file = self.file.lock().await;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.sync_all()?;

        let mut offset = self.current_offset.lock().await;
        *offset = 0;

        info!("Truncated log at {}", self.path.display());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_log_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let log = AppendOnlyLog::open(&log_path).await.unwrap();

        // Append some data
        let data1 = b"hello, world";
        let offset1 = log.append(data1).await.unwrap();

        let data2 = b"test data";
        let offset2 = log.append(data2).await.unwrap();

        // Read back
        let read1 = log.read_at(offset1).await.unwrap();
        assert_eq!(read1, Bytes::copy_from_slice(data1));

        let read2 = log.read_at(offset2).await.unwrap();
        assert_eq!(read2, Bytes::copy_from_slice(data2));
    }

    #[tokio::test]
    async fn test_log_size() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let log = AppendOnlyLog::open(&log_path).await.unwrap();

        assert_eq!(log.size().await.unwrap(), 0);

        log.append(b"test").await.unwrap();
        let size = log.size().await.unwrap();
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_log_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("test.log");

        let log = AppendOnlyLog::open(&log_path).await.unwrap();

        log.append(b"data").await.unwrap();
        assert!(log.size().await.unwrap() > 0);

        log.truncate().await.unwrap();
        assert_eq!(log.size().await.unwrap(), 0);
    }
}

