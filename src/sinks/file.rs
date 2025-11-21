//! File sink for writing events to files

use crate::core::Event;
use crate::sinks::{Sink, SinkError};
use async_trait::async_trait;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use tokio::sync::Mutex;
use tracing::debug;

/// File sink configuration
#[derive(Debug, Clone)]
pub struct FileSinkConfig {
    /// File path
    pub path: String,
    /// Format: "json", "csv", "text"
    pub format: String,
    /// Append to file (true) or overwrite (false)
    pub append: bool,
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            format: "json".to_string(),
            append: true,
        }
    }
}

/// File sink for writing events to files
pub struct FileSink {
    config: FileSinkConfig,
    file: Mutex<std::fs::File>,
}

impl FileSink {
    /// Create a new file sink
    pub fn new(config: FileSinkConfig) -> Result<Self, SinkError> {
        let path = Path::new(&config.path);

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(config.append)
            .write(true)
            .open(path)?;

        Ok(Self {
            config,
            file: Mutex::new(file),
        })
    }

    /// Create from a file path (defaults to JSON format, append mode)
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, SinkError> {
        Self::new(FileSinkConfig {
            path: path.as_ref().to_string_lossy().to_string(),
            format: "json".to_string(),
            append: true,
        })
    }

    /// Serialize event to string based on format
    fn serialize_event(&self, event: &Event) -> Result<String, SinkError> {
        match self.config.format.as_str() {
            "json" => self.serialize_json(event),
            "csv" => self.serialize_csv(event),
            "text" => self.serialize_text(event),
            _ => Err(SinkError::Other(format!(
                "Unknown format: {}",
                self.config.format
            ))),
        }
    }

    fn serialize_json(&self, event: &Event) -> Result<String, SinkError> {
        use serde_json::json;

        let mut obj = serde_json::Map::new();

        // Add key
        match &event.key {
            crate::core::EventKey::None => {}
            crate::core::EventKey::String(s) => {
                obj.insert("key".to_string(), json!(s.as_ref()));
            }
            crate::core::EventKey::Int(i) => {
                obj.insert("key".to_string(), json!(i));
            }
            crate::core::EventKey::Bytes(b) => {
                // For bytes, just store the length as a placeholder
                obj.insert("key".to_string(), json!(format!("bytes[{}]", b.len())));
            }
        }

        // Add timestamp
        obj.insert("timestamp".to_string(), json!(event.timestamp));

        // Add value
        match &event.value {
            crate::core::EventValue::Json(j) => {
                if let Some(map) = j.as_object() {
                    for (k, v) in map {
                        obj.insert(k.clone(), v.clone());
                    }
                } else {
                    obj.insert("value".to_string(), j.clone());
                }
            }
            crate::core::EventValue::String(s) => {
                obj.insert("value".to_string(), json!(s.as_ref()));
            }
            crate::core::EventValue::Int(i) => {
                obj.insert("value".to_string(), json!(i));
            }
            crate::core::EventValue::Float(f) => {
                obj.insert("value".to_string(), json!(f));
            }
            crate::core::EventValue::Bool(b) => {
                obj.insert("value".to_string(), json!(b));
            }
            _ => {}
        }

        serde_json::to_string(&json!(obj)).map_err(|e| SinkError::Serialization(e.to_string()))
    }

    fn serialize_csv(&self, _event: &Event) -> Result<String, SinkError> {
        // Simple CSV serialization
        // In a full implementation, this would handle headers and proper escaping
        Ok(String::new()) // Placeholder
    }

    fn serialize_text(&self, event: &Event) -> Result<String, SinkError> {
        match &event.value {
            crate::core::EventValue::String(s) => Ok(s.as_ref().to_string()),
            _ => Ok(format!("{:?}", event.value)),
        }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn write(&mut self, event: Event) -> Result<(), SinkError> {
        let line = self.serialize_event(&event)?;
        let mut file = self.file.lock().await;

        writeln!(file, "{}", line)?;
        file.flush()?;

        debug!("Wrote event to file: {}", self.config.path);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        let mut file = self.file.lock().await;
        file.flush()?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), SinkError> {
        self.flush().await?;
        // File will be closed when dropped
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Event, EventKey, EventValue};
    use std::fs;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_file_sink_json() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();

        let mut sink = FileSink::new(FileSinkConfig {
            path: path.clone(),
            format: "json".to_string(),
            append: false,
        })
        .unwrap();

        let event = Event::new(
            EventKey::from_str("test"),
            EventValue::Json(serde_json::json!({"value": 42})),
            1234567890,
        );

        sink.write(event).await.unwrap();
        sink.close().await.unwrap();

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("test"));
        assert!(content.contains("42"));
    }
}
