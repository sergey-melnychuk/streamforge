//! File source for reading events from files

use crate::core::{Event, EventKey, EventValue};
use crate::sources::Source;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;
use tokio::task;
use tokio::time::sleep;
use tracing::error;

/// File source configuration
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// File path
    pub path: String,
    /// Format: "json", "csv", "text"
    pub format: String,
    /// Whether to read continuously (tail -f style)
    pub follow: bool,
}

impl Default for FileSourceConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            format: "json".to_string(),
            follow: false,
        }
    }
}

/// File source for reading events from files
pub struct FileSource {
    config: FileSourceConfig,
    reader: Option<BufReader<File>>,
    exhausted: bool,
    line_number: u64,
}

impl FileSource {
    /// Create a new file source
    pub fn new(config: FileSourceConfig) -> Result<Self, std::io::Error> {
        let path = Path::new(&config.path);
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            config,
            reader: Some(reader),
            exhausted: false,
            line_number: 0,
        })
    }

    /// Create from a file path (defaults to JSON format)
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        Self::new(FileSourceConfig {
            path: path.as_ref().to_string_lossy().to_string(),
            format: "json".to_string(),
            follow: false,
        })
    }

    /// Read and parse a line
    fn parse_line(&mut self, line: String) -> Option<Event> {
        self.line_number += 1;
        let line = line.trim();

        if line.is_empty() {
            return None;
        }

        match self.config.format.as_str() {
            "json" => self.parse_json_line(line),
            "csv" => self.parse_csv_line(line),
            "text" => self.parse_text_line(line),
            _ => {
                error!("Unknown format: {}", self.config.format);
                None
            }
        }
    }

    fn parse_json_line(&self, line: &str) -> Option<Event> {
        match serde_json::from_str::<JsonValue>(line) {
            Ok(json) => {
                // Extract key if present
                let key = json
                    .get("key")
                    .and_then(|k| k.as_str())
                    .map(EventKey::from_str)
                    .unwrap_or_else(EventKey::default);

                // Use the JSON as the value
                let value = EventValue::Json(json);

                Some(Event::new(
                    key,
                    value,
                    chrono::Utc::now().timestamp_millis(),
                ))
            }
            Err(e) => {
                error!("Failed to parse JSON line: {} - {}", line, e);
                None
            }
        }
    }

    fn parse_csv_line(&self, line: &str) -> Option<Event> {
        // Simple CSV parsing (assumes header row was already read)
        let fields: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

        if fields.is_empty() {
            return None;
        }

        // Create a JSON object from CSV fields
        let mut json = serde_json::Map::new();
        for (i, field) in fields.iter().enumerate() {
            json.insert(format!("field_{}", i), JsonValue::String(field.to_string()));
        }

        let value = EventValue::Json(JsonValue::Object(json));
        Some(Event::new(
            EventKey::default(),
            value,
            chrono::Utc::now().timestamp_millis(),
        ))
    }

    fn parse_text_line(&self, line: &str) -> Option<Event> {
        let value = EventValue::String(line.into());
        Some(Event::new(
            EventKey::default(),
            value,
            chrono::Utc::now().timestamp_millis(),
        ))
    }
}

#[async_trait]
impl Source for FileSource {
    async fn read(&mut self) -> Option<Event> {
        if self.exhausted {
            return None;
        }

        // Read from file in a blocking task
        let reader = self.reader.take()?;
        let config = self.config.clone();
        let line_number = self.line_number;
        let follow = config.follow;

        let result = task::spawn_blocking(move || {
            let mut reader = reader;
            let mut line = String::new();

            match reader.read_line(&mut line) {
                Ok(0) => {
                    // EOF - check if we should follow (tail -f style)
                    if follow {
                        // In follow mode, EOF doesn't mean exhausted
                        // Return None but keep reader alive
                        (None, Some(reader), false)
                    } else {
                        // Not following, EOF means exhausted
                        (None, Some(reader), true)
                    }
                }
                Ok(_) => {
                    // Got a line
                    let mut source = FileSource {
                        config,
                        reader: None,
                        exhausted: false,
                        line_number,
                    };
                    let event = source.parse_line(line);
                    (event, Some(reader), false)
                }
                Err(e) => {
                    error!("Error reading file: {}", e);
                    (None, Some(reader), true)
                }
            }
        })
        .await;

        match result {
            Ok((event, reader, exhausted)) => {
                self.reader = reader;
                self.exhausted = exhausted;

                // If in follow mode and got EOF, wait a bit before next read
                if follow && event.is_none() && !exhausted {
                    sleep(Duration::from_millis(100)).await;
                }

                event
            }
            Err(e) => {
                error!("Task error: {}", e);
                self.exhausted = true;
                None
            }
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_file_source_json() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();
        drop(file); // Close the file so we can write to it

        // Write JSON lines to the file (with actual newlines)
        let json = "{\"key\": \"test\", \"value\": 42}\n{\"key\": \"test2\", \"value\": 43}\n";
        std::fs::write(&path, json).unwrap();

        let mut source = FileSource::new(FileSourceConfig {
            path,
            format: "json".to_string(),
            follow: false,
        })
        .unwrap();

        let event1 = source.read().await;
        assert!(event1.is_some());

        let event2 = source.read().await;
        assert!(event2.is_some());

        let event3 = source.read().await;
        assert!(event3.is_none());
        assert!(source.is_exhausted());
    }

    #[tokio::test]
    async fn test_file_source_text() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line 1").unwrap();
        writeln!(file, "line 2").unwrap();
        let path = file.path().to_string_lossy().to_string();

        let mut source = FileSource::new(FileSourceConfig {
            path,
            format: "text".to_string(),
            follow: false,
        })
        .unwrap();

        let event1 = source.read().await;
        assert!(event1.is_some());

        let event2 = source.read().await;
        assert!(event2.is_some());
    }
}
