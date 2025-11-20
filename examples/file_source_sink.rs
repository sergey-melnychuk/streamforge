//! File source and sink example
//!
//! Demonstrates reading from a file, processing, and writing to another file
//!
//! Run with: cargo run --example file_source_sink

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;
use streamforge::sinks::file::FileSink;
use streamforge::sources::file::FileSource;
use tempfile::NamedTempFile;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("================================================================");
    println!("     StreamForge File Source & Sink Example");
    println!("================================================================");
    println!();

    // Create input file with JSON events
    let mut input_file = NamedTempFile::new()?;
    writeln!(input_file, "{{\"key\": \"user1\", \"value\": 10}}")?;
    writeln!(input_file, "{{\"key\": \"user2\", \"value\": 20}}")?;
    writeln!(input_file, "{{\"key\": \"user1\", \"value\": 30}}")?;
    writeln!(input_file, "{{\"key\": \"user3\", \"value\": 40}}")?;
    let input_path = input_file.path().to_string_lossy().to_string();
    println!("Created input file: {}", input_path);

    // Create output file
    let output_file = NamedTempFile::new()?;
    let output_path = output_file.path().to_string_lossy().to_string();
    println!("Created output file: {}", output_path);
    println!();

    // Create file source
    let source = FileSource::from_path(&input_path)?;
    println!("Created file source");

    // Create stream from source
    let stream = Stream::from_source(source);
    println!("Created stream from source");

    // Process: filter events with value > 15, then double the value
    let processed = stream
        .filter(|e| {
            if let EventValue::Json(json) = &e.value {
                json.get("value")
                    .and_then(|v| v.as_i64())
                    .map(|v| v > 15)
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|e| {
            if let EventValue::Json(mut json) = e.value {
                if let Some(value) = json.get_mut("value") {
                    if let Some(v) = value.as_i64() {
                        *value = serde_json::Value::Number((v * 2).into());
                    }
                }
                Event::new(e.key, EventValue::Json(json), e.timestamp)
            } else {
                e
            }
        });
    println!("Applied filter and map operators");

    // Create file sink
    let sink = FileSink::from_path(&output_path)?;
    println!("Created file sink");

    // Write to sink
    processed.sink(sink).await?;
    println!("Wrote processed events to sink");
    println!();

    // Read and display output
    let output_content = std::fs::read_to_string(&output_path)?;
    println!("Output file contents:");
    println!("{}", output_content);

    println!();
    println!("================================================================");
    println!("     File source & sink example complete!");
    println!("================================================================");

    Ok(())
}

