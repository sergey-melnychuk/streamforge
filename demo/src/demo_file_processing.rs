//! Demo: File source and sink processing

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::execution::Stream;
use streamforge::sinks::file::FileSink;
use streamforge::sources::file::FileSource;
use tempfile::NamedTempFile;
use std::io::Write;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating input file with sample data...");
    
    // Create input file with JSON events
    let mut input_file = NamedTempFile::new()?;
    writeln!(input_file, r#"{{"user": "alice", "amount": 100, "category": "food"}}"#)?;
    writeln!(input_file, r#"{{"user": "bob", "amount": 200, "category": "electronics"}}"#)?;
    writeln!(input_file, r#"{{"user": "alice", "amount": 50, "category": "food"}}"#)?;
    writeln!(input_file, r#"{{"user": "charlie", "amount": 300, "category": "electronics"}}"#)?;
    writeln!(input_file, r#"{{"user": "bob", "amount": 75, "category": "food"}}"#)?;
    let input_path = input_file.path().to_string_lossy().to_string();
    println!("  Input file: {}", input_path);

    // Create output file
    let output_file = NamedTempFile::new()?;
    let output_path = output_file.path().to_string_lossy().to_string();
    println!("  Output file: {}\n", output_path);

    // Create file source
    println!("Creating file source...");
    let source = FileSource::from_path(&input_path)?;

    // Create stream from source
    println!("Creating stream from source...");
    let stream = Stream::from_source(source);

    // Process: Filter events with amount > 100, then double the amount
    println!("Processing: Filter amount > 100, then double the amount...");
    let processed = stream
        .filter(|e| {
            if let EventValue::Json(json) = &e.value {
                json.get("amount")
                    .and_then(|v| v.as_i64())
                    .map(|v| v > 100)
                    .unwrap_or(false)
            } else {
                false
            }
        })
        .map(|e| {
            if let EventValue::Json(mut json) = e.value {
                if let Some(amount) = json.get_mut("amount") {
                    if let Some(v) = amount.as_i64() {
                        *amount = serde_json::Value::Number((v * 2).into());
                    }
                }
                Event::new(e.key, EventValue::Json(json), e.timestamp)
            } else {
                e
            }
        });

    // Create file sink
    println!("Creating file sink...");
    let sink = FileSink::from_path(&output_path)?;

    // Write to sink
    println!("Writing processed events to sink...");
    processed.sink(sink).await?;
    println!("  Done!\n");

    // Read and display output
    let output_content = std::fs::read_to_string(&output_path)?;
    println!("Output file contents:");
    println!("{}", output_content);

    println!("\n✅ File processing demo complete!");
    Ok(())
}

