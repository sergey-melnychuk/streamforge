//! Word count example - a classic stream processing example
//!
//! Run with: cargo run --example wordcount

use std::collections::HashMap;
use streamforge::core::{Event, EventValue};
use streamforge::execution::Stream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== StreamForge Word Count Example ===\n");

    // Sample text data
    let texts = vec![
        "hello world",
        "hello streamforge",
        "world of streaming",
        "hello hello world",
    ];

    // Create a stream from the text lines
    let stream = Stream::from_values(texts.iter().map(|s| s.to_string()).collect());

    // Process: split into words, filter, and collect
    let events = stream
        .flat_map(|e| {
            // Split text into words
            let text = e.value.as_str().unwrap_or("");
            text.split_whitespace()
                .map(|word| Event::with_value(EventValue::from_str(word.to_lowercase())))
                .collect()
        })
        .filter(|e| {
            // Filter out short words
            e.value.as_str().map(|s| s.len() >= 3).unwrap_or(false)
        })
        .collect()
        .await?;

    // Count word occurrences
    let mut word_counts: HashMap<String, usize> = HashMap::new();
    for event in events {
        if let Some(word) = event.value.as_str() {
            *word_counts.entry(word.to_string()).or_insert(0) += 1;
        }
    }

    // Display results
    println!("Word counts:");
    let mut counts: Vec<_> = word_counts.iter().collect();
    counts.sort_by(|a, b| b.1.cmp(a.1));

    for (word, count) in counts {
        println!("  {:12} : {}", word, count);
    }

    Ok(())
}
