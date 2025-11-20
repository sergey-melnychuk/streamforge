//! Demo: Stateful operators with state backends

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::operators::stateful::{OperatorState, StatefulOperator};
use streamforge::operators::{CountOperator, StreamOperator};
use streamforge::state::MemoryStateBackend;
use std::sync::Arc;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating state backend (in-memory)...");
    let backend = Arc::new(MemoryStateBackend::new());
    
    // Create operator state with namespace
    println!("Creating operator state with namespace 'counter'...");
    let state = OperatorState::new(backend.clone(), "counter");

    // Store some state
    println!("\nStoring state values...");
    state.set(b"key1", b"value1".to_vec()).await?;
    state.set(b"key2", b"value2".to_vec()).await?;
    state.set(b"key3", b"value3".to_vec()).await?;
    println!("  Stored 3 key-value pairs");

    // Retrieve state
    println!("\nRetrieving state values...");
    let value1 = state.get(b"key1").await?;
    let value2 = state.get(b"key2").await?;
    println!("  key1 = {:?}", value1);
    println!("  key2 = {:?}", value2);

    // List all keys
    println!("\nListing all keys in namespace...");
    let keys = state.list_keys().await?;
    println!("  Keys: {:?}", keys);

    // Delete a key
    println!("\nDeleting key1...");
    state.delete(b"key1").await?;
    let value1_after = state.get(b"key1").await?;
    println!("  key1 after delete: {:?}", value1_after);

    // Create stateful operator
    println!("\nCreating stateful CountOperator...");
    let mut count_op = CountOperator::with_state(backend);
    
    // Process some events
    println!("Processing events with stateful operator...");
    let event1 = Event::new(
        EventKey::from_str("user1"),
        EventValue::String("event1".into()),
        1234567890,
    );
    let event2 = Event::new(
        EventKey::from_str("user2"),
        EventValue::String("event2".into()),
        1234567891,
    );
    
    let _result1 = count_op.process(event1)?;
    let _result2 = count_op.process(event2)?;
    println!("  Processed 2 events (results: {:?}, {:?})", _result1.is_some(), _result2.is_some());

    // Check state backend
    println!("\nChecking state backend...");
    if let Some(backend_ref) = count_op.state_backend() {
        let keys = backend_ref.list_keys(b"count:").await?;
        println!("  State backend has {} keys in 'count:' namespace", keys.len());
    }

    println!("\n✅ Stateful operators demo complete!");
    Ok(())
}

