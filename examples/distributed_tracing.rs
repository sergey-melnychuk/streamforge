//! Distributed Tracing Example
//!
//! Demonstrates distributed tracing across multiple nodes:
//! - Trace context propagation through RPC calls
//! - Span creation for operator execution
//! - End-to-end request tracing

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::tracing::{init_tracing, TraceContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    init_tracing("streamforge-tracing-demo")?;

    println!("🔍 StreamForge Distributed Tracing Demo");
    println!("========================================\n");

    // Create a root trace context
    let root_trace = TraceContext::new();
    println!("Root Trace ID: {}", root_trace.trace_id);
    println!("Root Span ID: {}\n", root_trace.span_id);

    // Simulate processing events with tracing
    let events = vec![
        Event::now(EventKey::String("key1".into()), EventValue::Int(1)),
        Event::now(EventKey::String("key2".into()), EventValue::Int(2)),
        Event::now(EventKey::String("key3".into()), EventValue::Int(3)),
    ];

    // Create child spans for each operation
    let child1 = root_trace.child();
    println!("Child Span 1:");
    println!("  Trace ID: {} (same as root)", child1.trace_id);
    println!("  Span ID: {} (new)", child1.span_id);
    println!(
        "  Parent Span ID: {} (root span)\n",
        child1.parent_span_id.unwrap()
    );

    let child2 = child1.child();
    println!("Child Span 2 (child of Span 1):");
    println!("  Trace ID: {} (same as root)", child2.trace_id);
    println!("  Span ID: {} (new)", child2.span_id);
    println!(
        "  Parent Span ID: {} (child1 span)\n",
        child2.parent_span_id.unwrap()
    );

    // Demonstrate trace context with attributes
    let mut traced_ctx = TraceContext::new();
    traced_ctx.set_attribute("service".to_string(), "streamforge".to_string());
    traced_ctx.set_attribute("operation".to_string(), "filter_events".to_string());
    traced_ctx.set_attribute("event_count".to_string(), events.len().to_string());

    println!("Trace Context with Attributes:");
    println!("  Trace ID: {}", traced_ctx.trace_id);
    for (key, value) in &traced_ctx.attributes {
        println!("  {}: {}", key, value);
    }

    println!("\n✅ Distributed tracing demo complete!");
    println!("\nIn a real distributed scenario:");
    println!("1. Root trace context is created at the entry point");
    println!("2. Each RPC call propagates the trace context");
    println!("3. Each node creates child spans for its operations");
    println!("4. All spans share the same trace ID");
    println!("5. Spans form a tree structure via parent-child relationships");
    println!("6. Traces can be exported to Jaeger, Zipkin, or OTLP backends");

    Ok(())
}
