//! Demo: Distributed execution across cluster nodes

use streamforge::core::{Event, EventKey, EventValue};
use streamforge::distributed::{
    node::{NodeId, NodeMetadata},
    ClusterMembership,
};
use streamforge::execution::distributed::{DistributedContext, DistributedExecutor};
use streamforge::operators::FilterOp;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up 3-node cluster...");
    
    // Create cluster with 3 nodes
    let node1_id = NodeId::new(1);
    let node2_id = NodeId::new(2);
    let node3_id = NodeId::new(3);

    let membership = Arc::new(ClusterMembership::new(node1_id, 30));
    
    // Add all nodes to cluster
    let node1 = NodeMetadata::new(node1_id, "127.0.0.1:9001".parse()?);
    let node2 = NodeMetadata::new(node2_id, "127.0.0.1:9002".parse()?);
    let node3 = NodeMetadata::new(node3_id, "127.0.0.1:9003".parse()?);
    
    membership.add_node(node1.clone());
    membership.add_node(node2.clone());
    membership.add_node(node3.clone());
    
    println!("  Node 1: {} @ {}", node1_id, node1.address);
    println!("  Node 2: {} @ {}", node2_id, node2.address);
    println!("  Node 3: {} @ {}\n", node3_id, node3.address);

    // Create distributed context for node 1
    println!("Creating distributed context for Node 1...");
    let context = Arc::new(DistributedContext::new(node1_id, membership.clone(), 12));
    
    // Update partition assignment
    println!("Updating partition assignment...");
    context.update_partition_assignment().await?;
    
    // Show partition assignment
    let local_partitions = context.get_local_partitions().await;
    println!("  Node 1 assigned partitions: {:?}", local_partitions);
    println!("  Total partitions: 12\n");

    // Create distributed executor
    println!("Creating distributed executor...");
    let executor = DistributedExecutor::new(context.clone());

    // Create sample events with different keys
    println!("Creating sample events with different keys...");
    let events = vec![
        Event::new(
            EventKey::from_str("user-alice"),
            EventValue::Json(serde_json::json!({"user": "alice", "value": 100})),
            1234567890,
        ),
        Event::new(
            EventKey::from_str("user-bob"),
            EventValue::Json(serde_json::json!({"user": "bob", "value": 200})),
            1234567891,
        ),
        Event::new(
            EventKey::from_str("user-charlie"),
            EventValue::Json(serde_json::json!({"user": "charlie", "value": 300})),
            1234567892,
        ),
    ];
    println!("  Created {} events\n", events.len());

    // Execute filter operator
    println!("Executing filter operator (value > 150)...");
    let filter_op = FilterOp::new(|e: &Event| {
        if let EventValue::Json(json) = &e.value {
            json.get("value")
                .and_then(|v| v.as_i64())
                .map(|v| v > 150)
                .unwrap_or(false)
        } else {
            false
        }
    });

    let results = executor.execute_operator(filter_op, events).await?;
    println!("  Processed {} events", results.len());
    println!("  Results:");
    for event in &results {
        if let EventValue::Json(json) = &event.value {
            println!("    - {}", json);
        }
    }

    println!("\n✅ Distributed execution demo complete!");
    Ok(())
}

