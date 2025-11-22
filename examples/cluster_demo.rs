//! Comprehensive cluster demonstration
//!
//! Shows how StreamForge clusters work end-to-end:
//! - Node discovery via gossip
//! - RPC communication
//! - Partition assignment and rebalancing
//! - State management
//!
//! Run with: cargo run --example cluster_demo

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use streamforge::distributed::{
    ClusterMembership, ConsistentHashAssigner, Discovery, GossipConfig, GossipDiscovery, NodeId,
    NodeMetadata, PartitionAssigner,
};
use streamforge::network::{RpcServer, Transport};
use streamforge::state::{MemoryStateBackend, StateBackend};
use tokio::time::sleep;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     StreamForge Cluster Demonstration                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // ========================================================================
    // Step 1: Create a 3-node cluster
    // ========================================================================
    println!("📡 Step 1: Creating a 3-node cluster\n");

    let node1_id = NodeId::new(1);
    let node2_id = NodeId::new(2);
    let node3_id = NodeId::new(3);

    // Use port 0 to get random available ports to avoid conflicts
    let node1_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node2_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let node3_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Bind transports to get actual addresses
    let transport1_temp = Arc::new(Transport::bind(node1_addr).await?);
    let transport2_temp = Arc::new(Transport::bind(node2_addr).await?);
    let transport3_temp = Arc::new(Transport::bind(node3_addr).await?);
    
    let actual_node1_addr = transport1_temp.local_addr();
    let actual_node2_addr = transport2_temp.local_addr();
    let actual_node3_addr = transport3_temp.local_addr();
    
    // Drop temporary transports
    drop(transport1_temp);
    drop(transport2_temp);
    drop(transport3_temp);
    
    let node1 = NodeMetadata::new(node1_id, actual_node1_addr);
    let node2 = NodeMetadata::new(node2_id, actual_node2_addr);
    let node3 = NodeMetadata::new(node3_id, actual_node3_addr);

    println!("   Node 1: {} @ {}", node1.id, node1.address);
    println!("   Node 2: {} @ {}", node2.id, node2.address);
    println!("   Node 3: {} @ {}", node3.id, node3.address);
    println!();

    // ========================================================================
    // Step 2: Set up Node 1 as the coordinator
    // ========================================================================
    println!("🎯 Step 2: Setting up Node 1 as coordinator\n");

    let membership1 = Arc::new(ClusterMembership::new(node1_id, 30));
    membership1.add_node(node1.clone());
    membership1.add_node(node2.clone());
    membership1.add_node(node3.clone());

    let transport1 = Arc::new(Transport::bind(actual_node1_addr).await?);
    let transport1_for_discovery = Arc::clone(&transport1);
    let rpc_server1 = RpcServer::new(Arc::clone(&transport1)).with_membership(membership1.clone());

    // Start RPC server
    let server1_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server1.start().await {
            eprintln!("RPC server error: {}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;
    println!(
        "   ✓ Node 1 RPC server started on {}",
        transport1_for_discovery.local_addr()
    );
    println!();

    // ========================================================================
    // Step 3: Demonstrate partition assignment
    // ========================================================================
    println!("🔀 Step 3: Partition assignment with consistent hashing\n");

    let assigner = ConsistentHashAssigner::new(100, 1);
    let nodes = membership1.get_alive_nodes();
    let num_partitions = 12;

    let assignment = assigner.assign(num_partitions, &nodes);

    println!(
        "   Assigning {} partitions across {} nodes:\n",
        num_partitions,
        nodes.len()
    );
    for (node_id, partitions) in &assignment {
        println!(
            "   {}: partitions {:?} ({} total)",
            node_id,
            partitions,
            partitions.len()
        );
    }

    let total: usize = assignment.values().map(|v| v.len()).sum();
    println!("\n   ✓ Total partitions assigned: {}", total);
    println!();

    // ========================================================================
    // Step 4: Demonstrate rebalancing when a node joins
    // ========================================================================
    println!("⚖️  Step 4: Rebalancing when Node 4 joins\n");

    let node4_id = NodeId::new(4);
    let node4_addr: SocketAddr = "127.0.0.1:9004".parse()?;
    let node4 = NodeMetadata::new(node4_id, node4_addr);

    membership1.add_node(node4.clone());
    let new_nodes = membership1.get_alive_nodes();

    let new_assignment = assigner.rebalance(num_partitions, &assignment, &new_nodes);

    println!("   New assignment with 4 nodes:\n");
    for (node_id, partitions) in &new_assignment {
        println!(
            "   {}: partitions {:?} ({} total)",
            node_id,
            partitions,
            partitions.len()
        );
    }

    // Calculate movement
    let mut moved = 0;
    for partition in 0..num_partitions {
        let old_node = assigner.get_node_for_partition(partition, &assignment);
        let new_node = assigner.get_node_for_partition(partition, &new_assignment);
        if old_node != new_node {
            moved += 1;
        }
    }

    println!(
        "\n   Partitions moved: {} / {} ({:.1}%)",
        moved,
        num_partitions,
        (moved as f64 / num_partitions as f64) * 100.0
    );
    println!();

    // ========================================================================
    // Step 5: Demonstrate state management
    // ========================================================================
    println!("💾 Step 5: State management per partition\n");

    let state_backend = MemoryStateBackend::new();

    // Simulate state for each partition
    for partition in 0..num_partitions {
        if let Some(node) = assigner.get_node_for_partition(partition, &new_assignment) {
            let key = format!("partition:{}:state", partition);
            let value = format!("node:{}:data", node.as_u64());

            state_backend.put(key.as_bytes(), value.into()).await?;
        }
    }

    println!("   Stored state for {} partitions", num_partitions);

    // Query state for a specific partition
    let partition_key = b"partition:5:state";
    if let Some(value) = state_backend.get(partition_key).await? {
        if let Some(node) = assigner.get_node_for_partition(5, &new_assignment) {
            println!(
                "   Partition 5 state: {} (assigned to {})",
                String::from_utf8_lossy(&value),
                node
            );
        }
    }

    // List all partition states
    let partition_states = state_backend.list_keys(b"partition:").await?;
    println!("   Total partition states: {}", partition_states.len());
    println!();

    // ========================================================================
    // Step 6: Demonstrate gossip discovery setup
    // ========================================================================
    println!("🌐 Step 6: Gossip discovery configuration\n");

    let gossip_config = GossipConfig {
        gossip_interval: Duration::from_secs(1),
        gossip_fanout: 2,
        heartbeat_timeout: Duration::from_secs(10),
        dead_timeout: Duration::from_secs(30),
        seed_nodes: vec![actual_node2_addr, actual_node3_addr],
    };

    let mut discovery = GossipDiscovery::new(node1.clone(), gossip_config.clone())
        .with_transport(transport1_for_discovery);

    println!("   Gossip interval: {:?}", gossip_config.gossip_interval);
    println!("   Gossip fanout: {}", gossip_config.gossip_fanout);
    println!(
        "   Heartbeat timeout: {:?}",
        gossip_config.heartbeat_timeout
    );
    println!("   Seed nodes: {}", gossip_config.seed_nodes.len());
    println!("   Network transport: enabled");
    println!();

    // ========================================================================
    // Step 7: Cluster summary
    // ========================================================================
    println!("📊 Step 7: Cluster summary\n");

    println!("   Cluster size: {} nodes", membership1.cluster_size());
    println!("   Total nodes: {}", membership1.node_count());
    println!("   Partitions: {}", num_partitions);
    println!(
        "   State backend: In-memory ({} keys)",
        partition_states.len()
    );
    println!("   Network: TCP-based RPC and gossip");
    println!();

    println!("   Cluster topology:");
    for node in membership1.get_all_nodes() {
        let assigned_partitions: Vec<u32> =
            new_assignment.get(&node.id).cloned().unwrap_or_default();
        println!(
            "     {} @ {}: {} partitions",
            node.id,
            node.address,
            assigned_partitions.len()
        );
    }
    println!();

    // Cleanup
    discovery.stop().await;
    server1_handle.abort();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Cluster demonstration complete!                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
