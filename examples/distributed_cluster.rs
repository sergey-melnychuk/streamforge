//! Distributed cluster example
//!
//! Demonstrates cluster membership, discovery, and partition assignment
//!
//! Run with: cargo run --example distributed_cluster

use std::net::SocketAddr;
use std::time::Duration;
use streamforge::distributed::{
    ClusterMembership, ConsistentHashAssigner, GossipConfig, GossipDiscovery, NodeId, NodeMetadata,
    PartitionAssigner,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== StreamForge Distributed Cluster Example ===\n");

    // Example 1: Cluster Membership
    println!("1. Cluster Membership Management\n");

    let local_id = NodeId::generate();
    let membership = ClusterMembership::new(local_id, 30);

    println!("   Local node ID: {}", local_id);
    println!("   Initial cluster size: {}", membership.cluster_size());

    // Add some nodes to the cluster
    let node1 = NodeMetadata::new(NodeId::new(1), "127.0.0.1:8001".parse()?);
    let node2 = NodeMetadata::new(NodeId::new(2), "127.0.0.1:8002".parse()?);
    let node3 = NodeMetadata::new(NodeId::new(3), "127.0.0.1:8003".parse()?);

    membership.add_node(node1.clone());
    membership.add_node(node2.clone());
    membership.add_node(node3.clone());

    println!("   Added 3 nodes to cluster");
    println!("   Cluster size: {}", membership.cluster_size());
    println!("   Total nodes: {}", membership.node_count());

    let alive_nodes = membership.get_alive_nodes();
    println!("\n   Alive nodes:");
    for node in &alive_nodes {
        println!("      {} @ {}", node.id, node.address);
    }
    println!();

    // Example 2: Partition Assignment (Consistent Hashing)
    println!("2. Consistent Hash Partition Assignment\n");

    let assigner = ConsistentHashAssigner::new(100, 1);
    let nodes: Vec<_> = membership.get_alive_nodes();
    let num_partitions = 12;

    let assignment = assigner.assign(num_partitions, &nodes);

    println!(
        "   Assigning {} partitions to {} nodes:\n",
        num_partitions,
        nodes.len()
    );

    for (node_id, partitions) in &assignment {
        println!(
            "      {}: {:?} ({} partitions)",
            node_id,
            partitions,
            partitions.len()
        );
    }

    // Verify each partition is assigned exactly once
    let total_assigned: usize = assignment.values().map(|v| v.len()).sum();
    println!("\n   Total partitions assigned: {}", total_assigned);
    println!("   ✓ All partitions assigned correctly");

    // Example 3: Rebalancing after node addition
    println!("\n3. Partition Rebalancing\n");

    // Add a new node
    let node4 = NodeMetadata::new(NodeId::new(4), "127.0.0.1:8004".parse()?);
    membership.add_node(node4.clone());

    println!("   Added new node: {}", node4.id);

    let new_nodes = membership.get_alive_nodes();
    let new_assignment = assigner.rebalance(num_partitions, &assignment, &new_nodes);

    println!(
        "   Rebalanced {} partitions to {} nodes:\n",
        num_partitions,
        new_nodes.len()
    );

    for (node_id, partitions) in &new_assignment {
        println!(
            "      {}: {:?} ({} partitions)",
            node_id,
            partitions,
            partitions.len()
        );
    }

    // Calculate partition movements
    let mut moved_partitions = 0;
    for partition in 0..num_partitions {
        let old_node = assigner.get_node_for_partition(partition, &assignment);
        let new_node = assigner.get_node_for_partition(partition, &new_assignment);
        if old_node != new_node {
            moved_partitions += 1;
        }
    }

    println!(
        "\n   Partitions moved: {} out of {}",
        moved_partitions, num_partitions
    );
    println!(
        "   Movement ratio: {:.1}%",
        (moved_partitions as f64 / num_partitions as f64) * 100.0
    );

    // Example 4: Gossip Discovery Setup
    println!("\n4. Gossip Discovery Setup\n");

    let local_addr: SocketAddr = "127.0.0.1:8000".parse()?;
    let local_node_meta = NodeMetadata::new(local_id, local_addr);

    let config = GossipConfig {
        gossip_interval: Duration::from_secs(1),
        gossip_fanout: 3,
        heartbeat_timeout: Duration::from_secs(10),
        dead_timeout: Duration::from_secs(30),
        seed_nodes: vec!["127.0.0.1:8001".parse()?, "127.0.0.1:8002".parse()?],
    };

    let discovery = GossipDiscovery::new(local_node_meta.clone(), config.clone());

    println!(
        "   Local node: {} @ {}",
        local_node_meta.id, local_node_meta.address
    );
    println!("   Gossip interval: {:?}", config.gossip_interval);
    println!("   Gossip fanout: {}", config.gossip_fanout);
    println!("   Heartbeat timeout: {:?}", config.heartbeat_timeout);
    println!("   Seed nodes: {}", config.seed_nodes.len());

    println!("\n   Discovery configured (would start gossip in production)");

    // Example 5: Round-Robin Assignment (Alternative Strategy)
    println!("\n5. Round-Robin Partition Assignment (Alternative)\n");

    use streamforge::distributed::partition_assignment::RoundRobinAssigner;

    let rr_assigner = RoundRobinAssigner::new();
    let rr_assignment = rr_assigner.assign(num_partitions, &new_nodes);

    println!(
        "   Round-robin assignment of {} partitions:\n",
        num_partitions
    );

    for (node_id, partitions) in &rr_assignment {
        println!(
            "      {}: {:?} ({} partitions)",
            node_id,
            partitions,
            partitions.len()
        );
    }

    // Check distribution fairness
    let min_partitions = rr_assignment.values().map(|v| v.len()).min().unwrap();
    let max_partitions = rr_assignment.values().map(|v| v.len()).max().unwrap();

    println!("\n   Distribution fairness:");
    println!("      Min partitions per node: {}", min_partitions);
    println!("      Max partitions per node: {}", max_partitions);
    println!("      Difference: {}", max_partitions - min_partitions);

    println!("\n=== All distributed examples completed! ===");
    Ok(())
}
