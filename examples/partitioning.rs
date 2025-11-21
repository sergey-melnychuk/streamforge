//! Partitioning example
//!
//! Demonstrates how events are partitioned by key
//!
//! Run with: cargo run --example partitioning

use streamforge::core::partition::{HashPartitioner, Partitioner, RoundRobinPartitioner};
use streamforge::core::EventKey;

fn main() {
    println!("=== StreamForge Partitioning Example ===\n");

    // Example 1: Hash partitioning
    println!("1. Hash Partitioning (consistent by key):");
    let hash_partitioner = HashPartitioner;
    let num_partitions = 4;

    let keys = vec![
        EventKey::from_str("user-123"),
        EventKey::from_str("user-456"),
        EventKey::from_str("user-123"), // Same as first
        EventKey::from_str("user-789"),
        EventKey::from_int(42),
        EventKey::from_int(42), // Same as previous
    ];

    for key in &keys {
        let partition = hash_partitioner.partition(key, num_partitions);
        println!("   Key: {:15} -> Partition: {}", key.to_string(), partition);
    }

    println!("\n   Note: Same keys always go to same partition\n");

    // Example 2: Round-robin partitioning
    println!("2. Round-Robin Partitioning (load balancing):");
    let rr_partitioner = RoundRobinPartitioner::new();

    let key = EventKey::from_str("any-key");
    for i in 0..8 {
        let partition = rr_partitioner.partition(&key, num_partitions);
        println!("   Request {} -> Partition: {}", i, partition);
    }

    println!("\n   Note: Events distributed evenly regardless of key\n");

    // Example 3: Partition distribution statistics
    println!("3. Distribution Analysis:");
    let hash_partitioner = HashPartitioner;
    let num_test_keys = 1000;
    let num_partitions = 8;

    let mut partition_counts = vec![0; num_partitions as usize];

    for i in 0..num_test_keys {
        let key = EventKey::from_int(i);
        let partition = hash_partitioner.partition(&key, num_partitions);
        partition_counts[partition as usize] += 1;
    }

    println!(
        "   Distribution of {} keys across {} partitions:",
        num_test_keys, num_partitions
    );
    for (partition, count) in partition_counts.iter().enumerate() {
        let percentage = (*count as f64 / num_test_keys as f64) * 100.0;
        println!(
            "   Partition {}: {:3} keys ({:.1}%)",
            partition, count, percentage
        );
    }

    println!("\n=== Example completed! ===");
}
