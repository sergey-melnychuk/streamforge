//! Partitioning logic for distributing events across parallel processors

use crate::core::EventKey;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Partition identifier
pub type Partition = u32;

/// Key used for partitioning
pub type PartitionKey = EventKey;

/// Trait for partitioning events
///
/// Partitioners determine which partition an event should be sent to
/// based on its key. This enables parallel processing and data locality.
pub trait Partitioner: Send + Sync {
    /// Determine the partition for the given key
    fn partition(&self, key: &PartitionKey, num_partitions: u32) -> Partition;
}

/// Hash-based partitioner using DefaultHasher
///
/// This is the default partitioning strategy. Events with the same key
/// will always go to the same partition.
#[derive(Debug, Clone, Default)]
pub struct HashPartitioner;

impl Partitioner for HashPartitioner {
    fn partition(&self, key: &PartitionKey, num_partitions: u32) -> Partition {
        if num_partitions == 0 {
            return 0;
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        (hash % num_partitions as u64) as u32
    }
}

/// Round-robin partitioner
///
/// Distributes events evenly across partitions in a round-robin fashion,
/// ignoring the key. Useful for load balancing when key-based partitioning
/// is not required.
#[derive(Debug)]
pub struct RoundRobinPartitioner {
    counter: std::sync::atomic::AtomicU64,
}

impl RoundRobinPartitioner {
    pub fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RoundRobinPartitioner {
    fn partition(&self, _key: &PartitionKey, num_partitions: u32) -> Partition {
        if num_partitions == 0 {
            return 0;
        }

        let count = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (count % num_partitions as u64) as u32
    }
}

/// Custom partitioner that allows user-defined partitioning logic
pub struct CustomPartitioner<F>
where
    F: Fn(&PartitionKey, u32) -> Partition + Send + Sync,
{
    partition_fn: F,
}

impl<F> CustomPartitioner<F>
where
    F: Fn(&PartitionKey, u32) -> Partition + Send + Sync,
{
    pub fn new(partition_fn: F) -> Self {
        Self { partition_fn }
    }
}

impl<F> Partitioner for CustomPartitioner<F>
where
    F: Fn(&PartitionKey, u32) -> Partition + Send + Sync,
{
    fn partition(&self, key: &PartitionKey, num_partitions: u32) -> Partition {
        (self.partition_fn)(key, num_partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_partitioner() {
        let partitioner = HashPartitioner;
        let key = EventKey::from_str("test-key");

        // Same key should always go to same partition
        let p1 = partitioner.partition(&key, 10);
        let p2 = partitioner.partition(&key, 10);
        assert_eq!(p1, p2);
        assert!(p1 < 10);

        // Different keys might go to different partitions
        let key2 = EventKey::from_str("other-key");
        let p3 = partitioner.partition(&key2, 10);
        assert!(p3 < 10);
    }

    #[test]
    fn test_round_robin_partitioner() {
        let partitioner = RoundRobinPartitioner::new();
        let key = EventKey::from_str("test-key");

        let partitions: Vec<_> = (0..10)
            .map(|_| partitioner.partition(&key, 5))
            .collect();

        // Should cycle through partitions
        assert_eq!(partitions, vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_custom_partitioner() {
        // Custom logic: partition by key type
        let partitioner = CustomPartitioner::new(|key: &PartitionKey, num_partitions: u32| {
            match key {
                EventKey::String(_) => 0,
                EventKey::Int(_) => 1,
                EventKey::Bytes(_) => 2,
                EventKey::None => 0,
            }
            .min(num_partitions - 1)
        });

        assert_eq!(
            partitioner.partition(&EventKey::from_str("test"), 10),
            0
        );
        assert_eq!(partitioner.partition(&EventKey::from_int(42), 10), 1);
    }

    #[test]
    fn test_partitioner_with_zero_partitions() {
        let partitioner = HashPartitioner;
        let key = EventKey::from_str("test");
        assert_eq!(partitioner.partition(&key, 0), 0);
    }
}
