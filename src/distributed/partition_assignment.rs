//! Partition assignment strategies for distributing work across nodes
//!
//! Assigns partitions to nodes using consistent hashing

use crate::distributed::node::{NodeId, NodeMetadata};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Strategy for assigning partitions to nodes
pub trait PartitionAssigner: Send + Sync {
    /// Assign partitions to nodes
    fn assign(&self, partitions: u32, nodes: &[NodeMetadata]) -> HashMap<NodeId, Vec<u32>>;

    /// Rebalance partitions when cluster topology changes
    fn rebalance(
        &self,
        partitions: u32,
        old_assignment: &HashMap<NodeId, Vec<u32>>,
        nodes: &[NodeMetadata],
    ) -> HashMap<NodeId, Vec<u32>>;

    /// Get the node responsible for a specific partition
    fn get_node_for_partition(&self, partition: u32, assignment: &HashMap<NodeId, Vec<u32>>) -> Option<NodeId>;
}

/// Consistent hash ring for partition assignment
struct HashRing {
    /// Virtual nodes per physical node
    virtual_nodes: u32,
    /// Ring entries (hash -> node_id)
    ring: Vec<(u64, NodeId)>,
}

impl HashRing {
    /// Create a new hash ring
    fn new(virtual_nodes: u32) -> Self {
        Self {
            virtual_nodes,
            ring: Vec::new(),
        }
    }

    /// Add a node to the ring
    fn add_node(&mut self, node_id: NodeId) {
        for i in 0..self.virtual_nodes {
            let hash = self.hash_virtual_node(node_id, i);
            self.ring.push((hash, node_id));
        }
        self.ring.sort_by_key(|(h, _)| *h);
    }

    /// Remove a node from the ring
    #[allow(dead_code)] // TODO: remove this
    fn remove_node(&mut self, node_id: NodeId) {
        self.ring.retain(|(_, id)| *id != node_id);
    }

    /// Find the node responsible for a hash value
    fn get_node(&self, hash: u64) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        // Binary search for the first node >= hash
        let idx = match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(idx) => idx,
            Err(idx) => {
                // Wrap around to the first node
                if idx >= self.ring.len() {
                    0
                } else {
                    idx
                }
            }
        };

        Some(self.ring[idx].1)
    }

    /// Hash a virtual node
    fn hash_virtual_node(&self, node_id: NodeId, virtual_idx: u32) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        node_id.as_u64().hash(&mut hasher);
        virtual_idx.hash(&mut hasher);
        hasher.finish()
    }

    /// Hash a partition ID
    fn hash_partition(&self, partition: u32) -> u64 {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        partition.hash(&mut hasher);
        hasher.finish()
    }
}

/// Consistent hash-based partition assigner
pub struct ConsistentHashAssigner {
    /// Number of virtual nodes per physical node
    pub virtual_nodes: u32,
    /// Replication factor
    pub replication_factor: u32,
}

impl ConsistentHashAssigner {
    /// Create a new consistent hash assigner
    pub fn new(virtual_nodes: u32, replication_factor: u32) -> Self {
        Self {
            virtual_nodes,
            replication_factor,
        }
    }

    /// Default configuration (100 virtual nodes, no replication)
    pub fn default() -> Self {
        Self::new(100, 1)
    }
}

impl PartitionAssigner for ConsistentHashAssigner {
    fn assign(&self, partitions: u32, nodes: &[NodeMetadata]) -> HashMap<NodeId, Vec<u32>> {
        let mut ring = HashRing::new(self.virtual_nodes);
        let mut assignment: HashMap<NodeId, Vec<u32>> = HashMap::new();

        // Build the hash ring
        for node in nodes {
            ring.add_node(node.id);
            assignment.insert(node.id, Vec::new());
        }

        if nodes.is_empty() {
            return assignment;
        }

        // Assign each partition to a node
        for partition in 0..partitions {
            let hash = ring.hash_partition(partition);
            if let Some(node_id) = ring.get_node(hash) {
                assignment.get_mut(&node_id).unwrap().push(partition);
            }
        }

        assignment
    }

    fn rebalance(
        &self,
        partitions: u32,
        old_assignment: &HashMap<NodeId, Vec<u32>>,
        nodes: &[NodeMetadata],
    ) -> HashMap<NodeId, Vec<u32>> {
        // For now, just do a full reassignment
        // In a production system, this would minimize partition movement
        let new_assignment = self.assign(partitions, nodes);

        // Log partition movements for observability
        let old_nodes: HashSet<_> = old_assignment.keys().cloned().collect();
        let new_nodes: HashSet<_> = nodes.iter().map(|n| n.id).collect();

        let added_nodes: Vec<_> = new_nodes.difference(&old_nodes).collect();
        let removed_nodes: Vec<_> = old_nodes.difference(&new_nodes).collect();

        if !added_nodes.is_empty() {
            tracing::info!("Added nodes: {:?}", added_nodes);
        }
        if !removed_nodes.is_empty() {
            tracing::info!("Removed nodes: {:?}", removed_nodes);
        }

        new_assignment
    }

    fn get_node_for_partition(&self, partition: u32, assignment: &HashMap<NodeId, Vec<u32>>) -> Option<NodeId> {
        for (node_id, partitions) in assignment {
            if partitions.contains(&partition) {
                return Some(*node_id);
            }
        }
        None
    }
}

/// Round-robin partition assigner (simpler strategy)
pub struct RoundRobinAssigner;

impl Default for RoundRobinAssigner {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinAssigner {
    pub fn new() -> Self {
        Self
    }
}

impl PartitionAssigner for RoundRobinAssigner {
    fn assign(&self, partitions: u32, nodes: &[NodeMetadata]) -> HashMap<NodeId, Vec<u32>> {
        let mut assignment: HashMap<NodeId, Vec<u32>> = HashMap::new();

        if nodes.is_empty() {
            return assignment;
        }

        // Initialize assignment for each node
        for node in nodes {
            assignment.insert(node.id, Vec::new());
        }

        // Round-robin assignment
        for partition in 0..partitions {
            let node_idx = (partition as usize) % nodes.len();
            let node_id = nodes[node_idx].id;
            assignment.get_mut(&node_id).unwrap().push(partition);
        }

        assignment
    }

    fn rebalance(
        &self,
        partitions: u32,
        _old_assignment: &HashMap<NodeId, Vec<u32>>,
        nodes: &[NodeMetadata],
    ) -> HashMap<NodeId, Vec<u32>> {
        // Round-robin just reassigns everything
        self.assign(partitions, nodes)
    }

    fn get_node_for_partition(&self, partition: u32, assignment: &HashMap<NodeId, Vec<u32>>) -> Option<NodeId> {
        for (node_id, partitions) in assignment {
            if partitions.contains(&partition) {
                return Some(*node_id);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn create_test_node(id: u64, port: u16) -> NodeMetadata {
        let node_id = NodeId::new(id);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        NodeMetadata::new(node_id, addr)
    }

    #[test]
    fn test_hash_ring_creation() {
        let mut ring = HashRing::new(10);
        assert_eq!(ring.ring.len(), 0);

        ring.add_node(NodeId::new(1));
        assert_eq!(ring.ring.len(), 10); // 10 virtual nodes
    }

    #[test]
    fn test_hash_ring_node_lookup() {
        let mut ring = HashRing::new(10);
        ring.add_node(NodeId::new(1));
        ring.add_node(NodeId::new(2));

        let hash = ring.hash_partition(100);
        let node = ring.get_node(hash);
        assert!(node.is_some());
    }

    #[test]
    fn test_consistent_hash_assignment() {
        let assigner = ConsistentHashAssigner::default();
        let nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
            create_test_node(3, 8082),
        ];

        let assignment = assigner.assign(12, &nodes);

        // All nodes should have at least one partition
        assert_eq!(assignment.len(), 3);

        // All partitions should be assigned
        let total_partitions: usize = assignment.values().map(|v| v.len()).sum();
        assert_eq!(total_partitions, 12);

        // Each partition should be assigned exactly once
        let mut all_partitions: Vec<u32> = assignment.values().flatten().copied().collect();
        all_partitions.sort();
        assert_eq!(all_partitions, (0..12).collect::<Vec<u32>>());
    }

    #[test]
    fn test_consistent_hash_get_node_for_partition() {
        let assigner = ConsistentHashAssigner::default();
        let nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
        ];

        let assignment = assigner.assign(10, &nodes);
        let node = assigner.get_node_for_partition(0, &assignment);
        assert!(node.is_some());
    }

    #[test]
    fn test_round_robin_assignment() {
        let assigner = RoundRobinAssigner::new();
        let nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
            create_test_node(3, 8082),
        ];

        let assignment = assigner.assign(9, &nodes);

        // All nodes should have exactly 3 partitions (9 / 3)
        assert_eq!(assignment.len(), 3);
        for partitions in assignment.values() {
            assert_eq!(partitions.len(), 3);
        }

        // All partitions should be assigned
        let total_partitions: usize = assignment.values().map(|v| v.len()).sum();
        assert_eq!(total_partitions, 9);
    }

    #[test]
    fn test_round_robin_uneven_distribution() {
        let assigner = RoundRobinAssigner::new();
        let nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
        ];

        let assignment = assigner.assign(5, &nodes);

        // 5 partitions / 2 nodes = 2 and 3
        let counts: Vec<usize> = assignment.values().map(|v| v.len()).collect();
        assert!(counts.contains(&2));
        assert!(counts.contains(&3));
    }

    #[test]
    fn test_rebalance_with_node_addition() {
        let assigner = ConsistentHashAssigner::default();
        let initial_nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
        ];

        let old_assignment = assigner.assign(10, &initial_nodes);

        // Add a new node
        let new_nodes = vec![
            create_test_node(1, 8080),
            create_test_node(2, 8081),
            create_test_node(3, 8082),
        ];

        let new_assignment = assigner.rebalance(10, &old_assignment, &new_nodes);

        // New node should have some partitions
        assert!(new_assignment.get(&NodeId::new(3)).is_some());
        assert!(!new_assignment.get(&NodeId::new(3)).unwrap().is_empty());
    }

    #[test]
    fn test_empty_nodes() {
        let assigner = ConsistentHashAssigner::default();
        let assignment = assigner.assign(10, &[]);
        assert!(assignment.is_empty());
    }
}
