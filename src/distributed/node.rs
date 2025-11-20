//! Node model and metadata
//!
//! Defines the identity and state of nodes in the cluster

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    /// Create a new NodeId from a u64
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate a random NodeId
    pub fn generate() -> Self {
        use std::collections::hash_map::RandomState;
        use std::hash::BuildHasher;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let state = RandomState::new();
        
        

        Self(state.hash_one(now))
    }

    /// Get the raw u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{:016x}", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

/// Status of a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Node is alive and participating
    Alive,
    /// Node is suspected to have failed
    Suspected,
    /// Node has been confirmed dead
    Dead,
    /// Node is leaving the cluster gracefully
    Leaving,
}

/// Metadata about a node in the cluster
#[derive(Debug, Clone)]
pub struct NodeMetadata {
    /// Unique node identifier
    pub id: NodeId,
    /// Network address for communication
    pub address: SocketAddr,
    /// Current status
    pub status: NodeStatus,
    /// When the node joined the cluster
    pub joined_at: u64,
    /// Last heartbeat timestamp
    pub last_heartbeat: u64,
    /// Partition IDs assigned to this node
    pub partitions: Vec<u32>,
    /// Custom tags for node properties (e.g., datacenter, rack)
    pub tags: Vec<(String, String)>,
}

impl NodeMetadata {
    /// Create new node metadata
    pub fn new(id: NodeId, address: SocketAddr) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            address,
            status: NodeStatus::Alive,
            joined_at: now,
            last_heartbeat: now,
            partitions: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Update the last heartbeat timestamp
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Check if node heartbeat is stale (older than timeout)
    pub fn is_stale(&self, timeout_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        now - self.last_heartbeat > timeout_secs
    }

    /// Add a tag to the node
    pub fn add_tag(&mut self, key: String, value: String) {
        self.tags.push((key, value));
    }

    /// Get a tag value by key
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// A node in the cluster
#[derive(Clone)]
pub struct Node {
    metadata: Arc<NodeMetadata>,
}

impl Node {
    /// Create a new node
    pub fn new(id: NodeId, address: SocketAddr) -> Self {
        Self {
            metadata: Arc::new(NodeMetadata::new(id, address)),
        }
    }

    /// Get the node ID
    pub fn id(&self) -> NodeId {
        self.metadata.id
    }

    /// Get the node address
    pub fn address(&self) -> SocketAddr {
        self.metadata.address
    }

    /// Get the node status
    pub fn status(&self) -> NodeStatus {
        self.metadata.status
    }

    /// Get the node metadata
    pub fn metadata(&self) -> &NodeMetadata {
        &self.metadata
    }

    /// Check if this node is alive
    pub fn is_alive(&self) -> bool {
        self.metadata.status == NodeStatus::Alive
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.metadata.id)
            .field("address", &self.metadata.address)
            .field("status", &self.metadata.status)
            .finish()
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.metadata.id, self.metadata.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_generation() {
        let id1 = NodeId::generate();
        let id2 = NodeId::generate();

        // Should be different (with very high probability)
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::new(0x1234567890abcdef);
        let display = format!("{}", id);
        assert_eq!(display, "node-1234567890abcdef");
    }

    #[test]
    fn test_node_creation() {
        let id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let node = Node::new(id, addr);

        assert_eq!(node.id(), id);
        assert_eq!(node.address(), addr);
        assert!(node.is_alive());
    }

    #[test]
    fn test_node_metadata_heartbeat() {
        let id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut metadata = NodeMetadata::new(id, addr);

        let initial_heartbeat = metadata.last_heartbeat;
        std::thread::sleep(std::time::Duration::from_secs(2));
        metadata.update_heartbeat();

        assert!(metadata.last_heartbeat > initial_heartbeat);
    }

    #[test]
    fn test_node_metadata_tags() {
        let id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut metadata = NodeMetadata::new(id, addr);

        metadata.add_tag("datacenter".to_string(), "us-east-1".to_string());
        metadata.add_tag("rack".to_string(), "rack-1".to_string());

        assert_eq!(metadata.get_tag("datacenter"), Some("us-east-1"));
        assert_eq!(metadata.get_tag("rack"), Some("rack-1"));
        assert_eq!(metadata.get_tag("zone"), None);
    }

    #[test]
    fn test_node_stale_detection() {
        let id = NodeId::new(1);
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut metadata = NodeMetadata::new(id, addr);

        // Fresh node should not be stale
        assert!(!metadata.is_stale(10));

        // Simulate old heartbeat
        metadata.last_heartbeat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - 20;

        // Should be stale with 10 second timeout
        assert!(metadata.is_stale(10));
    }
}
