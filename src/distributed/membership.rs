//! Cluster membership management
//!
//! Tracks which nodes are part of the cluster and their status

use crate::distributed::node::{NodeId, NodeMetadata, NodeStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

/// Events that can occur in cluster membership
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// A node has joined the cluster
    NodeJoined(NodeId),
    /// A node has left the cluster
    NodeLeft(NodeId),
    /// A node is suspected to have failed
    NodeSuspected(NodeId),
    /// A node has been confirmed dead
    NodeDead(NodeId),
    /// A node's status has changed
    NodeStatusChanged(NodeId, NodeStatus),
}

/// Manages cluster membership
pub struct ClusterMembership {
    /// This node's ID
    local_id: NodeId,
    /// All known nodes in the cluster
    nodes: Arc<RwLock<HashMap<NodeId, NodeMetadata>>>,
    /// Timeout for heartbeat staleness (in seconds)
    heartbeat_timeout: u64,
}

impl ClusterMembership {
    /// Create a new cluster membership manager
    pub fn new(local_id: NodeId, heartbeat_timeout: u64) -> Self {
        Self {
            local_id,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout,
        }
    }

    /// Get the local node ID
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Add a new node to the cluster
    pub fn add_node(&self, node: NodeMetadata) -> Option<MembershipEvent> {
        let mut nodes = self.nodes.write().unwrap();
        let node_id = node.id;

        if nodes.contains_key(&node_id) {
            return None;
        }

        nodes.insert(node_id, node);
        Some(MembershipEvent::NodeJoined(node_id))
    }

    /// Remove a node from the cluster
    pub fn remove_node(&self, node_id: NodeId) -> Option<MembershipEvent> {
        let mut nodes = self.nodes.write().unwrap();
        nodes.remove(&node_id)?;
        Some(MembershipEvent::NodeLeft(node_id))
    }

    /// Update a node's heartbeat
    pub fn update_heartbeat(&self, node_id: NodeId) -> bool {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.update_heartbeat();
            true
        } else {
            false
        }
    }

    /// Update a node's status
    pub fn update_status(&self, node_id: NodeId, status: NodeStatus) -> Option<MembershipEvent> {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(&node_id) {
            let old_status = node.status;
            if old_status != status {
                node.status = status;
                return Some(MembershipEvent::NodeStatusChanged(node_id, status));
            }
        }
        None
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<NodeMetadata> {
        let nodes = self.nodes.read().unwrap();
        nodes.get(&node_id).cloned()
    }

    /// Get all nodes
    pub fn get_all_nodes(&self) -> Vec<NodeMetadata> {
        let nodes = self.nodes.read().unwrap();
        nodes.values().cloned().collect()
    }

    /// Get all alive nodes
    pub fn get_alive_nodes(&self) -> Vec<NodeMetadata> {
        let nodes = self.nodes.read().unwrap();
        nodes
            .values()
            .filter(|n| n.status == NodeStatus::Alive)
            .cloned()
            .collect()
    }

    /// Get the number of nodes in the cluster
    pub fn node_count(&self) -> usize {
        let nodes = self.nodes.read().unwrap();
        nodes.len()
    }

    /// Get the number of alive nodes
    pub fn alive_node_count(&self) -> usize {
        let nodes = self.nodes.read().unwrap();
        nodes
            .values()
            .filter(|n| n.status == NodeStatus::Alive)
            .count()
    }

    /// Check for stale nodes and mark them as suspected
    pub fn check_stale_nodes(&self) -> Vec<MembershipEvent> {
        let mut events = Vec::new();
        let mut nodes = self.nodes.write().unwrap();

        for (node_id, node) in nodes.iter_mut() {
            if node.status == NodeStatus::Alive && node.is_stale(self.heartbeat_timeout) {
                node.status = NodeStatus::Suspected;
                events.push(MembershipEvent::NodeSuspected(*node_id));
            }
        }

        events
    }

    /// Mark suspected nodes as dead after extended timeout
    pub fn check_dead_nodes(&self, dead_timeout: u64) -> Vec<MembershipEvent> {
        let mut events = Vec::new();
        let mut nodes = self.nodes.write().unwrap();

        for (node_id, node) in nodes.iter_mut() {
            if node.status == NodeStatus::Suspected && node.is_stale(dead_timeout) {
                node.status = NodeStatus::Dead;
                events.push(MembershipEvent::NodeDead(*node_id));
            }
        }

        events
    }

    /// Get cluster size (alive nodes)
    pub fn cluster_size(&self) -> usize {
        self.alive_node_count()
    }

    /// Check if a node is a member of the cluster
    pub fn is_member(&self, node_id: NodeId) -> bool {
        let nodes = self.nodes.read().unwrap();
        nodes.contains_key(&node_id)
    }

    /// Get node address by ID
    pub fn get_node_address(&self, node_id: NodeId) -> Option<SocketAddr> {
        let nodes = self.nodes.read().unwrap();
        nodes.get(&node_id).map(|n| n.address)
    }
}

impl Clone for ClusterMembership {
    fn clone(&self) -> Self {
        Self {
            local_id: self.local_id,
            nodes: Arc::clone(&self.nodes),
            heartbeat_timeout: self.heartbeat_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node(id: u64, port: u16) -> NodeMetadata {
        let node_id = NodeId::new(id);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        NodeMetadata::new(node_id, addr)
    }

    #[test]
    fn test_membership_creation() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        assert_eq!(membership.local_id(), local_id);
        assert_eq!(membership.node_count(), 0);
    }

    #[test]
    fn test_add_remove_node() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        let node = create_test_node(2, 8080);
        let event = membership.add_node(node.clone());
        assert!(matches!(event, Some(MembershipEvent::NodeJoined(_))));
        assert_eq!(membership.node_count(), 1);

        let event = membership.remove_node(node.id);
        assert!(matches!(event, Some(MembershipEvent::NodeLeft(_))));
        assert_eq!(membership.node_count(), 0);
    }

    #[test]
    fn test_heartbeat_update() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        let node = create_test_node(2, 8080);
        membership.add_node(node.clone());

        let updated = membership.update_heartbeat(node.id);
        assert!(updated);

        let not_updated = membership.update_heartbeat(NodeId::new(999));
        assert!(!not_updated);
    }

    #[test]
    fn test_status_update() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        let node = create_test_node(2, 8080);
        membership.add_node(node.clone());

        let event = membership.update_status(node.id, NodeStatus::Suspected);
        assert!(matches!(
            event,
            Some(MembershipEvent::NodeStatusChanged(_, NodeStatus::Suspected))
        ));

        let retrieved = membership.get_node(node.id).unwrap();
        assert_eq!(retrieved.status, NodeStatus::Suspected);
    }

    #[test]
    fn test_get_alive_nodes() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        membership.add_node(create_test_node(2, 8080));
        membership.add_node(create_test_node(3, 8081));
        membership.add_node(create_test_node(4, 8082));

        membership.update_status(NodeId::new(3), NodeStatus::Suspected);

        let alive_nodes = membership.get_alive_nodes();
        assert_eq!(alive_nodes.len(), 2);
        assert_eq!(membership.alive_node_count(), 2);
    }

    #[test]
    fn test_cluster_size() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        membership.add_node(create_test_node(2, 8080));
        membership.add_node(create_test_node(3, 8081));

        assert_eq!(membership.cluster_size(), 2);

        membership.update_status(NodeId::new(2), NodeStatus::Dead);
        assert_eq!(membership.cluster_size(), 1);
    }

    #[test]
    fn test_is_member() {
        let local_id = NodeId::new(1);
        let membership = ClusterMembership::new(local_id, 30);

        let node = create_test_node(2, 8080);
        membership.add_node(node.clone());

        assert!(membership.is_member(node.id));
        assert!(!membership.is_member(NodeId::new(999)));
    }
}
