//! Peer discovery using gossip protocol
//!
//! Implements SWIM-style gossip for node discovery and failure detection

use crate::distributed::membership::{ClusterMembership, MembershipEvent};
use crate::distributed::node::{NodeId, NodeMetadata, NodeStatus};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// Discovery protocol for finding cluster peers
pub trait Discovery: Send + Sync {
    /// Start the discovery process
    fn start(&mut self) -> impl std::future::Future<Output = ()> + Send;

    /// Stop the discovery process
    fn stop(&mut self) -> impl std::future::Future<Output = ()> + Send;

    /// Get membership events (can only be called once)
    fn events(&mut self) -> mpsc::Receiver<MembershipEvent>;
}

/// Configuration for gossip discovery
#[derive(Clone)]
pub struct GossipConfig {
    /// Interval between gossip rounds
    pub gossip_interval: Duration,
    /// Number of nodes to gossip with per round
    pub gossip_fanout: usize,
    /// Heartbeat timeout
    pub heartbeat_timeout: Duration,
    /// Dead node timeout (after suspected)
    pub dead_timeout: Duration,
    /// Seed nodes to bootstrap discovery
    pub seed_nodes: Vec<SocketAddr>,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            gossip_fanout: 3,
            heartbeat_timeout: Duration::from_secs(10),
            dead_timeout: Duration::from_secs(30),
            seed_nodes: Vec::new(),
        }
    }
}

/// Gossip message types
#[derive(Debug, Clone)]
pub enum GossipMessage {
    /// Announce presence to the cluster
    Join {
        node: NodeMetadata,
    },
    /// Periodic heartbeat
    Heartbeat {
        node_id: NodeId,
    },
    /// Share membership view
    MembershipDigest {
        nodes: Vec<NodeMetadata>,
    },
    /// Graceful departure
    Leave {
        node_id: NodeId,
    },
    /// Acknowledge message receipt
    Ack {
        node_id: NodeId,
    },
}

/// Gossip-based discovery implementation
pub struct GossipDiscovery {
    /// Local node metadata
    local_node: NodeMetadata,
    /// Cluster membership
    membership: ClusterMembership,
    /// Configuration
    config: GossipConfig,
    /// Event channel
    event_tx: mpsc::Sender<MembershipEvent>,
    event_rx: Option<mpsc::Receiver<MembershipEvent>>,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl GossipDiscovery {
    /// Create a new gossip discovery instance
    pub fn new(local_node: NodeMetadata, config: GossipConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);
        let membership = ClusterMembership::new(
            local_node.id,
            config.heartbeat_timeout.as_secs(),
        );

        Self {
            local_node,
            membership,
            config,
            event_tx,
            event_rx: Some(event_rx),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Get the cluster membership
    pub fn membership(&self) -> &ClusterMembership {
        &self.membership
    }

    /// Select random nodes for gossiping
    fn select_gossip_targets(&self) -> Vec<NodeMetadata> {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        let mut alive_nodes = self.membership.get_alive_nodes();
        // Don't gossip with self
        alive_nodes.retain(|n| n.id != self.local_node.id);

        alive_nodes.shuffle(&mut rng);
        alive_nodes.into_iter()
            .take(self.config.gossip_fanout)
            .collect()
    }

    /// Process a gossip message
    #[allow(dead_code)] // TODO: remove this
    async fn process_message(&self, msg: GossipMessage) -> Option<MembershipEvent> {
        match msg {
            GossipMessage::Join { node } => {
                self.membership.add_node(node)
            }
            GossipMessage::Heartbeat { node_id } => {
                self.membership.update_heartbeat(node_id);
                None
            }
            GossipMessage::MembershipDigest { nodes } => {
                // Merge received membership info
                for node in nodes {
                    if !self.membership.is_member(node.id) {
                        if let Some(event) = self.membership.add_node(node) {
                            let _ = self.event_tx.send(event).await;
                        }
                    }
                }
                None
            }
            GossipMessage::Leave { node_id } => {
                self.membership.update_status(node_id, NodeStatus::Leaving);
                self.membership.remove_node(node_id)
            }
            GossipMessage::Ack { .. } => None,
        }
    }

    /// Run the gossip protocol
    async fn gossip_loop(&self) {
        let mut interval = time::interval(self.config.gossip_interval);

        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            interval.tick().await;

            // Check for stale nodes
            let events = self.membership.check_stale_nodes();
            for event in events {
                let _ = self.event_tx.send(event).await;
            }

            // Check for dead nodes
            let dead_events = self.membership.check_dead_nodes(
                self.config.dead_timeout.as_secs()
            );
            for event in dead_events {
                let _ = self.event_tx.send(event).await;
            }

            // Select nodes to gossip with
            let targets = self.select_gossip_targets();

            // In a real implementation, we would send gossip messages here
            // For now, this is a placeholder for the gossip round
            for _target in targets {
                // TODO: Send heartbeat and membership digest
            }
        }
    }

    /// Bootstrap from seed nodes
    async fn bootstrap(&self) {
        // In a real implementation, we would contact seed nodes
        // and exchange membership information
        for _seed in &self.config.seed_nodes {
            // TODO: Contact seed node and join cluster
        }
    }
}

impl Discovery for GossipDiscovery {
    async fn start(&mut self) {
        self.running.store(true, std::sync::atomic::Ordering::Relaxed);

        // Bootstrap from seed nodes
        self.bootstrap().await;

        // Start gossip loop
        let discovery = self.clone();
        tokio::spawn(async move {
            discovery.gossip_loop().await;
        });
    }

    async fn stop(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    fn events(&mut self) -> mpsc::Receiver<MembershipEvent> {
        self.event_rx.take().expect("events() can only be called once")
    }
}

impl Clone for GossipDiscovery {
    fn clone(&self) -> Self {
        Self {
            local_node: self.local_node.clone(),
            membership: self.membership.clone(),
            config: self.config.clone(),
            event_tx: self.event_tx.clone(),
            event_rx: None, // Can't clone receiver
            running: Arc::clone(&self.running),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::node::NodeId;

    fn create_test_node(id: u64, port: u16) -> NodeMetadata {
        let node_id = NodeId::new(id);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        NodeMetadata::new(node_id, addr)
    }

    #[test]
    fn test_gossip_config_default() {
        let config = GossipConfig::default();
        assert_eq!(config.gossip_interval, Duration::from_secs(1));
        assert_eq!(config.gossip_fanout, 3);
    }

    #[tokio::test]
    async fn test_gossip_discovery_creation() {
        let node = create_test_node(1, 8080);
        let config = GossipConfig::default();
        let discovery = GossipDiscovery::new(node, config);

        assert_eq!(discovery.membership().local_id(), NodeId::new(1));
    }

    #[tokio::test]
    async fn test_process_join_message() {
        let local_node = create_test_node(1, 8080);
        let config = GossipConfig::default();
        let discovery = GossipDiscovery::new(local_node, config);

        let new_node = create_test_node(2, 8081);
        let msg = GossipMessage::Join { node: new_node.clone() };

        let event = discovery.process_message(msg).await;
        assert!(matches!(event, Some(MembershipEvent::NodeJoined(_))));
        assert_eq!(discovery.membership().node_count(), 1);
    }

    #[tokio::test]
    async fn test_process_heartbeat_message() {
        let local_node = create_test_node(1, 8080);
        let config = GossipConfig::default();
        let discovery = GossipDiscovery::new(local_node, config);

        let new_node = create_test_node(2, 8081);
        discovery.membership().add_node(new_node.clone());

        let msg = GossipMessage::Heartbeat { node_id: new_node.id };
        let event = discovery.process_message(msg).await;

        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_select_gossip_targets() {
        let local_node = create_test_node(1, 8080);
        let mut config = GossipConfig::default();
        config.gossip_fanout = 2;
        let discovery = GossipDiscovery::new(local_node, config);

        // Add some nodes
        for i in 2..6 {
            discovery.membership().add_node(create_test_node(i, 8080 + i as u16));
        }

        let targets = discovery.select_gossip_targets();
        assert!(targets.len() <= 2); // Should select up to fanout nodes

        // Should not include self
        assert!(!targets.iter().any(|n| n.id == NodeId::new(1)));
    }
}
