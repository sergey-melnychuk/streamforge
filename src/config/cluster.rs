//! Cluster configuration

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Local node ID (auto-generated if None)
    pub node_id: Option<u64>,
    /// Bind address for cluster communication
    pub bind_address: String,
    /// Seed nodes for cluster discovery
    pub seed_nodes: Vec<String>,
    /// Gossip configuration
    pub gossip: GossipConfig,
    /// Raft configuration
    pub raft: RaftConfig,
    /// Replication configuration
    pub replication: ReplicationConfig,
}

/// Gossip protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    /// Gossip interval (seconds)
    pub gossip_interval: u64,
    /// Gossip fanout (number of nodes to gossip with)
    pub gossip_fanout: usize,
    /// Heartbeat timeout (seconds)
    pub heartbeat_timeout: u64,
    /// Dead node timeout (seconds)
    pub dead_timeout: u64,
}

/// Raft consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Election timeout minimum (milliseconds)
    pub election_timeout_min: u64,
    /// Election timeout maximum (milliseconds)
    pub election_timeout_max: u64,
    /// Heartbeat interval (milliseconds)
    pub heartbeat_interval: u64,
}

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Replication factor (number of replicas per partition)
    pub replication_factor: u32,
    /// Replica sync mode
    pub sync_mode: ReplicaSyncMode,
    /// Enable read replicas (route reads to followers)
    pub enable_read_replicas: bool,
}

/// Replica synchronization mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaSyncMode {
    /// Synchronous replication (wait for all replicas)
    Sync,
    /// Asynchronous replication (fire and forget)
    Async,
    /// Quorum-based (wait for majority)
    Quorum,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            bind_address: "127.0.0.1:9000".to_string(),
            seed_nodes: Vec::new(),
            gossip: GossipConfig::default(),
            raft: RaftConfig::default(),
            replication: ReplicationConfig::default(),
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replication_factor: 1, // No replication by default
            sync_mode: ReplicaSyncMode::Async,
            enable_read_replicas: false,
        }
    }
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            gossip_interval: 1,
            gossip_fanout: 3,
            heartbeat_timeout: 10,
            dead_timeout: 30,
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
        }
    }
}

