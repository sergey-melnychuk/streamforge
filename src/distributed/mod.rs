//! Distributed coordination and clustering
//!
//! This module provides zero-infrastructure distributed setup with:
//! - Gossip-based peer discovery
//! - Raft consensus for coordination
//! - Automatic partition assignment
//! - Failure detection and recovery

pub mod node;
pub mod membership;
pub mod discovery;
pub mod partition_assignment;

pub use node::{Node, NodeId, NodeMetadata, NodeStatus};
pub use membership::{ClusterMembership, MembershipEvent};
pub use discovery::{Discovery, GossipDiscovery, GossipConfig, GossipMessage};
pub use partition_assignment::{PartitionAssigner, ConsistentHashAssigner};
