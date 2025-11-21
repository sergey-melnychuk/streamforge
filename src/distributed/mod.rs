//! Distributed coordination and clustering
//!
//! This module provides zero-infrastructure distributed setup with:
//! - Gossip-based peer discovery (SWIM protocol)
//! - Raft consensus for coordination (leader election, log replication)
//! - Automatic partition assignment (consistent hashing)
//! - Failure detection and recovery
//! - Network communication (RPC, transport)

pub mod consensus;
pub mod discovery;
pub mod membership;
pub mod node;
pub mod partition_assignment;
pub mod replication;

pub use consensus::{Raft, RaftConfig, RaftError, RaftEvent, RaftResult, RaftRole};
pub use discovery::{Discovery, GossipConfig, GossipDiscovery, GossipMessage};
pub use membership::{ClusterMembership, MembershipEvent};
pub use node::{Node, NodeId, NodeMetadata, NodeStatus};
pub use partition_assignment::{ConsistentHashAssigner, PartitionAssigner};
pub use replication::{PartitionReplication, ReplicaRole, ReplicationManager};
