//! Distributed coordination and clustering
//!
//! This module provides zero-infrastructure distributed setup with:
//! - Gossip-based peer discovery (SWIM protocol)
//! - Raft consensus for coordination (leader election, log replication)
//! - Automatic partition assignment (consistent hashing)
//! - Failure detection and recovery
//! - Network communication (RPC, transport)

pub mod node;
pub mod membership;
pub mod discovery;
pub mod partition_assignment;
pub mod consensus;
pub mod replication;

pub use node::{Node, NodeId, NodeMetadata, NodeStatus};
pub use membership::{ClusterMembership, MembershipEvent};
pub use discovery::{Discovery, GossipDiscovery, GossipConfig, GossipMessage};
pub use partition_assignment::{PartitionAssigner, ConsistentHashAssigner};
pub use consensus::{Raft, RaftConfig, RaftEvent, RaftRole, RaftError, RaftResult};
pub use replication::{ReplicationManager, ReplicaRole, PartitionReplication};
