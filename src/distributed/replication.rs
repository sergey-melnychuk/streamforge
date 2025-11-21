//! Data replication for fault tolerance
//!
//! Implements leader-follower replication for partitions across nodes

use crate::distributed::node::NodeId;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Replication role for a partition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    /// Leader: handles writes and coordinates replication
    Leader,
    /// Follower: receives replicated data from leader
    Follower,
}

/// Replica information for a partition
#[derive(Debug, Clone)]
pub struct PartitionReplica {
    /// Node ID hosting this replica
    pub node_id: NodeId,
    /// Role of this replica
    pub role: ReplicaRole,
    /// Last known sequence number (for sync)
    pub last_sequence: u64,
    /// Whether this replica is in sync
    pub in_sync: bool,
}

/// Replication metadata for a partition
#[derive(Debug, Clone)]
pub struct PartitionReplication {
    /// Partition ID
    pub partition: u32,
    /// All replicas for this partition
    pub replicas: Vec<PartitionReplica>,
    /// Current leader node ID
    pub leader: Option<NodeId>,
    /// Replication factor (desired number of replicas)
    pub replication_factor: u32,
}

impl PartitionReplication {
    /// Create a new partition replication
    pub fn new(partition: u32, replication_factor: u32) -> Self {
        Self {
            partition,
            replicas: Vec::new(),
            leader: None,
            replication_factor,
        }
    }

    /// Get the leader replica
    pub fn get_leader(&self) -> Option<&PartitionReplica> {
        self.leader
            .and_then(|leader_id| self.replicas.iter().find(|r| r.node_id == leader_id))
    }

    /// Get all follower replicas
    pub fn get_followers(&self) -> Vec<&PartitionReplica> {
        self.replicas
            .iter()
            .filter(|r| r.role == ReplicaRole::Follower)
            .collect()
    }

    /// Get in-sync replicas
    pub fn get_in_sync_replicas(&self) -> Vec<&PartitionReplica> {
        self.replicas.iter().filter(|r| r.in_sync).collect()
    }

    /// Check if we have enough replicas
    pub fn has_sufficient_replicas(&self) -> bool {
        self.replicas.len() >= self.replication_factor as usize
    }

    /// Add a replica
    pub fn add_replica(&mut self, node_id: NodeId, role: ReplicaRole) {
        // Check if replica already exists
        if self.replicas.iter().any(|r| r.node_id == node_id) {
            return;
        }

        let replica = PartitionReplica {
            node_id,
            role,
            last_sequence: 0,
            in_sync: false,
        };

        self.replicas.push(replica);

        if role == ReplicaRole::Leader {
            self.leader = Some(node_id);
        }
    }

    /// Remove a replica
    pub fn remove_replica(&mut self, node_id: NodeId) {
        self.replicas.retain(|r| r.node_id != node_id);

        // If we removed the leader, elect a new one
        if self.leader == Some(node_id) {
            self.elect_new_leader();
        }
    }

    /// Elect a new leader from followers
    pub fn elect_new_leader(&mut self) {
        // Find the most up-to-date follower (highest sequence number)
        if let Some(new_leader) = self
            .replicas
            .iter()
            .filter(|r| r.role == ReplicaRole::Follower)
            .max_by_key(|r| r.last_sequence)
        {
            let new_leader_id = new_leader.node_id;

            // Update roles
            for replica in &mut self.replicas {
                if replica.node_id == new_leader_id {
                    replica.role = ReplicaRole::Leader;
                    replica.in_sync = true;
                }
            }

            self.leader = Some(new_leader_id);
            info!(
                "Elected new leader for partition {}: {:?}",
                self.partition, new_leader_id
            );
        } else {
            warn!(
                "No followers available to elect as leader for partition {}",
                self.partition
            );
            self.leader = None;
        }
    }

    /// Update replica sync status
    pub fn update_replica_sync(&mut self, node_id: NodeId, sequence: u64, in_sync: bool) {
        if let Some(replica) = self.replicas.iter_mut().find(|r| r.node_id == node_id) {
            replica.last_sequence = sequence;
            replica.in_sync = in_sync;
        }
    }
}

/// Replication manager for coordinating partition replication
pub struct ReplicationManager {
    /// Replication metadata for each partition
    partitions: Arc<RwLock<HashMap<u32, PartitionReplication>>>,
    /// Replication factor (default for new partitions)
    default_replication_factor: u32,
    /// Cluster membership (for failure detection)
    membership: Option<Arc<crate::distributed::membership::ClusterMembership>>,
    /// RPC client for promoting replicas (optional)
    rpc_client: Option<Arc<crate::network::RpcClient>>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(default_replication_factor: u32) -> Self {
        Self {
            partitions: Arc::new(RwLock::new(HashMap::new())),
            default_replication_factor,
            membership: None,
            rpc_client: None,
        }
    }

    /// Create with cluster membership for failure detection
    pub fn with_membership(
        default_replication_factor: u32,
        membership: Arc<crate::distributed::membership::ClusterMembership>,
    ) -> Self {
        Self {
            partitions: Arc::new(RwLock::new(HashMap::new())),
            default_replication_factor,
            membership: Some(membership),
            rpc_client: None,
        }
    }

    /// Set RPC client for replica promotion
    pub fn set_rpc_client(&mut self, rpc_client: Arc<crate::network::RpcClient>) {
        self.rpc_client = Some(rpc_client);
    }

    /// Handle node failure - automatically promote replicas if needed
    pub async fn handle_node_failure(&self, failed_node: NodeId) -> Result<Vec<(u32, NodeId)>> {
        let mut promoted = Vec::new();
        let partitions = self.partitions.read().await;
        let partitions_to_check: Vec<u32> = partitions.keys().copied().collect();
        drop(partitions);

        for partition in partitions_to_check {
            let mut partitions = self.partitions.write().await;
            if let Some(replication) = partitions.get_mut(&partition) {
                // Check if failed node is the leader
                if replication.leader == Some(failed_node) {
                    // Remove failed leader
                    replication.remove_replica(failed_node);

                    // Elect new leader
                    replication.elect_new_leader();

                    if let Some(new_leader) = replication.leader {
                        promoted.push((partition, new_leader));

                        // Notify new leader via RPC if available
                        if let Some(rpc_client) = &self.rpc_client {
                            if let Some(leader_addr) = self
                                .membership
                                .as_ref()
                                .and_then(|m| m.get_node_address(new_leader))
                            {
                                if let Err(e) = rpc_client
                                    .promote_replica(leader_addr, partition, new_leader)
                                    .await
                                {
                                    warn!(
                                        "Failed to notify new leader {} for partition {}: {}",
                                        new_leader, partition, e
                                    );
                                }
                            }
                        }

                        info!(
                            "Promoted replica {} to leader for partition {} after failure of {}",
                            new_leader, partition, failed_node
                        );
                    }
                } else {
                    // Failed node is a follower - just remove it
                    replication.remove_replica(failed_node);
                    info!(
                        "Removed failed follower {} from partition {}",
                        failed_node, partition
                    );
                }
            }
        }

        Ok(promoted)
    }

    /// Initialize replication for a partition
    pub async fn initialize_partition(
        &self,
        partition: u32,
        leader: NodeId,
        followers: Vec<NodeId>,
        replication_factor: Option<u32>,
    ) -> Result<()> {
        let rf = replication_factor.unwrap_or(self.default_replication_factor);
        let followers_clone = followers.clone();
        let mut replication = PartitionReplication::new(partition, rf);

        // Add leader
        replication.add_replica(leader, ReplicaRole::Leader);

        // Add followers
        for follower in followers {
            replication.add_replica(follower, ReplicaRole::Follower);
        }

        let mut partitions = self.partitions.write().await;
        partitions.insert(partition, replication);

        info!(
            "Initialized replication for partition {}: leader={:?}, followers={:?}, rf={}",
            partition, leader, followers_clone, rf
        );

        Ok(())
    }

    /// Get replication info for a partition
    pub async fn get_partition_replication(&self, partition: u32) -> Option<PartitionReplication> {
        let partitions = self.partitions.read().await;
        partitions.get(&partition).cloned()
    }

    /// Get the leader for a partition
    pub async fn get_leader(&self, partition: u32) -> Option<NodeId> {
        let partitions = self.partitions.read().await;
        partitions.get(&partition).and_then(|r| r.leader)
    }

    /// Get all replicas for a partition
    pub async fn get_replicas(&self, partition: u32) -> Vec<NodeId> {
        let partitions = self.partitions.read().await;
        partitions
            .get(&partition)
            .map(|r| r.replicas.iter().map(|replica| replica.node_id).collect())
            .unwrap_or_default()
    }

    /// Get followers for a partition (for read replicas)
    pub async fn get_followers(&self, partition: u32) -> Vec<NodeId> {
        let partitions = self.partitions.read().await;
        partitions
            .get(&partition)
            .map(|r| {
                r.replicas
                    .iter()
                    .filter(|replica| replica.role == ReplicaRole::Follower && replica.in_sync)
                    .map(|replica| replica.node_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a node is the leader for a partition
    pub async fn is_leader(&self, partition: u32, node_id: NodeId) -> bool {
        let partitions = self.partitions.read().await;
        partitions
            .get(&partition)
            .map(|r| r.leader == Some(node_id))
            .unwrap_or(false)
    }

    /// Handle leader failure and elect new leader
    pub async fn handle_leader_failure(
        &self,
        partition: u32,
        failed_leader: NodeId,
    ) -> Result<Option<NodeId>> {
        let mut partitions = self.partitions.write().await;

        if let Some(replication) = partitions.get_mut(&partition) {
            if replication.leader == Some(failed_leader) {
                replication.remove_replica(failed_leader);
                replication.elect_new_leader();
                return Ok(replication.leader);
            }
        }

        Ok(None)
    }

    /// Update replica sync status
    pub async fn update_replica_sync(
        &self,
        partition: u32,
        node_id: NodeId,
        sequence: u64,
        in_sync: bool,
    ) -> Result<()> {
        let mut partitions = self.partitions.write().await;

        if let Some(replication) = partitions.get_mut(&partition) {
            replication.update_replica_sync(node_id, sequence, in_sync);
        }

        Ok(())
    }

    /// Add a new replica to a partition
    pub async fn add_replica(&self, partition: u32, node_id: NodeId) -> Result<()> {
        let mut partitions = self.partitions.write().await;

        if let Some(replication) = partitions.get_mut(&partition) {
            // Only add if we don't have enough replicas
            if !replication.has_sufficient_replicas() {
                replication.add_replica(node_id, ReplicaRole::Follower);
                info!("Added replica for partition {}: {:?}", partition, node_id);
            }
        } else {
            // Create new replication if it doesn't exist
            let mut replication =
                PartitionReplication::new(partition, self.default_replication_factor);
            replication.add_replica(node_id, ReplicaRole::Leader);
            partitions.insert(partition, replication);
        }

        Ok(())
    }

    /// Remove a replica from a partition
    pub async fn remove_replica(&self, partition: u32, node_id: NodeId) -> Result<()> {
        let mut partitions = self.partitions.write().await;

        if let Some(replication) = partitions.get_mut(&partition) {
            replication.remove_replica(node_id);
            info!("Removed replica for partition {}: {:?}", partition, node_id);
        }

        Ok(())
    }

    /// Get all partitions for a node (as leader or follower)
    pub async fn get_partitions_for_node(&self, node_id: NodeId) -> Vec<u32> {
        let partitions = self.partitions.read().await;
        partitions
            .iter()
            .filter(|(_, replication)| replication.replicas.iter().any(|r| r.node_id == node_id))
            .map(|(partition, _)| *partition)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_manager_initialize() {
        let manager = ReplicationManager::new(3);
        let node1 = NodeId::from(1);
        let node2 = NodeId::from(2);
        let node3 = NodeId::from(3);

        manager
            .initialize_partition(0, node1, vec![node2, node3], None)
            .await
            .unwrap();

        let replication = manager.get_partition_replication(0).await.unwrap();
        assert_eq!(replication.leader, Some(node1));
        assert_eq!(replication.replicas.len(), 3);
        assert!(manager.is_leader(0, node1).await);
    }

    #[tokio::test]
    async fn test_leader_election() {
        let manager = ReplicationManager::new(3);
        let node1 = NodeId::from(1);
        let node2 = NodeId::from(2);
        let node3 = NodeId::from(3);

        manager
            .initialize_partition(0, node1, vec![node2, node3], None)
            .await
            .unwrap();

        // Simulate leader failure
        manager.handle_leader_failure(0, node1).await.unwrap();

        let replication = manager.get_partition_replication(0).await.unwrap();
        assert_ne!(replication.leader, Some(node1));
        assert!(replication.leader.is_some());
    }

    #[tokio::test]
    async fn test_replica_sync() {
        let manager = ReplicationManager::new(3);
        let node1 = NodeId::from(1);
        let node2 = NodeId::from(2);
        let node3 = NodeId::from(3);

        manager
            .initialize_partition(0, node1, vec![node2, node3], None)
            .await
            .unwrap();

        manager
            .update_replica_sync(0, node2, 100, true)
            .await
            .unwrap();

        let replication = manager.get_partition_replication(0).await.unwrap();
        let replica = replication
            .replicas
            .iter()
            .find(|r| r.node_id == node2)
            .unwrap();
        assert_eq!(replica.last_sequence, 100);
        assert!(replica.in_sync);
    }
}
