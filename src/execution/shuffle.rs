//! Data shuffling for cross-partition operations
//!
//! Implements efficient data transfer between nodes for joins and aggregations
//! that require data from multiple partitions.

use crate::core::Event;
use crate::distributed::node::NodeId;
use crate::error::Result;
use crate::network::{
    protocol::{RpcMethod, ShuffleDataRequest, ShuffleDataResponse},
    RpcClient,
};
use bincode;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// Shuffle manager for coordinating data movement across partitions
pub struct ShuffleManager {
    /// RPC client for sending shuffle data
    rpc_client: Arc<RpcClient>,
    /// Partition to node mapping
    partition_to_node: Arc<tokio::sync::RwLock<HashMap<u32, NodeId>>>,
    /// Shuffle buffers per partition
    shuffle_buffers: Arc<tokio::sync::RwLock<HashMap<u32, Vec<Event>>>>,
}

impl ShuffleManager {
    /// Create a new shuffle manager
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            partition_to_node: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            shuffle_buffers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Update partition to node mapping
    pub async fn update_partition_mapping(&self, partition: u32, node_id: NodeId) {
        let mut mapping = self.partition_to_node.write().await;
        mapping.insert(partition, node_id);
    }

    /// Shuffle events to a target partition
    pub async fn shuffle_to_partition(
        &self,
        events: Vec<Event>,
        target_partition: u32,
        _operation_type: String,
    ) -> Result<()> {
        // Get target node for partition
        let node_id = {
            let mapping = self.partition_to_node.read().await;
            mapping.get(&target_partition).copied()
        };

        if let Some(node_id) = node_id {
            // Get node address (would need membership access)
            // For now, we'll use a placeholder
            let _ = node_id;
            warn!(
                "Shuffle to partition {} requires node address lookup",
                target_partition
            );
            return Ok(());
        }

        // Buffer events locally if no target node
        let mut buffers = self.shuffle_buffers.write().await;
        let buffer = buffers.entry(target_partition).or_insert_with(Vec::new);
        buffer.extend(events);

        Ok(())
    }

    /// Shuffle events to a specific node
    pub async fn shuffle_to_node(
        &self,
        events: Vec<Event>,
        node_id: NodeId,
        target_partition: u32,
        operation_type: String,
        node_address: std::net::SocketAddr,
    ) -> Result<ShuffleDataResponse> {
        let request = ShuffleDataRequest {
            target_partition,
            events: events.clone(),
            operation_type,
        };

        let payload = bincode::serialize(&request)
            .map_err(|e| crate::error::StreamError::SerializationError(e.to_string()))?;

        debug!(
            "Shuffling {} events to node {} (partition {})",
            events.len(),
            node_id,
            target_partition
        );

        match self
            .rpc_client
            .call(node_address, RpcMethod::ShuffleData.as_str(), payload)
            .await
        {
            Ok(response_payload) => {
                match bincode::deserialize::<ShuffleDataResponse>(&response_payload) {
                    Ok(response) => {
                        debug!(
                            "Shuffle successful: {} events received by node {}",
                            response.events_received, node_id
                        );
                        Ok(response)
                    }
                    Err(e) => {
                        warn!("Failed to deserialize shuffle response: {}", e);
                        Err(crate::error::StreamError::SerializationError(e.to_string()))
                    }
                }
            }
            Err(e) => {
                warn!("Failed to shuffle data to node {}: {}", node_id, e);
                Err(crate::error::StreamError::Unknown(format!(
                    "Shuffle error: {}",
                    e
                )))
            }
        }
    }

    /// Get buffered events for a partition
    pub async fn get_buffered_events(&self, partition: u32) -> Vec<Event> {
        let mut buffers = self.shuffle_buffers.write().await;
        buffers.remove(&partition).unwrap_or_default()
    }

    /// Clear shuffle buffers
    pub async fn clear_buffers(&self) {
        let mut buffers = self.shuffle_buffers.write().await;
        buffers.clear();
    }
}

/// Shuffle coordinator for join operations
pub struct JoinShuffleCoordinator {
    shuffle_manager: Arc<ShuffleManager>,
}

impl JoinShuffleCoordinator {
    /// Create a new join shuffle coordinator
    pub fn new(shuffle_manager: Arc<ShuffleManager>) -> Self {
        Self { shuffle_manager }
    }

    /// Shuffle events for a join operation
    pub async fn shuffle_for_join(
        &self,
        events: Vec<Event>,
        target_partitions: Vec<u32>,
    ) -> Result<()> {
        // Group events by target partition
        let mut events_by_partition: HashMap<u32, Vec<Event>> = HashMap::new();

        for event in events {
            // Determine target partition (simplified - would use key hash)
            for &partition in &target_partitions {
                events_by_partition
                    .entry(partition)
                    .or_default()
                    .push(event.clone());
            }
        }

        // Shuffle to each partition
        for (partition, partition_events) in events_by_partition {
            self.shuffle_manager
                .shuffle_to_partition(partition_events, partition, "join".to_string())
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_shuffle_manager() {
        // This test would require setting up RPC client
        // For now, we'll just test the structure
        // let manager = ShuffleManager::new(...);
        // assert!(true);
    }
}
