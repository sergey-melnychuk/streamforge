//! Distributed execution engine
//!
//! Executes stream processing operators across cluster nodes

use crate::core::{Event, EventKey};
use crate::distributed::{
    node::NodeId,
    partition_assignment::{ConsistentHashAssigner, PartitionAssigner},
    replication::ReplicationManager,
    ClusterMembership,
};
use crate::error::Result;
use crate::network::{
    protocol::{ExecuteOperatorRequest, ExecuteOperatorResponse, RpcMethod},
    RpcClient,
};
use crate::operators::StreamOperator;
use crate::tracing::instrumentation::{current_context, TraceInstrumentation};
use bincode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Distributed execution context
pub struct DistributedContext {
    /// Local node ID
    pub local_node_id: NodeId,
    /// Cluster membership
    pub membership: Arc<ClusterMembership>,
    /// Partition assigner
    pub partition_assigner: Arc<dyn PartitionAssigner>,
    /// Number of partitions
    pub num_partitions: u32,
    /// Current partition assignment
    partition_assignment: Arc<tokio::sync::RwLock<HashMap<NodeId, Vec<u32>>>>,
    /// Replication manager (optional)
    replication_manager: Option<Arc<ReplicationManager>>,
}

impl DistributedContext {
    /// Create a new distributed context
    pub fn new(
        local_node_id: NodeId,
        membership: Arc<ClusterMembership>,
        num_partitions: u32,
    ) -> Self {
        let partition_assigner: Arc<dyn PartitionAssigner> =
            Arc::new(ConsistentHashAssigner::default());

        Self {
            local_node_id,
            membership,
            partition_assigner,
            num_partitions,
            partition_assignment: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            replication_manager: None,
        }
    }

    /// Create with replication manager
    pub fn with_replication(
        local_node_id: NodeId,
        membership: Arc<ClusterMembership>,
        num_partitions: u32,
        replication_manager: Arc<ReplicationManager>,
    ) -> Self {
        let mut ctx = Self::new(local_node_id, membership, num_partitions);
        ctx.replication_manager = Some(replication_manager);
        ctx
    }

    /// Get replication manager
    pub fn replication_manager(&self) -> Option<Arc<ReplicationManager>> {
        self.replication_manager.clone()
    }

    /// Update partition assignment based on current cluster state
    pub async fn update_partition_assignment(&self) -> Result<()> {
        let nodes = self.membership.get_alive_nodes();
        let old_assignment = self.partition_assignment.read().await.clone();

        let new_assignment = if old_assignment.is_empty() {
            self.partition_assigner.assign(self.num_partitions, &nodes)
        } else {
            self.partition_assigner
                .rebalance(self.num_partitions, &old_assignment, &nodes)
        };

        *self.partition_assignment.write().await = new_assignment.clone();

        info!(
            "Updated partition assignment: {} partitions across {} nodes",
            self.num_partitions,
            nodes.len()
        );

        Ok(())
    }

    /// Get partitions assigned to the local node
    pub async fn get_local_partitions(&self) -> Vec<u32> {
        let assignment = self.partition_assignment.read().await;
        assignment
            .get(&self.local_node_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get the node responsible for a partition (leader for writes)
    pub async fn get_node_for_partition(&self, partition: u32) -> Option<NodeId> {
        // If replication is enabled, get the leader
        if let Some(rm) = &self.replication_manager {
            if let Some(leader) = rm.get_leader(partition).await {
                return Some(leader);
            }
        }

        // Fallback to partition assignment
        let assignment = self.partition_assignment.read().await;
        self.partition_assigner
            .get_node_for_partition(partition, &assignment)
    }

    /// Get a replica node for a partition (for reads)
    /// If read replicas are enabled, returns a follower; otherwise returns the leader
    pub async fn get_replica_for_partition(
        &self,
        partition: u32,
        prefer_read_replica: bool,
    ) -> Option<NodeId> {
        if prefer_read_replica {
            if let Some(rm) = &self.replication_manager {
                let followers = rm.get_followers(partition).await;
                if !followers.is_empty() {
                    // Simple round-robin selection (could be improved with load balancing)
                    // For now, just return first available follower
                    return Some(followers[0]);
                }
            }
        }

        // Fallback to leader
        self.get_node_for_partition(partition).await
    }

    /// Check if local node is leader for a partition
    pub async fn is_leader(&self, partition: u32) -> bool {
        if let Some(rm) = &self.replication_manager {
            return rm.is_leader(partition, self.local_node_id).await;
        }
        // Without replication, if we have the partition, we're the "leader"
        self.is_local_partition(partition).await
    }

    /// Check if a partition is assigned to the local node
    pub async fn is_local_partition(&self, partition: u32) -> bool {
        let local_partitions = self.get_local_partitions().await;
        local_partitions.contains(&partition)
    }

    /// Get partition for an event based on its key
    pub fn get_partition_for_event(&self, event: &Event) -> u32 {
        // Hash the event key to determine partition
        let key_hash = match &event.key {
            EventKey::None => 0,
            EventKey::String(s) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                s.as_ref().hash(&mut hasher);
                hasher.finish() as u32
            }
            EventKey::Int(i) => (*i as u64) as u32,
            EventKey::Bytes(b) => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                b.hash(&mut hasher);
                hasher.finish() as u32
            }
        };

        key_hash % self.num_partitions
    }
}

/// Distributed operator executor
pub struct DistributedExecutor {
    context: Arc<DistributedContext>,
    rpc_client: Option<Arc<RpcClient>>,
}

impl DistributedExecutor {
    /// Create a new distributed executor
    pub fn new(context: Arc<DistributedContext>) -> Self {
        Self {
            context,
            rpc_client: None,
        }
    }

    /// Create with RPC client for remote execution
    pub fn with_rpc(context: Arc<DistributedContext>, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            context,
            rpc_client: Some(rpc_client),
        }
    }

    /// Execute an operator on events, routing to appropriate nodes
    pub async fn execute_operator<O>(
        &self,
        mut operator: O,
        events: Vec<Event>,
    ) -> Result<Vec<Event>>
    where
        O: StreamOperator + Send + 'static,
    {
        // Create or get trace context
        let trace_ctx = current_context().unwrap_or_default();
        let (span, _child_ctx) =
            TraceInstrumentation::start_span("execute_operator", Some(&trace_ctx));
        let _guard = span.enter();

        span.record("operator_type", self.get_operator_type(&operator).as_str());
        span.record("event_count", events.len() as u64);

        let mut local_events = Vec::new();
        let mut remote_events: HashMap<NodeId, Vec<Event>> = HashMap::new();

        // Partition events by destination node (route to leaders for writes)
        for event in events {
            let partition = self.context.get_partition_for_event(&event);

            // Check if we're the leader for this partition
            if self.context.is_leader(partition).await {
                // Process locally (we're the leader)
                if let Some(result) = operator.process(event.clone())? {
                    local_events.push(result);
                }

                // Replicate to followers if replication is enabled
                if let Some(rpc_client) = &self.rpc_client {
                    if let Some(rm) = self.context.replication_manager() {
                        let followers = rm.get_followers(partition).await;
                        for follower_id in followers {
                            if let Some(follower_addr) =
                                self.context.membership.get_node_address(follower_id)
                            {
                                // Replicate the event to follower
                                // In a full implementation, we'd track sequence numbers
                                let sequence = 0; // TODO: Track sequence numbers
                                if let Err(e) = rpc_client
                                    .replicate_data(
                                        follower_addr,
                                        partition,
                                        sequence,
                                        vec![event.clone()],
                                    )
                                    .await
                                {
                                    warn!("Failed to replicate to follower {}: {}", follower_id, e);
                                }
                            }
                        }
                    }
                }
            } else {
                // Route to leader (remote node)
                if let Some(node_id) = self.context.get_node_for_partition(partition).await {
                    remote_events.entry(node_id).or_default().push(event);
                } else {
                    warn!("No node found for partition {}, dropping event", partition);
                }
            }
        }

        // Send remote_events to their destination nodes via RPC
        let remote_count: usize = remote_events.values().map(|v| v.len()).sum();
        let mut remote_results = Vec::new();

        if let Some(rpc_client) = &self.rpc_client {
            for (node_id, events) in remote_events {
                if let Some(node_addr) = self.context.membership.get_node_address(node_id) {
                    // For now, we'll use a simple operator type identifier
                    // In a full implementation, we'd serialize the operator configuration
                    let operator_type = self.get_operator_type(&operator);

                    let request = ExecuteOperatorRequest {
                        operator_type,
                        operator_config: Vec::new(), // TODO: Serialize operator config
                        events: events.clone(),
                    };

                    let payload = bincode::serialize(&request).map_err(|e| {
                        crate::error::StreamError::SerializationError(e.to_string())
                    })?;

                    match rpc_client
                        .call(node_addr, RpcMethod::ExecuteOperator.as_str(), payload)
                        .await
                    {
                        Ok(response_payload) => {
                            match bincode::deserialize::<ExecuteOperatorResponse>(&response_payload)
                            {
                                Ok(response) => {
                                    remote_results.extend(response.events);
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to deserialize operator response from {}: {}",
                                        node_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to execute operator on {}: {}", node_id, e);
                        }
                    }
                } else {
                    warn!(
                        "No address found for node {}, dropping {} events",
                        node_id,
                        events.len()
                    );
                }
            }

            local_events.extend(remote_results);
        } else {
            warn!(
                "No RPC client available, dropping {} remote events",
                remote_count
            );
        }

        debug!(
            "Processed {} events locally, {} events from remote nodes",
            local_events.len().saturating_sub(remote_count),
            remote_count
        );

        span.record("result_count", local_events.len() as u64);
        Ok(local_events)
    }

    /// Get operator type identifier (simplified - in production would serialize config)
    fn get_operator_type<O>(&self, _operator: &O) -> String {
        // This is a placeholder - in a real implementation, we'd inspect the operator
        // or require operators to implement a trait that provides their type/config
        "filter".to_string() // Default to filter for now
    }

    /// Execute operator on a stream of events
    pub async fn execute_stream<O, S>(
        &self,
        operator: O,
        stream: S,
    ) -> Result<mpsc::Receiver<Event>>
    where
        O: StreamOperator + Send + 'static,
        S: futures::Stream<Item = Event> + Send + Unpin + 'static,
    {
        let (tx, rx) = mpsc::channel(1000);

        let context = self.context.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut op = operator;
            let mut stream = stream;
            let mut batch = Vec::new();
            const BATCH_SIZE: usize = 100;

            while let Some(event) = stream.next().await {
                batch.push(event);

                if batch.len() >= BATCH_SIZE {
                    let partition = context.get_partition_for_event(&batch[0]);

                    if context.is_local_partition(partition).await {
                        // Process batch locally
                        let batch_to_process = std::mem::take(&mut batch);
                        if let Ok(results) = op.process_batch(batch_to_process) {
                            for result in results {
                                if tx.send(result).await.is_err() {
                                    return;
                                }
                            }
                        }
                    } else {
                        batch.clear();
                    }
                }
            }

            // Process remaining events
            if !batch.is_empty() {
                let partition = context.get_partition_for_event(&batch[0]);

                if context.is_local_partition(partition).await {
                    if let Ok(results) = op.process_batch(batch) {
                        for result in results {
                            let _ = tx.send(result).await;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::node::NodeMetadata;

    #[tokio::test]
    async fn test_distributed_context() {
        let node_id = NodeId::generate();
        let membership = Arc::new(ClusterMembership::new(node_id, 30));

        // Add local node to membership
        let local_node = NodeMetadata::new(node_id, "127.0.0.1:8000".parse().unwrap());
        membership.add_node(local_node);

        let context = DistributedContext::new(node_id, membership.clone(), 10);

        // Add some other nodes
        let node1 = NodeMetadata::new(NodeId::new(1), "127.0.0.1:8001".parse().unwrap());
        let node2 = NodeMetadata::new(NodeId::new(2), "127.0.0.1:8002".parse().unwrap());

        membership.add_node(node1);
        membership.add_node(node2);

        context.update_partition_assignment().await.unwrap();

        let local_partitions = context.get_local_partitions().await;
        assert!(!local_partitions.is_empty());
    }

    #[tokio::test]
    async fn test_get_partition_for_event() {
        let node_id = NodeId::generate();
        let membership = Arc::new(ClusterMembership::new(node_id, 30));
        let context = DistributedContext::new(node_id, membership, 10);

        let event = Event::new(
            EventKey::from_str("test-key"),
            crate::core::EventValue::String("value".into()),
            1234567890,
        );

        let partition = context.get_partition_for_event(&event);
        assert!(partition < 10);
    }
}
