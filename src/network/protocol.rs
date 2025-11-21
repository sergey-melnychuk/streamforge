//! Wire protocol for cluster communication
//!
//! Defines message types and RPC protocol for distributed coordination

use crate::distributed::discovery::GossipMessage;
use crate::distributed::node::NodeId;
use crate::distributed::node::NodeMetadata;
use crate::tracing::context::TraceContext;
use serde::{Deserialize, Serialize};

/// Message type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Gossip protocol messages
    Gossip,
    /// RPC request
    RpcRequest,
    /// RPC response
    RpcResponse,
    /// Heartbeat ping
    Ping,
    /// Heartbeat pong
    Pong,
}

/// RPC request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    /// Request ID for correlation
    pub request_id: u64,
    /// RPC method name
    pub method: String,
    /// Serialized method arguments
    pub payload: Vec<u8>,
    /// Trace context for distributed tracing (optional)
    pub trace_context: Option<TraceContext>,
}

/// RPC response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    /// Request ID this response corresponds to
    pub request_id: u64,
    /// Whether the request was successful
    pub success: bool,
    /// Response payload (or error message)
    pub payload: Vec<u8>,
    /// Trace context (echoed from request)
    pub trace_context: Option<TraceContext>,
}

/// Network message wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Gossip protocol message
    Gossip {
        /// Source node ID
        from: NodeId,
        /// Gossip message content
        content: GossipMessage,
    },
    /// RPC request
    RpcRequest(RpcRequest),
    /// RPC response
    RpcResponse(RpcResponse),
    /// Ping for connection health check
    Ping {
        /// Source node ID
        from: NodeId,
        /// Timestamp
        timestamp: u64,
    },
    /// Pong response to ping
    Pong {
        /// Source node ID
        from: NodeId,
        /// Original ping timestamp
        timestamp: u64,
    },
}

impl Message {
    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Gossip { .. } => MessageType::Gossip,
            Message::RpcRequest(_) => MessageType::RpcRequest,
            Message::RpcResponse(_) => MessageType::RpcResponse,
            Message::Ping { .. } => MessageType::Ping,
            Message::Pong { .. } => MessageType::Pong,
        }
    }

    /// Create a gossip message
    pub fn gossip(from: NodeId, content: GossipMessage) -> Self {
        Message::Gossip { from, content }
    }

    /// Create a ping message
    pub fn ping(from: NodeId, timestamp: u64) -> Self {
        Message::Ping { from, timestamp }
    }

    /// Create a pong message
    pub fn pong(from: NodeId, timestamp: u64) -> Self {
        Message::Pong { from, timestamp }
    }
}

/// RPC methods for cluster coordination
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcMethod {
    /// Join the cluster
    Join,
    /// Leave the cluster
    Leave,
    /// Get cluster membership
    GetMembership,
    /// Get partition assignment
    GetPartitions,
    /// Update partition assignment
    UpdatePartitions,
    /// Raft: Request vote
    RaftRequestVote,
    /// Raft: Append entries
    RaftAppendEntries,
    /// Execute operator on remote node
    ExecuteOperator,
    /// Shuffle data to remote node
    ShuffleData,
    /// Replicate data to follower
    ReplicateData,
    /// Sync replica state
    SyncReplica,
    /// Promote replica to leader
    PromoteReplica,
    /// Execute SQL query aggregation on remote node
    ExecuteQueryAggregation,
    /// Collect query results from remote node
    CollectQueryResults,
}

impl RpcMethod {
    /// Get method name as string
    pub fn as_str(&self) -> &'static str {
        match self {
            RpcMethod::Join => "join",
            RpcMethod::Leave => "leave",
            RpcMethod::GetMembership => "get_membership",
            RpcMethod::GetPartitions => "get_partitions",
            RpcMethod::UpdatePartitions => "update_partitions",
            RpcMethod::RaftRequestVote => "raft_request_vote",
            RpcMethod::RaftAppendEntries => "raft_append_entries",
            RpcMethod::ExecuteOperator => "execute_operator",
            RpcMethod::ShuffleData => "shuffle_data",
            RpcMethod::ReplicateData => "replicate_data",
            RpcMethod::SyncReplica => "sync_replica",
            RpcMethod::PromoteReplica => "promote_replica",
            RpcMethod::ExecuteQueryAggregation => "execute_query_aggregation",
            RpcMethod::CollectQueryResults => "collect_query_results",
        }
    }

    /// Parse method name
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "join" => Some(RpcMethod::Join),
            "leave" => Some(RpcMethod::Leave),
            "get_membership" => Some(RpcMethod::GetMembership),
            "get_partitions" => Some(RpcMethod::GetPartitions),
            "update_partitions" => Some(RpcMethod::UpdatePartitions),
            "raft_request_vote" => Some(RpcMethod::RaftRequestVote),
            "raft_append_entries" => Some(RpcMethod::RaftAppendEntries),
            "execute_operator" => Some(RpcMethod::ExecuteOperator),
            "shuffle_data" => Some(RpcMethod::ShuffleData),
            "replicate_data" => Some(RpcMethod::ReplicateData),
            "sync_replica" => Some(RpcMethod::SyncReplica),
            "promote_replica" => Some(RpcMethod::PromoteReplica),
            "execute_query_aggregation" => Some(RpcMethod::ExecuteQueryAggregation),
            "collect_query_results" => Some(RpcMethod::CollectQueryResults),
            _ => None,
        }
    }
}

/// Join request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node: NodeMetadata,
}

/// Join response payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub success: bool,
    pub cluster_nodes: Vec<NodeMetadata>,
}

/// Get membership response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipResponse {
    pub nodes: Vec<NodeMetadata>,
}

/// Get partitions response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionsResponse {
    pub partitions: Vec<u32>,
}

/// Update partitions request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePartitionsRequest {
    pub node_id: NodeId,
    pub partitions: Vec<u32>,
}

/// Execute operator request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteOperatorRequest {
    /// Operator type identifier (e.g., "filter", "map")
    pub operator_type: String,
    /// Serialized operator configuration
    pub operator_config: Vec<u8>,
    /// Events to process
    pub events: Vec<crate::core::Event>,
}

/// Execute operator response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteOperatorResponse {
    /// Processed events
    pub events: Vec<crate::core::Event>,
}

/// Shuffle data request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleDataRequest {
    /// Target partition for shuffling
    pub target_partition: u32,
    /// Events to shuffle
    pub events: Vec<crate::core::Event>,
    /// Shuffle operation type (e.g., "join", "aggregate")
    pub operation_type: String,
}

/// Shuffle data response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleDataResponse {
    /// Acknowledgment that data was received
    pub success: bool,
    /// Number of events received
    pub events_received: usize,
}

/// Request to replicate data to a follower
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateDataRequest {
    /// Partition ID
    pub partition: u32,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Events to replicate
    pub events: Vec<crate::core::Event>,
}

/// Response from replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateDataResponse {
    pub success: bool,
    pub sequence: u64,
    pub message: String,
}

/// Request to sync replica state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReplicaRequest {
    /// Partition ID
    pub partition: u32,
    /// Current sequence number
    pub sequence: u64,
}

/// Response from sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReplicaResponse {
    pub success: bool,
    pub in_sync: bool,
    pub last_sequence: u64,
}

/// Request to promote replica to leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteReplicaRequest {
    /// Partition ID
    pub partition: u32,
    /// New leader node ID
    pub new_leader: crate::distributed::node::NodeId,
}

/// Response from promotion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteReplicaResponse {
    pub success: bool,
    pub message: String,
}

/// Request to execute query aggregation on remote node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteQueryAggregationRequest {
    /// Query ID for tracking
    pub query_id: String,
    /// Serialized query AST
    pub query: Vec<u8>,
    /// Events to aggregate
    pub events: Vec<crate::core::Event>,
    /// Partition ID (for tracking)
    pub partition: u32,
}

/// Response from query aggregation execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteQueryAggregationResponse {
    /// Aggregated results
    pub results: Vec<crate::core::Event>,
    /// Partition ID
    pub partition: u32,
}

/// Request to collect query results from remote node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectQueryResultsRequest {
    /// Query ID
    pub query_id: String,
}

/// Response with query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectQueryResultsResponse {
    /// Query results
    pub results: Vec<crate::core::Event>,
    /// Node ID that produced these results
    pub node_id: crate::distributed::node::NodeId,
}
