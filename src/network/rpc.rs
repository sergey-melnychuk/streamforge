//! RPC client and server for cluster communication
//!
//! Provides request/response RPC semantics over the transport layer

use crate::distributed::membership::ClusterMembership;
use crate::distributed::node::NodeMetadata;
use crate::network::protocol::{
    JoinRequest, JoinResponse, MembershipResponse, Message, RpcMethod, RpcRequest, RpcResponse,
};
use crate::network::transport::{Transport, TransportConnection, TransportError};
use crate::tracing::context::TraceContext;
use crate::tracing::instrumentation::{current_context, set_context};
use bincode;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, span, warn, Level};

/// Error type for RPC operations
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Method not found: {0}")]
    MethodNotFound(String),
    #[error("Request timeout")]
    Timeout,
}

/// Result type for RPC operations
pub type RpcResult<T> = Result<T, RpcError>;

/// Handler function for RPC methods
pub type RpcHandler = Arc<
    dyn Fn(
            String,
            Vec<u8>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = RpcResult<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

/// RPC client for making requests to remote nodes
pub struct RpcClient {
    transport: Transport,
    connections: Arc<Mutex<HashMap<SocketAddr, TransportConnection>>>,
    request_id_counter: Arc<Mutex<u64>>,
}

impl RpcClient {
    /// Create a new RPC client
    pub fn new(transport: Transport) -> Self {
        Self {
            transport,
            connections: Arc::new(Mutex::new(HashMap::new())),
            request_id_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Get or create a connection to a remote address
    async fn get_connection(&self, addr: SocketAddr) -> RpcResult<TransportConnection> {
        let mut connections = self.connections.lock().await;

        if let Some(conn) = connections.get(&addr) {
            return Ok(conn.clone());
        }

        // Create new connection
        let conn = self.transport.connect(addr).await?;
        connections.insert(addr, conn.clone());
        Ok(conn)
    }

    /// Generate a new request ID
    async fn next_request_id(&self) -> u64 {
        let mut counter = self.request_id_counter.lock().await;
        *counter += 1;
        *counter
    }

    /// Make an RPC call
    pub async fn call(
        &self,
        addr: SocketAddr,
        method: &str,
        payload: Vec<u8>,
    ) -> RpcResult<Vec<u8>> {
        // Get or create trace context
        let trace_ctx = current_context().unwrap_or_default();
        let child_ctx = trace_ctx.child();

        let span = span!(
            Level::INFO,
            "rpc.call",
            method = method,
            addr = %addr,
            trace_id = %child_ctx.trace_id,
            span_id = %child_ctx.span_id,
        );
        let _guard = span.enter();

        let request_id = self.next_request_id().await;
        let conn = self.get_connection(addr).await?;

        // Create request with trace context
        let request = RpcRequest {
            request_id,
            method: method.to_string(),
            payload,
            trace_context: Some(child_ctx),
        };

        // Send request
        let msg = Message::RpcRequest(request.clone());
        conn.send(msg).await?;

        // Wait for response
        loop {
            let response_msg = conn.recv().await?;

            match response_msg {
                Some(Message::RpcResponse(resp)) => {
                    if resp.request_id == request_id {
                        // Restore trace context from response if present
                        if let Some(ctx) = resp.trace_context {
                            set_context(ctx);
                        }

                        if resp.success {
                            return Ok(resp.payload);
                        } else {
                            let error_msg = String::from_utf8_lossy(&resp.payload);
                            span.record("error", true);
                            span.record("error.message", error_msg.as_ref());
                            return Err(RpcError::Rpc(error_msg.to_string()));
                        }
                    }
                    // Different request ID, continue waiting
                }
                Some(_) => {
                    // Not a response, ignore
                }
                None => {
                    span.record("error", true);
                    span.record("error.message", "Connection closed");
                    return Err(RpcError::Transport(TransportError::ConnectionClosed));
                }
            }
        }
    }

    /// Join a cluster
    pub async fn join(&self, addr: SocketAddr, node: NodeMetadata) -> RpcResult<JoinResponse> {
        let request = JoinRequest { node };
        let payload =
            bincode::serialize(&request).map_err(|e| RpcError::Serialization(e.to_string()))?;

        let response_payload = self.call(addr, RpcMethod::Join.as_str(), payload).await?;

        let response: JoinResponse = bincode::deserialize(&response_payload)
            .map_err(|e| RpcError::Deserialization(e.to_string()))?;

        Ok(response)
    }

    /// Get cluster membership
    pub async fn get_membership(&self, addr: SocketAddr) -> RpcResult<MembershipResponse> {
        let payload = Vec::new();
        let response_payload = self
            .call(addr, RpcMethod::GetMembership.as_str(), payload)
            .await?;

        let response: MembershipResponse = bincode::deserialize(&response_payload)
            .map_err(|e| RpcError::Deserialization(e.to_string()))?;

        Ok(response)
    }

    /// Replicate data to a follower node
    pub async fn replicate_data(
        &self,
        addr: SocketAddr,
        partition: u32,
        sequence: u64,
        events: Vec<crate::core::Event>,
    ) -> RpcResult<crate::network::protocol::ReplicateDataResponse> {
        use crate::network::protocol::ReplicateDataRequest;

        let request = ReplicateDataRequest {
            partition,
            sequence,
            events,
        };
        let payload =
            bincode::serialize(&request).map_err(|e| RpcError::Serialization(e.to_string()))?;

        let response_payload = self
            .call(addr, RpcMethod::ReplicateData.as_str(), payload)
            .await?;

        let response: crate::network::protocol::ReplicateDataResponse =
            bincode::deserialize(&response_payload)
                .map_err(|e| RpcError::Deserialization(e.to_string()))?;

        Ok(response)
    }

    /// Sync replica state
    pub async fn sync_replica(
        &self,
        addr: SocketAddr,
        partition: u32,
        sequence: u64,
    ) -> RpcResult<crate::network::protocol::SyncReplicaResponse> {
        use crate::network::protocol::SyncReplicaRequest;

        let request = SyncReplicaRequest {
            partition,
            sequence,
        };
        let payload =
            bincode::serialize(&request).map_err(|e| RpcError::Serialization(e.to_string()))?;

        let response_payload = self
            .call(addr, RpcMethod::SyncReplica.as_str(), payload)
            .await?;

        let response: crate::network::protocol::SyncReplicaResponse =
            bincode::deserialize(&response_payload)
                .map_err(|e| RpcError::Deserialization(e.to_string()))?;

        Ok(response)
    }

    /// Promote replica to leader
    pub async fn promote_replica(
        &self,
        addr: SocketAddr,
        partition: u32,
        new_leader: crate::distributed::node::NodeId,
    ) -> RpcResult<crate::network::protocol::PromoteReplicaResponse> {
        use crate::network::protocol::PromoteReplicaRequest;

        let request = PromoteReplicaRequest {
            partition,
            new_leader,
        };
        let payload =
            bincode::serialize(&request).map_err(|e| RpcError::Serialization(e.to_string()))?;

        let response_payload = self
            .call(addr, RpcMethod::PromoteReplica.as_str(), payload)
            .await?;

        let response: crate::network::protocol::PromoteReplicaResponse =
            bincode::deserialize(&response_payload)
                .map_err(|e| RpcError::Deserialization(e.to_string()))?;

        Ok(response)
    }
}

/// RPC server for handling requests from remote nodes
pub struct RpcServer {
    transport: Transport,
    handlers: Arc<Mutex<HashMap<String, RpcHandler>>>,
    membership: Option<Arc<ClusterMembership>>,
    raft: Option<Arc<crate::distributed::consensus::Raft>>,
}

impl RpcServer {
    /// Create a new RPC server
    pub fn new(transport: Transport) -> Self {
        Self {
            transport,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            membership: None,
            raft: None,
        }
    }

    /// Set the cluster membership (for built-in handlers)
    pub fn with_membership(mut self, membership: Arc<ClusterMembership>) -> Self {
        self.membership = Some(membership);
        self
    }

    /// Set the Raft instance (for Raft RPC handlers)
    pub fn with_raft(mut self, raft: Arc<crate::distributed::consensus::Raft>) -> Self {
        self.raft = Some(raft);
        self
    }

    /// Register a handler for an RPC method
    pub async fn register_handler(&self, method: &str, handler: RpcHandler) {
        let mut handlers = self.handlers.lock().await;
        handlers.insert(method.to_string(), handler);
    }

    /// Start the RPC server
    pub async fn start(&self) -> RpcResult<()> {
        let _listener = self
            .transport
            .listener()
            .ok_or(RpcError::Transport(TransportError::ConnectionClosed))?;

        info!("RPC server started on {}", self.transport.local_addr());

        loop {
            match self.transport.accept().await {
                Ok(conn) => {
                    let handlers = Arc::clone(&self.handlers);
                    let membership = self.membership.clone();
                    let raft = self.raft.clone();

                    tokio::spawn(async move {
                        // Create a minimal server instance for handling the connection
                        // We only need handlers, membership, and raft, not the full transport
                        if let Err(e) =
                            Self::handle_connection_static(handlers, membership, raft, conn).await
                        {
                            error!("Error handling connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
    }

    /// Static method to handle a connection (doesn't need full server instance)
    async fn handle_connection_static(
        handlers: Arc<Mutex<HashMap<String, RpcHandler>>>,
        membership: Option<Arc<ClusterMembership>>,
        raft: Option<Arc<crate::distributed::consensus::Raft>>,
        conn: TransportConnection,
    ) -> RpcResult<()> {
        debug!("Handling connection from {}", conn.addr());

        loop {
            let msg = conn.recv().await?;

            match msg {
                Some(Message::RpcRequest(request)) => {
                    if let Err(e) = Self::handle_request_static(
                        handlers.clone(),
                        membership.clone(),
                        raft.clone(),
                        request,
                        &conn,
                    )
                    .await
                    {
                        error!("Error handling request: {}", e);
                    }
                }
                Some(Message::Ping { from, timestamp }) => {
                    // Respond to ping
                    let pong = Message::pong(from, timestamp);
                    if let Err(e) = conn.send(pong).await {
                        error!("Error sending pong: {}", e);
                        break;
                    }
                }
                Some(_) => {
                    // Other message types, ignore for now
                }
                None => {
                    debug!("Connection closed: {}", conn.addr());
                    break;
                }
            }
        }

        Ok(())
    }

    /// Static method to handle a request
    async fn handle_request_static(
        handlers: Arc<Mutex<HashMap<String, RpcHandler>>>,
        membership: Option<Arc<ClusterMembership>>,
        raft: Option<Arc<crate::distributed::consensus::Raft>>,
        request: RpcRequest,
        conn: &TransportConnection,
    ) -> RpcResult<()> {
        // Extract and set trace context from request
        let trace_ctx = request.trace_context.clone();
        let span = if let Some(ref ctx) = trace_ctx {
            set_context(ctx.clone());
            let parent_span_id_str = ctx.parent_span_id.as_ref().map(|id| id.to_string());
            if let Some(ref parent_id) = parent_span_id_str {
                span!(
                    Level::INFO,
                    "rpc.handle",
                    method = %request.method,
                    trace_id = %ctx.trace_id,
                    span_id = %ctx.span_id,
                    parent_span_id = %parent_id,
                )
            } else {
                span!(
                    Level::INFO,
                    "rpc.handle",
                    method = %request.method,
                    trace_id = %ctx.trace_id,
                    span_id = %ctx.span_id,
                )
            }
        } else {
            // No trace context, create a new one
            let new_ctx = TraceContext::new();
            set_context(new_ctx.clone());
            span!(
                Level::INFO,
                "rpc.handle",
                method = %request.method,
                trace_id = %new_ctx.trace_id,
                span_id = %new_ctx.span_id,
            )
        };
        let _guard = span.enter();

        let handler = {
            let handlers_guard = handlers.lock().await;
            handlers_guard.get(&request.method).cloned()
        };

        let response = if let Some(handler) = handler {
            // Call custom handler
            handler(request.method.clone(), request.payload.clone()).await
        } else {
            // Try built-in handlers (membership and Raft)
            Self::handle_builtin_static(membership, raft, &request.method, &request.payload).await
        };

        let (success, payload) = match response {
            Ok(payload) => (true, payload),
            Err(e) => {
                let error_msg = e.to_string();
                span.record("error", true);
                span.record("error.message", error_msg.as_str());
                (false, error_msg.into_bytes())
            }
        };

        // Get current trace context for response (may have been updated)
        let response_trace_ctx = current_context();

        let response = RpcResponse {
            request_id: request.request_id,
            success,
            payload,
            trace_context: response_trace_ctx,
        };

        let msg = Message::RpcResponse(response);
        conn.send(msg).await?;

        Ok(())
    }

    /// Static method to handle built-in RPC methods
    async fn handle_builtin_static(
        membership: Option<Arc<ClusterMembership>>,
        raft: Option<Arc<crate::distributed::consensus::Raft>>,
        method: &str,
        payload: &[u8],
    ) -> RpcResult<Vec<u8>> {
        // Try Raft handlers first
        if let Some(raft) = raft {
            match method {
                "raft_request_vote" => {
                    let args: crate::distributed::consensus::RequestVoteArgs =
                        bincode::deserialize(payload)
                            .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                    let result = raft.handle_request_vote(args).await;
                    let response = bincode::serialize(&result)
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    return Ok(response);
                }
                "raft_append_entries" => {
                    let args: crate::distributed::consensus::AppendEntriesArgs =
                        bincode::deserialize(payload)
                            .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                    let result = raft.handle_append_entries(args).await;
                    let response = bincode::serialize(&result)
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    return Ok(response);
                }
                _ => {}
            }
        }

        // Try membership handlers
        match RpcMethod::from_str(method) {
            Some(RpcMethod::GetMembership) => {
                if let Some(membership) = membership {
                    let nodes = membership.get_all_nodes();
                    let response = MembershipResponse { nodes };
                    let payload = bincode::serialize(&response)
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    Ok(payload)
                } else {
                    Err(RpcError::Rpc("Membership not available".to_string()))
                }
            }
            Some(RpcMethod::Join) => {
                let request: JoinRequest = bincode::deserialize(payload)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;

                if let Some(membership) = membership {
                    membership.add_node(request.node.clone());
                    let nodes = membership.get_all_nodes();
                    let response = JoinResponse {
                        success: true,
                        cluster_nodes: nodes,
                    };
                    let payload = bincode::serialize(&response)
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    Ok(payload)
                } else {
                    Err(RpcError::Rpc("Membership not available".to_string()))
                }
            }
            Some(RpcMethod::ExecuteOperator) => Self::handle_execute_operator(payload).await,
            Some(RpcMethod::ExecuteQueryAggregation) => Self::handle_execute_query_aggregation(payload).await,
            Some(RpcMethod::ShuffleData) => Self::handle_shuffle_data(payload).await,
            Some(RpcMethod::ReplicateData) => Self::handle_replicate_data(payload).await,
            Some(RpcMethod::SyncReplica) => Self::handle_sync_replica(payload).await,
            Some(RpcMethod::PromoteReplica) => Self::handle_promote_replica(payload).await,
            _ => Err(RpcError::MethodNotFound(method.to_string())),
        }
    }

    /// Handle execute operator RPC
    async fn handle_execute_operator(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::ExecuteOperatorRequest;
        use crate::operators::{FilterOp, MapOp, StreamOperator};

        let request: ExecuteOperatorRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        // Process events based on operator type
        // Note: This is a simplified implementation. In production, we'd need
        // to serialize/deserialize operator configurations properly
        let results = match request.operator_type.as_str() {
            "filter" => {
                // For filter, we need a predicate. Since we can't serialize closures,
                // we'll use a simple default filter (pass all events)
                // In a real implementation, we'd serialize the predicate logic
                let mut op = FilterOp::new(|_| true);
                let mut processed = Vec::new();
                for event in request.events {
                    if let Ok(Some(result)) = op.process(event) {
                        processed.push(result);
                    }
                }
                processed
            }
            "map" => {
                // For map, we'll use identity mapping
                // In a real implementation, we'd serialize the mapper function
                let mut op = MapOp::new(|e| e);
                let mut processed = Vec::new();
                for event in request.events {
                    if let Ok(Some(result)) = op.process(event) {
                        processed.push(result);
                    }
                }
                processed
            }
            _ => {
                return Err(RpcError::Rpc(format!(
                    "Unknown operator type: {}",
                    request.operator_type
                )));
            }
        };

        let response = crate::network::protocol::ExecuteOperatorResponse { events: results };

        let payload =
            bincode::serialize(&response).map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle execute query aggregation RPC
    async fn handle_execute_query_aggregation(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::{ExecuteQueryAggregationRequest, ExecuteQueryAggregationResponse};
        use crate::query::{QueryExecutor, ast::Query};

        let request: ExecuteQueryAggregationRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        debug!(
            "Received query aggregation request: query_id={}, partition={}, events={}",
            request.query_id,
            request.partition,
            request.events.len()
        );

        // Deserialize the query AST
        let query: Query = bincode::deserialize(&request.query)
            .map_err(|e| RpcError::Deserialization(format!("Failed to deserialize query: {}", e)))?;

        // Create a local query executor (not distributed, since this is the remote node)
        let executor = QueryExecutor::new(query);

        // Apply WHERE filter if present
        let filtered_events = if let Some(ref where_clause) = executor.query.where_clause {
            request.events
                .into_iter()
                .filter(|event| {
                    match where_clause.condition.evaluate(event) {
                        crate::query::ast::Value::Boolean(b) => b,
                        _ => false,
                    }
                })
                .collect()
        } else {
            request.events
        };

        // Determine aggregation type and execute
        let results = if let (Some(ref group_by), Some(ref aggregations)) = 
            (&executor.query.group_by, &executor.query.aggregations) {
            // Grouped aggregations
            executor.execute_grouped_aggregations(filtered_events, group_by, aggregations)
                .await
                .map_err(|e| RpcError::Rpc(format!("Query execution error: {}", e)))?
        } else if let Some(ref aggregations) = executor.query.aggregations {
            // Global aggregations (no GROUP BY)
            executor.execute_global_aggregations(filtered_events, aggregations)
                .await
                .map_err(|e| RpcError::Rpc(format!("Query execution error: {}", e)))?
        } else {
            // No aggregations - just return filtered events (shouldn't happen in practice)
            warn!("Query aggregation request has no aggregations, returning filtered events");
            filtered_events
        };

        debug!(
            "Query aggregation completed: query_id={}, partition={}, results={}",
            request.query_id,
            request.partition,
            results.len()
        );

        let response = ExecuteQueryAggregationResponse {
            results,
            partition: request.partition,
        };

        let payload = bincode::serialize(&response)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle shuffle data RPC
    async fn handle_shuffle_data(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::ShuffleDataRequest;

        let request: ShuffleDataRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        debug!(
            "Received shuffle data: {} events for partition {}, operation: {}",
            request.events.len(),
            request.target_partition,
            request.operation_type
        );

        // In a full implementation, we'd:
        // 1. Store events in a shuffle buffer
        // 2. Process them when all data for the operation is received
        // 3. Return acknowledgment

        let response = crate::network::protocol::ShuffleDataResponse {
            success: true,
            events_received: request.events.len(),
        };

        let payload =
            bincode::serialize(&response).map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle replicate data RPC (follower receives data from leader)
    async fn handle_replicate_data(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::{ReplicateDataRequest, ReplicateDataResponse};

        let request: ReplicateDataRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        debug!(
            "Received replication data: {} events for partition {}, sequence: {}",
            request.events.len(),
            request.partition,
            request.sequence
        );

        // In a full implementation, we'd:
        // 1. Store events in the replica's state backend
        // 2. Update replica sync status
        // 3. Return acknowledgment with sequence number

        let response = ReplicateDataResponse {
            success: true,
            sequence: request.sequence,
            message: format!("Replicated {} events", request.events.len()),
        };

        let payload =
            bincode::serialize(&response).map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle sync replica RPC (check replica sync status)
    async fn handle_sync_replica(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::{SyncReplicaRequest, SyncReplicaResponse};

        let request: SyncReplicaRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        debug!(
            "Sync replica request for partition {}, current sequence: {}",
            request.partition, request.sequence
        );

        // In a full implementation, we'd:
        // 1. Check local sequence number
        // 2. Compare with requested sequence
        // 3. Return sync status

        let response = SyncReplicaResponse {
            success: true,
            in_sync: true, // Simplified: always in sync
            last_sequence: request.sequence,
        };

        let payload =
            bincode::serialize(&response).map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle promote replica RPC (promote follower to leader)
    async fn handle_promote_replica(payload: &[u8]) -> RpcResult<Vec<u8>> {
        use crate::network::protocol::{PromoteReplicaRequest, PromoteReplicaResponse};

        let request: PromoteReplicaRequest =
            bincode::deserialize(payload).map_err(|e| RpcError::Deserialization(e.to_string()))?;

        debug!(
            "Promote replica request for partition {}, new leader: {:?}",
            request.partition, request.new_leader
        );

        // In a full implementation, we'd:
        // 1. Update local replica role to Leader
        // 2. Start accepting writes
        // 3. Notify other replicas

        let response = PromoteReplicaResponse {
            success: true,
            message: format!("Promoted to leader for partition {}", request.partition),
        };

        let payload =
            bincode::serialize(&response).map_err(|e| RpcError::Serialization(e.to_string()))?;
        Ok(payload)
    }

    /// Handle a connection (instance method that delegates to static)
    #[allow(dead_code)] // May be used in the future
    async fn handle_connection(&self, conn: TransportConnection) -> RpcResult<()> {
        Self::handle_connection_static(
            Arc::clone(&self.handlers),
            self.membership.clone(),
            self.raft.clone(),
            conn,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::membership::ClusterMembership;
    use crate::distributed::node::NodeId;

    fn create_test_node(id: u64, port: u16) -> NodeMetadata {
        let node_id = NodeId::new(id);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        NodeMetadata::new(node_id, addr)
    }

    #[tokio::test]
    async fn test_rpc_join() {
        // Create server transport
        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server_transport = Transport::bind(server_addr).await.unwrap();
        let server_addr = server_transport.local_addr();

        let membership = Arc::new(ClusterMembership::new(NodeId::new(1), 30));
        let server = RpcServer::new(server_transport).with_membership(membership.clone());

        // Start server in background
        let server_handle = tokio::spawn(async move {
            let _ = server.start().await;
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create client with separate transport (doesn't need listener)
        let client_transport = Transport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client = RpcClient::new(client_transport);

        // Join
        let node = create_test_node(2, 8081);
        let response = client.join(server_addr, node.clone()).await.unwrap();

        assert!(response.success);
        assert!(response.cluster_nodes.len() > 0);

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_get_membership() {
        // Create server transport
        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server_transport = Transport::bind(server_addr).await.unwrap();
        let server_addr = server_transport.local_addr();

        let membership = Arc::new(ClusterMembership::new(NodeId::new(1), 30));
        // Add some nodes
        membership.add_node(create_test_node(2, 8081));
        membership.add_node(create_test_node(3, 8082));

        let server = RpcServer::new(server_transport).with_membership(membership.clone());

        // Start server in background
        let server_handle = tokio::spawn(async move {
            let _ = server.start().await;
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create client
        let client_transport = Transport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client = RpcClient::new(client_transport);

        // Get membership
        let response = client.get_membership(server_addr).await.unwrap();

        assert_eq!(response.nodes.len(), 2);

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_method_not_found() {
        // Create server transport
        let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server_transport = Transport::bind(server_addr).await.unwrap();
        let server_addr = server_transport.local_addr();

        let membership = Arc::new(ClusterMembership::new(NodeId::new(1), 30));
        let server = RpcServer::new(server_transport).with_membership(membership);

        // Start server in background
        let server_handle = tokio::spawn(async move {
            let _ = server.start().await;
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create client
        let client_transport = Transport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client = RpcClient::new(client_transport);

        // Call non-existent method
        let result = client
            .call(server_addr, "nonexistent_method", Vec::new())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RpcError::Rpc(msg) => {
                assert!(msg.contains("Method not found") || msg.contains("nonexistent_method"));
            }
            _ => panic!("Expected RpcError::Rpc"),
        }

        // Cleanup
        server_handle.abort();
    }
}
