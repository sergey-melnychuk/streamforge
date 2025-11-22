//! CLI command implementations

use crate::cli::executor::JobExecutor;
use crate::cli::job::JobManager;
use std::path::PathBuf;
use std::sync::Arc;
use streamforge::config::Config;
use tracing::{debug, error, info, warn};

/// Submit a new job to a cluster node
pub async fn submit_job(
    config_path: PathBuf,
    name: Option<String>,
    daemon: bool,
    max_restarts: u32,
    restart_delay: u64,
    nodes: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use std::time::Duration;
    use streamforge::network::{RpcClient, Transport};
    
    info!("Submitting job from config: {:?}", config_path);

    // Load and validate config
    let config = load_and_validate_config(&config_path).await?;

    // Determine job name
    let job_name = name.unwrap_or_else(|| {
        config_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unnamed")
            .to_string()
    });

    // Parse node addresses from --nodes argument or use defaults
    let node_addresses: Vec<SocketAddr> = if let Some(nodes_str) = nodes {
        let parsed: Vec<SocketAddr> = nodes_str
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| s.parse().ok())
            .collect();
        if parsed.is_empty() {
            return Err(format!("Invalid node addresses: '{}'. Expected format: 'host:port,host:port'", nodes_str).into());
        }
        parsed
    } else {
        // Fallback to seed nodes from config, or default
        let parsed: Vec<SocketAddr> = config.cluster.seed_nodes
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        if parsed.is_empty() {
            vec!["127.0.0.1:9001".parse().unwrap()]
        } else {
            parsed
        }
    };

    info!("Will try submitting to nodes: {:?}", node_addresses);

    // Serialize job submission request
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SubmitJobRequest {
        name: String,
        config: streamforge::config::Config,
        daemon: bool,
        max_restarts: u32,
        restart_delay: u64,
    }

    let request = SubmitJobRequest {
        name: job_name.clone(),
        config,
        daemon,
        max_restarts,
        restart_delay,
    };

    // Use JSON instead of bincode because bincode doesn't support deserialize_any
    // which is required for tagged enums like SourceConfig and SinkConfig
    let payload = serde_json::to_vec(&request)
        .map_err(|e| format!("Failed to serialize job request: {}", e))?;

    // Create RPC client
    let transport = Transport::bind("0.0.0.0:0".parse().unwrap()).await
        .map_err(|e| format!("Failed to create transport: {}", e))?;
    let rpc_client = RpcClient::new(transport);
    
    // Try each node in order until one succeeds
    let mut last_error = None;
    let mut response_payload = None;
    let mut successful_node = None;
    
    for node_addr in &node_addresses {
        info!("Trying to submit job to node {}", node_addr);
        
        // Retry connection to this node with exponential backoff
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5; // Fewer retries per node since we try multiple nodes
        
        match loop {
            match rpc_client.call(*node_addr, "submit_job", payload.clone()).await {
                Ok(response) => break Ok(response),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        break Err(e);
                    }
                    retries += 1;
                    let delay = Duration::from_millis(500 * (1 << retries.min(3))); // Exponential backoff, max 4s
                    warn!("Failed to connect to node {} (attempt {}/{}): {}, retrying in {:?}...", 
                          node_addr, retries, MAX_RETRIES, e, delay);
                    tokio::time::sleep(delay).await;
                }
            }
        } {
            Ok(response) => {
                response_payload = Some(response);
                successful_node = Some(*node_addr);
                break;
            }
            Err(e) => {
                warn!("Failed to submit to node {}: {}", node_addr, e);
                last_error = Some(e);
                continue; // Try next node
            }
        }
    }
    
    let response_payload = response_payload.ok_or_else(|| {
        format!("Failed to submit job to any node. Tried: {:?}. Last error: {}", 
                node_addresses, 
                last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown".to_string()))
    })?;
    
    let node_addr = successful_node.unwrap();

    #[derive(serde::Deserialize)]
    struct SubmitJobResponse {
        success: bool,
        job_id: Option<String>,
        error: Option<String>,
    }

    // Use JSON instead of bincode for consistency
    let response: SubmitJobResponse = serde_json::from_slice(&response_payload)
        .map_err(|e| format!("Failed to deserialize response: {}", e))?;

    if response.success {
        if let Some(job_id) = response.job_id {
            println!("✅ Job submitted and started successfully!");
            println!("   Job ID: {}", job_id);
            println!("   Node: {}", node_addr);
            if daemon {
                println!("   Daemon mode: enabled (max restarts: {}, delay: {}s)", max_restarts, restart_delay);
            }
            println!("   Use 'streamforge status {}' to check status", job_id);
        } else {
            return Err("Job submission succeeded but no job ID returned".into());
        }
    } else {
        let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
        return Err(format!("Job submission failed: {}", error_msg).into());
    }

    Ok(())
}

/// List all jobs from cluster (queries any node, which forwards to Raft leader)
pub async fn list_jobs(
    verbose: bool,
    nodes: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use streamforge::network::{RpcClient, Transport};
    
    // Parse node addresses from --nodes argument or use defaults
    let node_addresses: Vec<SocketAddr> = if let Some(nodes_str) = nodes {
        let parsed: Vec<SocketAddr> = nodes_str
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| s.parse().ok())
            .collect();
        if parsed.is_empty() {
            return Err(format!("Invalid node addresses: '{}'. Expected format: 'host:port,host:port'", nodes_str).into());
        }
        parsed
    } else {
        // Default to common cluster nodes
        vec!["127.0.0.1:9001".parse().unwrap()]
    };
    
    info!("Querying jobs from cluster nodes: {:?}", node_addresses);
    
    // Create RPC client
    let transport = Transport::bind("0.0.0.0:0".parse().unwrap()).await
        .map_err(|e| format!("Failed to create transport: {}", e))?;
    let rpc_client = RpcClient::new(transport);
    
    // Try nodes until one responds (any node can forward to leader)
    let mut jobs = Vec::new();
    let mut queried_node = None;
    
    for node_addr in &node_addresses {
        match rpc_client.call(*node_addr, "list_jobs", vec![]).await {
            Ok(response_payload) => {
                #[derive(serde::Deserialize)]
                struct ListJobsResponse {
                    jobs: Vec<crate::cli::job::Job>,
                    error: Option<String>,
                }
                
                match serde_json::from_slice::<ListJobsResponse>(&response_payload) {
                    Ok(response) => {
                        if let Some(error) = response.error {
                            warn!("Error from node {}: {}", node_addr, error);
                            continue; // Try next node
                        }
                        jobs = response.jobs;
                        queried_node = Some(*node_addr);
                        break; // Success, stop trying other nodes
                    }
                    Err(e) => {
                        warn!("Failed to deserialize jobs from node {}: {}", node_addr, e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to query jobs from node {}: {}", node_addr, e);
            }
        }
    }
    
    if jobs.is_empty() {
        if queried_node.is_none() {
            println!("No jobs found (could not connect to any nodes).");
        } else {
            println!("No jobs found.");
        }
        return Ok(());
    }
    
    if verbose {
        println!(
            "{:<36} {:<20} {:<15} {:<20}",
            "JOB ID", "NAME", "STATUS", "SUBMITTED"
        );
        println!("{}", "-".repeat(91));
        for job in jobs {
            println!(
                "{:<36} {:<20} {:<15} {:<20}",
                job.id,
                job.name,
                format!("{:?}", job.status),
                format_time(job.submitted_at)
            );
        }
    } else {
        println!("{:<36} {:<20} {:<15}", "JOB ID", "NAME", "STATUS");
        println!("{}", "-".repeat(71));
        for job in jobs {
            println!(
                "{:<36} {:<20} {:<15}",
                job.id,
                job.name,
                format!("{:?}", job.status)
            );
        }
    }
    
    if let Some(node) = queried_node {
        println!("\n✓ Queried node: {}", node);
    }

    Ok(())
}

/// Show node status (queries cluster membership to find node, then queries node via RPC)
pub async fn show_node_status(
    node_id: u64,
    nodes: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use streamforge::distributed::NodeId;
    use streamforge::network::{RpcClient, Transport};
    
    let target_node_id = NodeId::new(node_id);
    
    // Parse node addresses from --nodes argument or use defaults
    let node_addresses: Vec<SocketAddr> = if let Some(nodes_str) = nodes {
        let parsed: Vec<SocketAddr> = nodes_str
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| s.parse().ok())
            .collect();
        if parsed.is_empty() {
            return Err(format!("Invalid node addresses: '{}'. Expected format: 'host:port,host:port'", nodes_str).into());
        }
        parsed
    } else {
        // Default to common cluster nodes
        vec!["127.0.0.1:9001".parse().unwrap()]
    };
    
    info!("Querying status of node {} from cluster nodes: {:?}", node_id, node_addresses);
    
    // Create RPC client
    let transport = Transport::bind("0.0.0.0:0".parse().unwrap()).await
        .map_err(|e| format!("Failed to create transport: {}", e))?;
    let rpc_client = RpcClient::new(transport);
    
    // Query any node to get membership, then find target node address
    let mut target_node_addr = None;
    
    for node_addr in &node_addresses {
        match rpc_client.get_membership(*node_addr).await {
            Ok(membership) => {
                // Find target node in membership
                for node_metadata in membership.nodes {
                    if node_metadata.id == target_node_id {
                        target_node_addr = Some(node_metadata.address);
                        break;
                    }
                }
                if target_node_addr.is_some() {
                    break;
                }
            }
            Err(e) => {
                warn!("Failed to get membership from node {}: {}", node_addr, e);
            }
        }
    }
    
    let target_addr = target_node_addr.ok_or_else(|| {
        format!("Node {} not found in cluster membership", node_id)
    })?;
    
    // Query target node for status
    match rpc_client.call(target_addr, "node_status", vec![]).await {
        Ok(response_payload) => {
            #[derive(serde::Deserialize)]
            struct NodeStatusResponse {
                node_id: u64,
                address: String,
                status: String,
                running_jobs: usize,
                total_jobs: usize,
            }
            
            match serde_json::from_slice::<NodeStatusResponse>(&response_payload) {
                Ok(status) => {
                    println!("Node Status:");
                    println!("  ID: {}", status.node_id);
                    println!("  Address: {}", status.address);
                    println!("  Status: {}", status.status);
                    println!("  Running Jobs: {}", status.running_jobs);
                    println!("  Total Jobs: {}", status.total_jobs);
                }
                Err(e) => {
                    return Err(format!("Failed to deserialize node status: {}", e).into());
                }
            }
        }
        Err(e) => {
            return Err(format!("Failed to query node status from {}: {}", target_addr, e).into());
        }
    }
    
    Ok(())
}

/// Stop a node (queries cluster membership to find node, then sends stop signal via RPC)
pub async fn stop_node(
    node_id: u64,
    force: bool,
    nodes: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use streamforge::distributed::NodeId;
    use streamforge::network::{RpcClient, Transport};
    
    let target_node_id = NodeId::new(node_id);
    
    info!("Stopping node: {} (force: {})", node_id, force);
    
    // Parse node addresses from --nodes argument or use defaults
    let node_addresses: Vec<SocketAddr> = if let Some(nodes_str) = nodes {
        let parsed: Vec<SocketAddr> = nodes_str
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| s.parse().ok())
            .collect();
        if parsed.is_empty() {
            return Err(format!("Invalid node addresses: '{}'. Expected format: 'host:port,host:port'", nodes_str).into());
        }
        parsed
    } else {
        // Default to common cluster nodes
        vec!["127.0.0.1:9001".parse().unwrap()]
    };
    
    // Create RPC client
    let transport = Transport::bind("0.0.0.0:0".parse().unwrap()).await
        .map_err(|e| format!("Failed to create transport: {}", e))?;
    let rpc_client = RpcClient::new(transport);
    
    // Query any node to get membership, then find target node address
    let mut target_node_addr = None;
    
    for node_addr in &node_addresses {
        match rpc_client.get_membership(*node_addr).await {
            Ok(membership) => {
                // Find target node in membership
                for node_metadata in membership.nodes {
                    if node_metadata.id == target_node_id {
                        target_node_addr = Some(node_metadata.address);
                        break;
                    }
                }
                if target_node_addr.is_some() {
                    break;
                }
            }
            Err(e) => {
                warn!("Failed to get membership from node {}: {}", node_addr, e);
            }
        }
    }
    
    let target_addr = target_node_addr.ok_or_else(|| {
        format!("Node {} not found in cluster membership", node_id)
    })?;
    
    // Send stop request to target node
    #[derive(serde::Serialize)]
    struct StopNodeRequest {
        force: bool,
    }
    
    let payload = serde_json::to_vec(&StopNodeRequest { force })
        .map_err(|e| format!("Failed to serialize stop request: {}", e))?;
    
    match rpc_client.call(target_addr, "stop_node", payload).await {
        Ok(response_payload) => {
            #[derive(serde::Deserialize)]
            struct StopNodeResponse {
                success: bool,
                error: Option<String>,
            }
            
            match serde_json::from_slice::<StopNodeResponse>(&response_payload) {
                Ok(response) => {
                    if response.success {
                        println!("✅ Stop signal sent to node {} ({})", node_id, target_addr);
                    } else {
                        let error = response.error.unwrap_or_else(|| "Unknown error".to_string());
                        return Err(format!("Failed to stop node: {}", error).into());
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to deserialize stop response: {}", e).into());
                }
            }
        }
        Err(e) => {
            return Err(format!("Failed to send stop request to node {} ({}): {}", node_id, target_addr, e).into());
        }
    }
    
    Ok(())
}

/// Validate a configuration file
pub async fn validate_config(
    config_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Validating configuration: {:?}", config_path);

    match load_and_validate_config(&config_path).await {
        Ok(config) => {
            println!("✅ Configuration is valid!");
            println!(
                "   Processing parallelism: {}",
                config.processing.parallelism
            );
            println!("   Cluster bind address: {:?}", config.cluster.bind_address);
            println!("   State backend: {:?}", config.state.backend_type);
            Ok(())
        }
        Err(e) => {
            error!("Configuration validation failed: {}", e);
            Err(e)
        }
    }
}

/// Show cluster status
pub async fn cluster_status() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Cluster Status:");
    println!("  (Cluster status functionality coming soon)");
    Ok(())
}

/// Show cluster nodes
pub async fn cluster_nodes() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Cluster Nodes:");
    println!("  (Cluster nodes functionality coming soon)");
    Ok(())
}

/// Start a cluster node
pub async fn start_node(
    config_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use streamforge::distributed::{Discovery, GossipConfig, GossipDiscovery, NodeId, NodeMetadata};
    use streamforge::metrics::server::MetricsServer;
    use streamforge::network::{RpcServer, Transport};
    use tokio::signal;
    use tokio::time::sleep;

    info!("Starting cluster node from config: {:?}", config_path);

    // Load config
    let config = load_and_validate_config(&config_path).await?;
    
    info!("Loaded config - cluster.bind_address: {}, metrics.enabled: {}, metrics.export_endpoint: {:?}", 
          config.cluster.bind_address, config.metrics.enabled, config.metrics.export_endpoint);

    // Parse bind address
    let bind_addr: SocketAddr = config.cluster.bind_address.parse()
        .map_err(|e| format!("Invalid bind address '{}': {}", config.cluster.bind_address, e))?;

    // Generate or use configured node ID
    let node_id = config.cluster.node_id
        .map(NodeId::new)
        .unwrap_or_else(NodeId::generate);

    let node_metadata = NodeMetadata::new(node_id, bind_addr);

    info!("Starting node {} on {}", node_id, bind_addr);

    // Start metrics server if configured
    let node_collector = if config.metrics.enabled {
        if let Some(ref _endpoint) = config.metrics.export_endpoint {
            Some(Arc::new(streamforge::metrics::collector::MetricsCollector::new(
                format!("node-{}", node_id.as_u64())
            )))
        } else {
            None
        }
    } else {
        None
    };
    
    let metrics_server_handle = if let Some(ref collector) = node_collector {
        if let Some(ref endpoint) = config.metrics.export_endpoint {
            let metrics_addr: SocketAddr = endpoint.parse()
                .map_err(|e| format!("Invalid metrics endpoint '{}': {}", endpoint, e))?;
            let metrics_server = MetricsServer::new(metrics_addr);
            
            // Register the collector for node metrics
            metrics_server.register_collector("node".to_string(), Arc::clone(collector)).await;
            
            let handle = metrics_server.start().await?;
            // Give the server a moment to bind and start listening
            sleep(Duration::from_millis(500)).await;
            info!("Metrics server handle obtained, should be listening on {}", metrics_addr);
            Some(handle)
        } else {
            None
        }
    } else {
        None
    };

    // Create transport and bind
    let transport = Arc::new(Transport::bind(bind_addr).await
        .map_err(|e| format!("Failed to bind transport to {}: {}", bind_addr, e))?);
    info!("Transport bound to {}", transport.local_addr());

    // Create cluster membership
    let membership = Arc::new(streamforge::distributed::ClusterMembership::new(
        node_id,
        config.cluster.gossip.heartbeat_timeout,
    ));
    membership.add_node(node_metadata.clone());

    // Create job manager and executor for this node
    let job_manager = Arc::new(JobManager::new().await?);
    let job_executor = Arc::new(JobExecutor::new_daemon(
        Arc::clone(&job_manager),
        3, // max restarts
        Duration::from_secs(5), // restart delay
    ));

    // Get metrics collector to track cluster activity
    let node_collector_opt = if config.metrics.enabled {
        if let Some(ref _endpoint) = config.metrics.export_endpoint {
            // We already created the collector above, but we need to access it
            // Let's create a shared reference to track cluster metrics
            Some(Arc::new(streamforge::metrics::collector::MetricsCollector::new(
                format!("node-{}", node_id.as_u64())
            )))
        } else {
            None
        }
    } else {
        None
    };

    // Create RPC server and register job submission handler
    // Now using Arc<Transport> so the listener is preserved
    let rpc_server = RpcServer::new(Arc::clone(&transport))
        .with_membership(membership.clone());
    
    // Register custom handler for job submission
    let executor_for_handler = job_executor.clone();
    let manager_for_handler = job_manager.clone();
    let handler: streamforge::network::rpc::RpcHandler = Arc::new(move |_method: String, payload: Vec<u8>| {
        let executor = executor_for_handler.clone();
        let manager = manager_for_handler.clone();
        Box::pin(async move {            
            #[derive(serde::Serialize, serde::Deserialize)]
            struct SubmitJobRequest {
                name: String,
                config: streamforge::config::Config,
                daemon: bool,
                max_restarts: u32,
                restart_delay: u64,
            }

            #[derive(serde::Serialize, serde::Deserialize)]
            struct SubmitJobResponse {
                success: bool,
                job_id: Option<String>,
                error: Option<String>,
            }

            // Use JSON instead of bincode because bincode doesn't support deserialize_any
            let request: SubmitJobRequest = match serde_json::from_slice(&payload) {
                Ok(r) => r,
                Err(e) => {
                    return Ok(serde_json::to_vec(&SubmitJobResponse {
                        success: false,
                        job_id: None,
                        error: Some(format!("Failed to deserialize request: {}", e)),
                    }).unwrap());
                }
            };

            // Submit job to manager
            let job_id = match manager.submit_job(request.name.clone(), request.config.clone()).await {
                Ok(id) => id,
                Err(e) => {
                    return Ok(serde_json::to_vec(&SubmitJobResponse {
                        success: false,
                        job_id: None,
                        error: Some(e.to_string()),
                    }).unwrap());
                }
            };

            // Start job execution
            if let Err(e) = executor.start_job(&job_id).await {
                return Ok(serde_json::to_vec(&SubmitJobResponse {
                    success: false,
                    job_id: None,
                    error: Some(format!("Failed to start job: {}", e)),
                }).unwrap());
            }

            let response = SubmitJobResponse {
                success: true,
                job_id: Some(job_id),
                error: None,
            };

            serde_json::to_vec(&response)
                .map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))
        })
    });
    rpc_server.register_handler("submit_job", handler).await;
    
    // Register handler for list_jobs
    // TODO: When Raft is integrated, check if this node is the leader.
    // If not, forward the request to the leader node.
    let manager_for_list = job_manager.clone();
    let list_handler: streamforge::network::rpc::RpcHandler = Arc::new(move |_method: String, _payload: Vec<u8>| {
        let manager = manager_for_list.clone();
        Box::pin(async move {
            // TODO: Check if this node is the Raft leader.
            // If not, forward request to leader and return its response.
            // For now, return jobs from this node's JobManager.
            
            let jobs = match manager.list_jobs().await {
                Ok(jobs) => jobs,
                Err(e) => {
                    #[derive(serde::Serialize)]
                    struct ListJobsResponse {
                        jobs: Vec<crate::cli::job::Job>,
                        error: Option<String>,
                    }
                    return Ok(serde_json::to_vec(&ListJobsResponse {
                        jobs: vec![],
                        error: Some(e.to_string()),
                    }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))?);
                }
            };
            
            #[derive(serde::Serialize)]
            struct ListJobsResponse {
                jobs: Vec<crate::cli::job::Job>,
                error: Option<String>,
            }
            
            serde_json::to_vec(&ListJobsResponse {
                jobs,
                error: None,
            }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))
        })
    });
    rpc_server.register_handler("list_jobs", list_handler).await;
    
    // Register handler for node_status
    let manager_for_status = job_manager.clone();
    let node_id_for_status = node_id;
    let bind_addr_for_status = bind_addr;
    let status_handler: streamforge::network::rpc::RpcHandler = Arc::new(move |_method: String, _payload: Vec<u8>| {
        let manager = manager_for_status.clone();
        let node_id = node_id_for_status;
        let bind_addr = bind_addr_for_status;
        Box::pin(async move {
            let jobs = manager.list_jobs().await.unwrap_or_default();
            let running_jobs = jobs.iter()
                .filter(|j| matches!(j.status, crate::cli::job::JobStatus::Running))
                .count();
            
            #[derive(serde::Serialize)]
            struct NodeStatusResponse {
                node_id: u64,
                address: String,
                status: String,
                running_jobs: usize,
                total_jobs: usize,
            }
            
            serde_json::to_vec(&NodeStatusResponse {
                node_id: node_id.as_u64(),
                address: bind_addr.to_string(),
                status: "Running".to_string(),
                running_jobs,
                total_jobs: jobs.len(),
            }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))
        })
    });
    rpc_server.register_handler("node_status", status_handler).await;
    
    // Create shutdown channel for stop_node handler
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    
    // Register handler for stop_node
    let node_id_for_stop = node_id;
    let shutdown_tx_for_handler = shutdown_tx.clone();
    let stop_handler: streamforge::network::rpc::RpcHandler = Arc::new(move |_method: String, payload: Vec<u8>| {
        let shutdown_tx = shutdown_tx_for_handler.clone();
        let node_id = node_id_for_stop;
        Box::pin(async move {
            #[derive(serde::Deserialize)]
            struct StopNodeRequest {
                force: bool,
            }
            
            let request: StopNodeRequest = match serde_json::from_slice(&payload) {
                Ok(r) => r,
                Err(e) => {
                    #[derive(serde::Serialize)]
                    struct StopNodeResponse {
                        success: bool,
                        error: Option<String>,
                    }
                    return Ok(serde_json::to_vec(&StopNodeResponse {
                        success: false,
                        error: Some(format!("Failed to deserialize request: {}", e)),
                    }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))?);
                }
            };
            
            info!("Received stop request for node {} (force: {})", node_id, request.force);
            
            // Send shutdown signal (non-blocking send)
            if shutdown_tx.try_send(()).is_err() {
                #[derive(serde::Serialize)]
                struct StopNodeResponse {
                    success: bool,
                    error: Option<String>,
                }
                return Ok(serde_json::to_vec(&StopNodeResponse {
                    success: false,
                    error: Some("Shutdown channel closed".to_string()),
                }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))?);
            }
            
            #[derive(serde::Serialize)]
            struct StopNodeResponse {
                success: bool,
                error: Option<String>,
            }
            
            serde_json::to_vec(&StopNodeResponse {
                success: true,
                error: None,
            }).map_err(|e| streamforge::network::rpc::RpcError::Serialization(e.to_string()))
        })
    });
    rpc_server.register_handler("stop_node", stop_handler).await;
    
    let _membership_for_metrics = Arc::clone(&membership);
    let collector_for_rpc = node_collector_opt.clone();
    
    // Spawn RPC server as a background daemon task
    // It will keep running even if it encounters errors
    // The RPC server holds Arc<Transport>, so the transport (and its listener) will stay alive
    let rpc_server_handle = tokio::spawn(async move {
        // Track RPC server start as an event
        if let Some(ref collector) = collector_for_rpc {
            collector.record_event(0); // Record server start
        }
        
        // RPC server runs indefinitely in daemon mode
        // The rpc_server holds Arc<Transport>, which keeps the listener alive
        if let Err(e) = rpc_server.start().await {
            error!("RPC server error: {}", e);
            // In daemon mode, we log the error but don't exit
            // The node continues running
        }
    });
    
    // Spawn task to periodically update job metrics from running jobs
    let job_manager_for_metrics = Arc::clone(&job_manager);
    if let Some(ref collector) = node_collector {
        let collector_for_jobs = Arc::clone(collector);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                
                // Count running jobs
                let jobs = job_manager_for_metrics.list_jobs().await.unwrap_or_default();
                let running_count = jobs.iter()
                    .filter(|j| matches!(j.status, crate::cli::job::JobStatus::Running))
                    .count();
                
                if running_count > 0 {
                    collector_for_jobs.record_event(running_count);
                }
            }
        });
    }

    // Parse seed nodes
    let seed_nodes: Vec<SocketAddr> = config.cluster.seed_nodes
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    // Create gossip config
    let gossip_config = GossipConfig {
        gossip_interval: Duration::from_secs(config.cluster.gossip.gossip_interval),
        gossip_fanout: config.cluster.gossip.gossip_fanout,
        heartbeat_timeout: Duration::from_secs(config.cluster.gossip.heartbeat_timeout),
        dead_timeout: Duration::from_secs(config.cluster.gossip.dead_timeout),
        seed_nodes,
    };

    // Create and start gossip discovery
    let mut discovery = GossipDiscovery::new(node_metadata.clone(), gossip_config.clone())
        .with_transport(transport.clone());

    // Start discovery (this spawns background tasks)
    // Note: start() returns immediately after spawning background tasks
    discovery.start().await;
    info!("Gossip discovery started");

    // Wait a bit for discovery to bootstrap
    sleep(Duration::from_millis(500)).await;

    println!("✅ Cluster node started successfully!");
    println!("   Node ID: {}", node_id);
    println!("   Address: {}", bind_addr);
    if let Some(ref endpoint) = config.metrics.export_endpoint {
        println!("   Metrics: http://{}/metrics", endpoint);
    }
    println!("   Running as daemon (use 'streamforge stop' or kill signal to stop)");

    // Keep transport and RPC server alive to prevent connection issues
    // The RPC server is already spawned in a background task, but we need to keep
    // the transport Arc alive so the listener stays bound
    let _transport_keepalive = Arc::clone(&transport);
    
    // Run as daemon - keep alive until explicitly stopped
    // Spawn tasks for RPC and metrics servers, but don't exit if they fail
    // The node should keep running even if individual components fail
    if let Some(handle) = metrics_server_handle {
        tokio::spawn(async move {
            if let Err(e) = handle.await {
                error!("Metrics server task error: {:?}", e);
            }
        });
    }
    
    // Keep the main task alive - wait for shutdown signal
    // In daemon mode, we only exit on explicit shutdown signals
    // Also monitor RPC server to ensure it stays running
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal, shutting down node {}...", node_id);
        }
        _ = shutdown_rx.recv() => {
            info!("Received stop_node RPC request, shutting down node {}...", node_id);
        }
        result = rpc_server_handle => {
            error!("RPC server task exited unexpectedly: {:?}", result);
            // In daemon mode, we should keep running even if RPC server fails
            // Wait for explicit shutdown
            signal::ctrl_c().await.ok();
        }
        _ = async {
            // Keep alive indefinitely - daemon mode
            std::future::pending::<()>().await;
        } => {
            // This branch should never be reached
        }
    }

    info!("Node {} stopped", node_id);

    Ok(())
}

/// Load and validate a configuration file
async fn load_and_validate_config(
    path: &PathBuf,
) -> Result<Config, Box<dyn std::error::Error + Send + Sync>> {
    // Read file
    let content = tokio::fs::read_to_string(path).await?;

    // Try to parse as full Config first
    let config: Result<Config, _> = toml::from_str(&content);
    
    // If that fails, try parsing as job-only config
    let config = match config {
        Ok(cfg) => cfg,
        Err(e) => {
            // Log the error for debugging
            debug!("Failed to parse as full Config, trying job-only: {}", e);
            // Parse as job-only config (just [processing] and [job] sections)
            use streamforge::config::{JobDefinition, ProcessingConfig};
            let job_config: toml::Value = toml::from_str(&content)
                .map_err(|e| format!("Failed to parse config: {}", e))?;
            
            // Extract processing config by serializing the section and deserializing
            let processing = job_config
                .get("processing")
                .and_then(|p| {
                    toml::to_string(p)
                        .ok()
                        .and_then(|s| toml::from_str::<ProcessingConfig>(&s).ok())
                })
                .unwrap_or_default();
            
            // Extract job definition from [job] section
            // Also check if sql/source/sink are at top level (for backward compatibility)
            let job = job_config
                .get("job")
                .and_then(|j| {
                    toml::to_string(j)
                        .ok()
                        .and_then(|s| toml::from_str::<JobDefinition>(&s).ok())
                })
                .or_else(|| {
                    // If [job] section doesn't exist, check if sql/source/sink are at top level
                    if job_config.get("source").is_some() || job_config.get("sql").is_some() {
                        toml::from_str::<JobDefinition>(&content).ok()
                    } else {
                        None
                    }
                });
            
            // Try to extract cluster and metrics configs if they exist
            let cluster = if let Some(cluster_table) = job_config.get("cluster").and_then(|c| c.as_table()) {
                // Manually extract cluster config fields
                let mut cluster = streamforge::config::cluster::ClusterConfig::default();
                if let Some(bind_addr) = cluster_table.get("bind_address").and_then(|v| v.as_str()) {
                    cluster.bind_address = bind_addr.to_string();
                }
                if let Some(seeds) = cluster_table.get("seed_nodes").and_then(|v| v.as_array()) {
                    cluster.seed_nodes = seeds.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                }
                // Extract gossip config if present
                if let Some(gossip_table) = cluster_table.get("gossip").and_then(|g| g.as_table()) {
                    if let Some(interval) = gossip_table.get("gossip_interval").and_then(|v| v.as_integer()) {
                        cluster.gossip.gossip_interval = interval as u64;
                    }
                    if let Some(fanout) = gossip_table.get("gossip_fanout").and_then(|v| v.as_integer()) {
                        cluster.gossip.gossip_fanout = fanout as usize;
                    }
                    if let Some(timeout) = gossip_table.get("heartbeat_timeout").and_then(|v| v.as_integer()) {
                        cluster.gossip.heartbeat_timeout = timeout as u64;
                    }
                    if let Some(dead) = gossip_table.get("dead_timeout").and_then(|v| v.as_integer()) {
                        cluster.gossip.dead_timeout = dead as u64;
                    }
                }
                cluster
            } else {
                streamforge::config::cluster::ClusterConfig::default()
            };
            
            let metrics = job_config
                .get("metrics")
                .and_then(|m| {
                    toml::to_string(m)
                        .ok()
                        .and_then(|s| toml::from_str::<streamforge::config::MetricsConfig>(&s).ok())
                })
                .unwrap_or_default();
            
            // Create Config with extracted or default fields
            Config {
                processing,
                cluster,
                state: Default::default(),
                network: Default::default(),
                metrics,
                job,
            }
        }
    };

    // Validate
    validate_config_values(&config)?;

    Ok(config)
}

/// Validate configuration values
fn validate_config_values(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Validate parallelism
    if config.processing.parallelism == 0 {
        return Err("Processing parallelism must be > 0".into());
    }

    // Validate buffer size
    if config.processing.buffer_size == 0 {
        return Err("Buffer size must be > 0".into());
    }

    // Validate batch size
    if config.processing.batch_size == 0 {
        return Err("Batch size must be > 0".into());
    }

    Ok(())
}

/// Check if cluster nodes are ready
pub async fn check_nodes_ready(
    nodes: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::net::SocketAddr;
    use streamforge::network::{RpcClient, Transport};
    
    // Parse node addresses
    let node_addresses: Vec<SocketAddr> = nodes
        .split(',')
        .map(|s| s.trim())
        .filter_map(|s| s.parse().ok())
        .collect();
    
    if node_addresses.is_empty() {
        return Err("No valid node addresses provided. Expected format: 'host:port,host:port'".into());
    }
    
    info!("Checking readiness of {} nodes: {:?}", node_addresses.len(), node_addresses);
    
    // Create RPC client
    let transport = Transport::bind("0.0.0.0:0".parse().unwrap()).await
        .map_err(|e| format!("Failed to create transport: {}", e))?;
    let rpc_client = RpcClient::new(transport);
    
    let mut ready_nodes = Vec::new();
    let mut not_ready_nodes: Vec<(SocketAddr, String)> = Vec::new();
    
    // Check each node
    for node_addr in &node_addresses {
        // Try to get membership - if we can connect and get a response, node is ready
        match rpc_client.get_membership(*node_addr).await {
            Ok(_) => {
                ready_nodes.push(*node_addr);
                println!("✅ Node {} is ready", node_addr);
            }
            Err(e) => {
                let error_msg = e.to_string();
                not_ready_nodes.push((*node_addr, error_msg.clone()));
                println!("❌ Node {} is not ready: {}", node_addr, error_msg);
            }
        }
    }
    
    println!("\nSummary: {}/{} nodes ready", ready_nodes.len(), node_addresses.len());
    
    if not_ready_nodes.is_empty() {
        println!("✅ All nodes are ready!");
        Ok(())
    } else {
        println!("⚠️  Some nodes are not ready:");
        for (addr, error) in &not_ready_nodes {
            println!("   {}: {}", addr, error);
        }
        Err(format!("Not all nodes are ready: {}/{} ready", ready_nodes.len(), node_addresses.len()).into())
    }
}

/// Format timestamp for display
fn format_time(timestamp: u64) -> String {
    use std::time::SystemTime;
    let datetime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
    format!("{:?}", datetime)
}
