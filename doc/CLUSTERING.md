# StreamForge Cluster Guide

## Overview

StreamForge clusters provide distributed stream processing with zero-infrastructure setup. This guide explains how clusters work and how to use them.

## Architecture

### Components

1. **Nodes**: Individual processing units in the cluster
2. **Membership**: Tracks which nodes are alive/dead
3. **Discovery**: Gossip-based peer discovery (SWIM protocol)
4. **Partition Assignment**: Consistent hashing for work distribution
5. **RPC**: Network communication between nodes
6. **State Backend**: Storage for operator state

### Cluster Lifecycle

```
1. Node Startup
   ├── Bind to network address
   ├── Start RPC server
   └── Initialize membership

2. Discovery
   ├── Contact seed nodes (if any)
   ├── Exchange membership information
   └── Start gossip protocol

3. Partition Assignment
   ├── Calculate consistent hash ring
   ├── Assign partitions to nodes
   └── Store assignment in state

4. Processing
   ├── Events routed to assigned partitions
   ├── State stored per partition
   └── Heartbeats maintain membership

5. Rebalancing
   ├── Node joins/leaves detected
   ├── Recalculate partition assignment
   └── Migrate state (if needed)
```

## Key Concepts

### 1. Node Identity

Each node has a unique `NodeId` and network address:

```rust
let node_id = NodeId::generate();
let address: SocketAddr = "127.0.0.1:9001".parse()?;
let node = NodeMetadata::new(node_id, address);
```

### 2. Cluster Membership

Membership tracks all nodes and their status:

```rust
let membership = ClusterMembership::new(local_id, heartbeat_timeout);
membership.add_node(node);
let alive_nodes = membership.get_alive_nodes();
```

**Node Statuses:**
- `Alive`: Node is healthy and processing
- `Suspected`: Heartbeat timeout, but not confirmed dead
- `Dead`: Confirmed failed node
- `Leaving`: Graceful shutdown in progress

### 3. Raft Consensus

Raft provides strong consistency guarantees for cluster coordination:

```rust
let raft_config = RaftConfig {
    election_timeout_min: Duration::from_millis(150),
    election_timeout_max: Duration::from_millis(300),
    heartbeat_interval: Duration::from_millis(50),
};

let raft = Arc::new(Raft::new(local_id, Arc::new(transport), raft_config));
raft.add_node(node_id, address).await;
raft.start().await?;

// Check leadership
if raft.is_leader().await {
    // This node is the leader
}
```

**How it works:**
1. **Follower**: Receives log entries from leader
2. **Candidate**: Seeks votes for leadership (on timeout)
3. **Leader**: Handles requests and replicates log to followers
4. **Election**: Majority vote required to become leader
5. **Term**: Increments on each election attempt

**Raft RPCs:**
- `RequestVote`: Candidate requests vote from followers
- `AppendEntries`: Leader sends log entries (or heartbeats)

### 4. Gossip Discovery

SWIM-style gossip protocol for automatic peer discovery:

```rust
let config = GossipConfig {
    gossip_interval: Duration::from_secs(1),
    gossip_fanout: 3,
    heartbeat_timeout: Duration::from_secs(10),
    dead_timeout: Duration::from_secs(30),
    seed_nodes: vec!["127.0.0.1:9001".parse()?],
};

let discovery = GossipDiscovery::new(local_node, config)
    .with_transport(Arc::new(transport));
discovery.start().await;
```

**How it works:**
1. Periodically select random nodes to gossip with
2. Exchange membership information
3. Detect failures via heartbeat timeouts
4. Propagate membership changes through gossip

### 5. Partition Assignment

Consistent hashing ensures minimal partition movement:

```rust
let assigner = ConsistentHashAssigner::new(100, 1); // 100 virtual nodes
let assignment = assigner.assign(num_partitions, &nodes);

// When topology changes:
let new_assignment = assigner.rebalance(num_partitions, &old_assignment, &new_nodes);
```

**Benefits:**
- Only ~1/N partitions move when a node joins/leaves (N = number of nodes)
- Deterministic assignment
- Supports virtual nodes for better distribution

### 5. RPC Communication

Nodes communicate via RPC for coordination:

```rust
// Server side
let server = RpcServer::new(transport)
    .with_membership(membership);
server.start().await?;

// Client side
let client = RpcClient::new(transport);
let response = client.join(server_addr, node).await?;
```

**RPC Methods:**
- `join`: Join a cluster
- `leave`: Leave a cluster
- `get_membership`: Get current cluster membership
- `get_partitions`: Get partition assignment
- `update_partitions`: Update partition assignment

### 6. State Management

State is stored per partition using pluggable backends:

```rust
// In-memory (fast, non-persistent)
let backend = MemoryStateBackend::new();

// Store state
backend.put(b"partition:5:state", data.into()).await?;

// Retrieve state
let state = backend.get(b"partition:5:state").await?;

// List keys with prefix
let keys = backend.list_keys(b"partition:").await?;
```

## Example: Multi-Node Cluster with Raft

```rust
// Node 1: Leader with Raft
let transport1 = Transport::bind("127.0.0.1:9001".parse()?).await?;
let raft1 = Arc::new(Raft::new(node1_id, Arc::new(transport1.clone()), config));
raft1.add_node(node1_id, node1_addr).await;
raft1.add_node(node2_id, node2_addr).await;
raft1.start().await?;

let rpc_server1 = RpcServer::new(transport1)
    .with_membership(membership)
    .with_raft(raft1.clone());
tokio::spawn(async move { rpc_server1.start().await });

// Node 2: Follower
let transport2 = Transport::bind("127.0.0.1:9002".parse()?).await?;
let raft2 = Arc::new(Raft::new(node2_id, Arc::new(transport2.clone()), config));
raft2.add_node(node1_id, node1_addr).await;
raft2.add_node(node2_id, node2_addr).await;
raft2.start().await?;
```

## Example: Multi-Node Cluster (Legacy)

```rust
// Node 1: Coordinator
let transport1 = Transport::bind("127.0.0.1:9001".parse()?).await?;
let membership1 = Arc::new(ClusterMembership::new(node1_id, 30));
let server1 = RpcServer::new(transport1.clone())
    .with_membership(membership1.clone());
tokio::spawn(async move { server1.start().await });

// Node 2: Worker
let transport2 = Transport::bind("127.0.0.1:9002".parse()?).await?;
let client2 = RpcClient::new(transport2);
client2.join(node1_addr, node2).await?;

// Assign partitions
let assigner = ConsistentHashAssigner::new(100, 1);
let assignment = assigner.assign(12, &membership1.get_alive_nodes());
```

## Rebalancing

When a node joins or leaves:

1. **Detection**: Gossip protocol detects membership change
2. **Recalculation**: Consistent hash recalculates assignment
3. **Minimal Movement**: Only affected partitions move
4. **State Migration**: State can be migrated (future feature)

**Example:**
- 3 nodes, 12 partitions → 4 nodes, 12 partitions
- Only ~3 partitions move (25% movement ratio)
- Other 9 partitions stay on same nodes

## Failure Handling

1. **Heartbeat Timeout**: Node marked as `Suspected`
2. **Dead Timeout**: Node marked as `Dead` after extended timeout
3. **Automatic Rebalancing**: Partitions reassigned to remaining nodes
4. **State Recovery**: From checkpoints (future feature)

## Best Practices

1. **Seed Nodes**: Configure 2-3 seed nodes for bootstrap
2. **Heartbeat Timeout**: Balance between fast detection and network tolerance
3. **Virtual Nodes**: Use 100+ virtual nodes for better distribution
4. **Partition Count**: Choose partition count based on expected cluster size
5. **State Backend**: Use in-memory for speed, RocksDB for durability (future)

## Performance Characteristics

- **Discovery**: O(log N) gossip rounds for full propagation
- **Partition Assignment**: O(P) where P = number of partitions
- **Rebalancing**: Only ~1/N partitions move (N = number of nodes)
- **RPC Latency**: Sub-millisecond for local network
- **State Operations**: < 100μs for in-memory backend

## See Also

- [examples/cluster_demo.rs](examples/cluster_demo.rs) - Complete cluster demonstration
- [examples/distributed_cluster.rs](examples/distributed_cluster.rs) - Basic distributed operations
- [PLAN.md](PLAN.md) - Full architecture and roadmap

