# StreamForge Performance Tuning Guide

This guide covers performance optimization strategies for StreamForge.

## Table of Contents

1. [Performance Targets](#performance-targets)
2. [CPU Optimization](#cpu-optimization)
3. [Memory Optimization](#memory-optimization)
4. [Network Optimization](#network-optimization)
5. [State Backend Optimization](#state-backend-optimization)
6. [Cluster Optimization](#cluster-optimization)
7. [Benchmarking](#benchmarking)
8. [Troubleshooting Performance Issues](#troubleshooting-performance-issues)

## Performance Targets

StreamForge is designed to achieve:

- **Throughput**: > 1M events/sec per core
- **Latency**: p99 < 10ms for simple operations
- **State Operations**: < 100μs for state lookups
- **Recovery Time**: < 30s for 1GB state
- **Network Overhead**: < 5% of payload size

## CPU Optimization

### Parallelism

Set parallelism to match CPU cores:

```toml
[processing]
parallelism = 8  # Match number of CPU cores
```

**Best Practices:**
- Use `num_cpus::get()` to auto-detect cores
- For CPU-intensive workloads, use 1-2x CPU cores
- For I/O-intensive workloads, use 2-4x CPU cores

### Batch Size

Optimize batch size for throughput vs latency:

```toml
[processing]
batch_size = 1000  # Larger = higher throughput, higher latency
```

**Guidelines:**
- Small batches (100-500): Lower latency, lower throughput
- Medium batches (1000-5000): Balanced
- Large batches (10000+): Higher throughput, higher latency

### Operator Chaining

Chain operators to reduce overhead:

```rust
// Good: Chained operators
stream
    .filter(|e| e.value.as_int() > 0)
    .map(|e| e.with_value_changed(e.value.as_int().unwrap() * 2))
    .collect()
    .await?;

// Avoid: Separate collections
let filtered = stream.filter(|e| e.value.as_int() > 0).collect().await?;
let mapped = filtered.map(|e| e.with_value_changed(e.value.as_int().unwrap() * 2)).collect().await?;
```

## Memory Optimization

### Buffer Sizes

Configure buffer sizes based on memory availability:

```toml
[processing]
buffer_size = 2048  # Events in buffer
```

**Guidelines:**
- Small buffers (512-1024): Lower memory, may cause backpressure
- Medium buffers (2048-4096): Balanced
- Large buffers (8192+): Higher memory, smoother processing

### Backpressure

Enable backpressure to prevent memory issues:

```toml
[processing]
backpressure_enabled = true
```

**When to disable:**
- Very low latency requirements
- Guaranteed downstream capacity
- Small event sizes

### State Backend Selection

Choose appropriate state backend:

```toml
[state]
backend_type = "RocksDB"  # Persistent, slower
# backend_type = "Memory"  # Fast, non-persistent
```

**Memory Backend:**
- Fastest performance
- No persistence
- Limited by available RAM

**RocksDB Backend:**
- Persistent storage
- Slower than memory
- Can use disk for large state

## Network Optimization

### Buffer Sizes

Tune network buffers for your network:

```toml
[network]
send_buffer_size = 65536   # 64KB
recv_buffer_size = 65536   # 64KB
```

**Guidelines:**
- High-bandwidth networks: 128KB-256KB
- Low-latency networks: 32KB-64KB
- WAN connections: 256KB+

### Connection Pooling

Limit connections to prevent resource exhaustion:

```toml
[network]
max_connections = 1000
```

### Keepalive

Configure TCP keepalive:

```toml
[network]
tcp_keepalive = 60  # seconds
```

## State Backend Optimization

### Checkpointing

Balance checkpoint frequency:

```toml
[state.checkpoint]
enabled = true
interval = 60  # seconds
max_checkpoints = 10
```

**Guidelines:**
- Frequent checkpoints (30s): Faster recovery, higher overhead
- Moderate checkpoints (60-120s): Balanced
- Infrequent checkpoints (300s+): Lower overhead, slower recovery

### State Directory

Use fast storage for state:

```toml
[state]
state_dir = "/fast/ssd/state"  # Use SSD
```

**Storage Recommendations:**
- SSD for state directory
- Separate disk for checkpoints
- RAID 0 or 10 for performance

## Cluster Optimization

### Gossip Configuration

Tune gossip for cluster size:

```toml
[cluster.gossip]
gossip_interval = 1      # seconds
gossip_fanout = 3        # nodes per round
heartbeat_timeout = 10   # seconds
dead_timeout = 30        # seconds
```

**Guidelines:**
- Small clusters (3-5 nodes): fanout = 2-3
- Medium clusters (6-10 nodes): fanout = 3-4
- Large clusters (10+ nodes): fanout = 4-5

### Raft Configuration

Optimize Raft for your network:

```toml
[cluster.raft]
election_timeout_min = 150   # milliseconds
election_timeout_max = 300   # milliseconds
heartbeat_interval = 50      # milliseconds
```

**Guidelines:**
- Low-latency networks: Lower timeouts
- High-latency networks: Higher timeouts
- Unstable networks: Increase timeouts

### Partition Assignment

Use consistent hashing for minimal rebalancing:

```rust
let assigner = ConsistentHashAssigner::new(100);  // 100 virtual nodes
```

**Virtual Nodes:**
- More virtual nodes = better distribution
- More virtual nodes = higher memory
- Recommended: 100-200 virtual nodes

## Benchmarking

### Running Benchmarks

```bash
# Throughput benchmark
cargo bench --bench throughput

# Latency benchmark
cargo bench --bench latency
```

### Key Metrics to Monitor

1. **Throughput**: Events per second
2. **Latency**: p50, p95, p99 percentiles
3. **CPU Usage**: Per-core utilization
4. **Memory Usage**: Heap and state size
5. **Network I/O**: Bytes sent/received
6. **State Operations**: Lookup/update latency

### Example Benchmark Results

```
Throughput: 1,234,567 events/sec
Latency p50: 2.3ms
Latency p95: 5.1ms
Latency p99: 8.7ms
State lookup: 45μs
CPU usage: 85%
Memory usage: 2.3GB
```

## Troubleshooting Performance Issues

### High Latency

**Symptoms:**
- p99 latency > 10ms
- High tail latency

**Solutions:**
1. Reduce batch size
2. Increase buffer sizes
3. Check network latency
4. Profile CPU usage
5. Check for blocking operations

### Low Throughput

**Symptoms:**
- Throughput < 1M events/sec per core
- CPU not fully utilized

**Solutions:**
1. Increase parallelism
2. Increase batch size
3. Check for bottlenecks
4. Profile with `perf` or `flamegraph`
5. Optimize hot paths

### Memory Issues

**Symptoms:**
- High memory usage
- Out of memory errors
- Frequent GC pauses

**Solutions:**
1. Reduce buffer sizes
2. Enable backpressure
3. Use RocksDB for large state
4. Increase checkpoint frequency
5. Monitor state size

### Network Issues

**Symptoms:**
- High network latency
- Connection timeouts
- Low throughput

**Solutions:**
1. Increase buffer sizes
2. Tune TCP keepalive
3. Check network bandwidth
4. Reduce connection count
5. Use faster network

### State Backend Issues

**Symptoms:**
- Slow state lookups
- High disk I/O
- Checkpoint failures

**Solutions:**
1. Use SSD for state directory
2. Tune RocksDB options
3. Increase checkpoint interval
4. Monitor disk I/O
5. Consider memory backend for small state

## Performance Checklist

- [ ] Parallelism matches CPU cores
- [ ] Buffer sizes optimized for workload
- [ ] Backpressure enabled
- [ ] State backend appropriate for use case
- [ ] Checkpointing configured
- [ ] Network buffers tuned
- [ ] Gossip/Raft timeouts optimized
- [ ] Metrics collection enabled
- [ ] Benchmarks run and documented
- [ ] Performance targets met

## Advanced Optimization

### Zero-Copy Operations

Use zero-copy where possible:

```rust
// Use Arc<str> for string keys
let key = Arc::from("my_key");

// Use Bytes for binary data
let data = Bytes::from(vec![1, 2, 3]);
```

### Lock-Free Data Structures

StreamForge uses lock-free structures internally:
- `dashmap` for concurrent maps
- `crossbeam` for lock-free queues
- Atomic operations for counters

### Custom Serialization

For high-performance scenarios, consider custom serialization:

```rust
// Use bincode for fast binary serialization
let encoded = bincode::serialize(&data)?;
```

## Monitoring Performance

### Metrics Endpoint

Query metrics:

```bash
curl http://localhost:9000/metrics
```

### Key Metrics

- `streamforge_events_processed_total`: Total events
- `streamforge_events_per_second`: Current throughput
- `streamforge_latency_avg_us`: Average latency
- `streamforge_state_size_bytes`: State size

### Grafana Dashboard

Create dashboards for:
- Throughput over time
- Latency percentiles
- CPU/Memory usage
- Network I/O
- State size

## Next Steps

- See [DEPLOYMENT.md](DEPLOYMENT.md) for deployment configuration
- See [README.md](README.md) for API documentation
- Run benchmarks to establish baseline
- Profile with `perf` or `flamegraph`
- Monitor metrics in production

