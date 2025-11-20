# StreamForge MVP Demo

This demo showcases all MVP features of StreamForge:

1. **File Source & Sink** - Read from files, process, and write to files
2. **Distributed Execution** - Execute operators across cluster nodes
3. **Stateful Operators** - Operators that maintain state using state backends

## Running the Demo

### Run All Demos

```bash
cd demo
cargo run
```

### Run Individual Demos

```bash
# File processing demo
cargo run -- file

# Distributed execution demo
cargo run -- distributed

# Stateful operators demo
cargo run -- stateful

# Production workload demo (NEW!)
cargo run -- production
```

## Demo Features

### 1. File Source & Sink Demo

Demonstrates:
- Reading JSON events from a file
- Filtering events (amount > 100)
- Transforming events (doubling amounts)
- Writing processed events to a file

**What it shows:**
- File source creation and usage
- Stream processing with filter and map operators
- File sink for output

### 2. Distributed Execution Demo

Demonstrates:
- Setting up a 3-node cluster
- Partition assignment across nodes
- Distributed operator execution
- Event routing based on keys

**What it shows:**
- Cluster membership management
- Partition assignment using consistent hashing
- Distributed context and executor
- Local partition processing

### 3. Stateful Operators Demo

Demonstrates:
- Creating state backends (in-memory)
- Using operator state with namespaces
- Stateful operator implementation
- State storage and retrieval

**What it shows:**
- State backend abstraction
- OperatorState helper for namespaced state
- StatefulOperator trait usage
- CountOperator example

### 4. Production Workload Demo (Single Node)

Demonstrates:
- Real-time e-commerce order processing
- Windowed aggregations (revenue per minute, orders per category)
- Stateful analytics (customer lifetime value, top products)
- Watermark-based event-time processing
- Backpressure detection and flow control
- Metrics collection and monitoring
- High-throughput processing (900+ orders in 2 minutes)

**What it shows:**
- Production-like workload simulation
- Multiple features working together
- Real-time analytics dashboard
- Performance metrics and monitoring

### 5. Distributed Production Workload Demo (Multi-Node) ⭐ NEW!

Demonstrates:
- **Multi-node cluster setup** (3 nodes)
- **Partition-based event routing** (events distributed across nodes)
- **Distributed execution context** (coordinator node managing cluster)
- **Local vs remote processing** (shows which events stay local vs routed)
- **Cluster membership and coordination**
- **Distributed analytics aggregation**
- **High-throughput distributed processing** (450+ orders across cluster)

**What it shows:**
- **True distributed execution**: Events are partitioned and routed to different nodes
- **Partition assignment**: Shows how partitions are assigned to nodes (e.g., Node 1: [2, 5, 7, 8])
- **Event distribution**: Tracks local vs remote event routing
- **Cluster coordination**: Multi-node cluster membership and coordination
- **Production-ready distribution**: Same logic used in production deployments

**Key Differences from Single-Node Demo:**
- Events are partitioned by key (customer_id) and routed to appropriate nodes
- Only events for local partitions are processed locally
- Other events are routed to remote nodes (would use RPC in full production)
- Shows realistic distribution: ~30% local, ~70% remote (typical for 3-node cluster)

## MVP Capabilities Showcased

✅ **End-to-End Processing**: Read → Process → Write
✅ **Stream Operators**: Filter, map, and other transformations
✅ **Distributed Processing**: Multi-node cluster execution
✅ **State Management**: Persistent state for operators
✅ **Partition Assignment**: Automatic data distribution
✅ **Event-Time Processing**: Watermark-based windowing
✅ **Backpressure Control**: Flow control and throttling
✅ **Metrics & Monitoring**: Real-time performance tracking
✅ **Production Workloads**: Real-world use case simulation
✅ **Distributed Execution**: Multi-node cluster processing
✅ **Partition-Based Routing**: Events distributed across cluster nodes
✅ **Cluster Coordination**: Multi-node membership and coordination

## Next Steps

After running the demo, you can:
1. Explore the code to understand how features work
2. Modify the demos to test different scenarios
3. Integrate StreamForge into your own projects
4. Contribute improvements and new features

