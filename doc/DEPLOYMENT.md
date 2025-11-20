# StreamForge Deployment Guide

This guide covers deploying StreamForge in production environments.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [Single Node Deployment](#single-node-deployment)
4. [Cluster Deployment](#cluster-deployment)
5. [Docker Deployment](#docker-deployment)
6. [Kubernetes Deployment](#kubernetes-deployment)
7. [Monitoring and Metrics](#monitoring-and-metrics)
8. [Performance Tuning](#performance-tuning)

## Quick Start

### Prerequisites

- Rust 1.70+ (for building from source)
- Linux/macOS (Windows support coming soon)
- 2GB+ RAM recommended
- Network connectivity for cluster mode

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/streamforge.git
cd streamforge

# Build from source
cargo build --release

# Or install via cargo
cargo install --path .
```

## Configuration

StreamForge supports multiple configuration methods:

### 1. Default Configuration

```rust
use streamforge::config::Config;

let config = Config::default();
```

### 2. Builder Pattern

```rust
use streamforge::config::ConfigBuilder;

let config = ConfigBuilder::new()
    .with_parallelism(8)
    .with_buffer_size(2048)
    .with_cluster_bind_address("0.0.0.0:9000".to_string())
    .build();
```

### 3. Environment Variables

```bash
export STREAMFORGE_PARALLELISM=16
export STREAMFORGE_BUFFER_SIZE=4096
```

### 4. TOML Configuration File

Create `streamforge.toml`:

```toml
[processing]
parallelism = 8
buffer_size = 2048
batch_size = 1000
backpressure_enabled = true

[cluster]
bind_address = "0.0.0.0:9000"
seed_nodes = ["127.0.0.1:9001", "127.0.0.1:9002"]

[cluster.gossip]
gossip_interval = 1
gossip_fanout = 3
heartbeat_timeout = 10
dead_timeout = 30

[cluster.raft]
election_timeout_min = 150
election_timeout_max = 300
heartbeat_interval = 50

[state]
backend_type = "RocksDB"
state_dir = "./data/state"

[state.checkpoint]
enabled = true
interval = 60
checkpoint_dir = "./data/checkpoints"
max_checkpoints = 10

[network]
tcp_keepalive = 60
connection_timeout = 30
max_connections = 1000
send_buffer_size = 65536
recv_buffer_size = 65536

[metrics]
enabled = true
export_interval = 10
```

Load from file:

```rust
let config = Config::from_file("streamforge.toml")?;
```

## Single Node Deployment

### Basic Setup

```bash
# Run with default configuration
./target/release/streamforge

# Run with custom config file
./target/release/streamforge --config streamforge.toml

# Run with environment variables
STREAMFORGE_PARALLELISM=8 ./target/release/streamforge
```

### Systemd Service

Create `/etc/systemd/system/streamforge.service`:

```ini
[Unit]
Description=StreamForge Stream Processing Engine
After=network.target

[Service]
Type=simple
User=streamforge
WorkingDirectory=/opt/streamforge
ExecStart=/opt/streamforge/streamforge --config /etc/streamforge/config.toml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable streamforge
sudo systemctl start streamforge
sudo systemctl status streamforge
```

## Cluster Deployment

### 3-Node Cluster Example

**Node 1** (`streamforge.toml`):

```toml
[cluster]
bind_address = "192.168.1.10:9000"
seed_nodes = ["192.168.1.11:9000", "192.168.1.12:9000"]
```

**Node 2** (`streamforge.toml`):

```toml
[cluster]
bind_address = "192.168.1.11:9000"
seed_nodes = ["192.168.1.10:9000", "192.168.1.12:9000"]
```

**Node 3** (`streamforge.toml`):

```toml
[cluster]
bind_address = "192.168.1.12:9000"
seed_nodes = ["192.168.1.10:9000", "192.168.1.11:9000"]
```

Start all nodes:

```bash
# Node 1
./streamforge --config node1.toml

# Node 2
./streamforge --config node2.toml

# Node 3
./streamforge --config node3.toml
```

## Docker Deployment

### Dockerfile

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/streamforge /usr/local/bin/
EXPOSE 9000
CMD ["streamforge"]
```

### Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  streamforge-1:
    build: .
    ports:
      - "9001:9000"
    volumes:
      - ./config/node1.toml:/etc/streamforge/config.toml
      - ./data/node1:/data
    command: ["streamforge", "--config", "/etc/streamforge/config.toml"]

  streamforge-2:
    build: .
    ports:
      - "9002:9000"
    volumes:
      - ./config/node2.toml:/etc/streamforge/config.toml
      - ./data/node2:/data
    command: ["streamforge", "--config", "/etc/streamforge/config.toml"]

  streamforge-3:
    build: .
    ports:
      - "9003:9000"
    volumes:
      - ./config/node3.toml:/etc/streamforge/config.toml
      - ./data/node3:/data
    command: ["streamforge", "--config", "/etc/streamforge/config.toml"]
```

Deploy:

```bash
docker-compose up -d
```

## Kubernetes Deployment

### Deployment YAML

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: streamforge
spec:
  serviceName: streamforge
  replicas: 3
  selector:
    matchLabels:
      app: streamforge
  template:
    metadata:
      labels:
        app: streamforge
    spec:
      containers:
      - name: streamforge
        image: streamforge:latest
        ports:
        - containerPort: 9000
        volumeMounts:
        - name: config
          mountPath: /etc/streamforge
        - name: data
          mountPath: /data
        env:
        - name: STREAMFORGE_PARALLELISM
          value: "8"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: streamforge
spec:
  clusterIP: None
  selector:
    app: streamforge
  ports:
  - port: 9000
    targetPort: 9000
```

## Monitoring and Metrics

### Metrics Endpoint

StreamForge exposes metrics via HTTP endpoint (when enabled):

```bash
curl http://localhost:9000/metrics
```

### Key Metrics

- `streamforge_events_processed_total`: Total events processed
- `streamforge_events_per_second`: Events per second
- `streamforge_latency_p50`: 50th percentile latency
- `streamforge_latency_p99`: 99th percentile latency
- `streamforge_state_size_bytes`: State size in bytes
- `streamforge_cluster_nodes`: Number of nodes in cluster

## Performance Tuning

### CPU

- Set `parallelism` to number of CPU cores
- Use CPU affinity for critical processes

### Memory

- Increase `buffer_size` for higher throughput
- Monitor state size and adjust checkpointing interval

### Network

- Tune `send_buffer_size` and `recv_buffer_size` for network conditions
- Adjust `tcp_keepalive` based on network stability

### State Backend

- Use RocksDB for persistent state
- Configure checkpointing interval based on recovery requirements
- Monitor checkpoint size and retention

## Troubleshooting

### Common Issues

1. **High Latency**: Increase buffer sizes, check network conditions
2. **Memory Issues**: Reduce parallelism or buffer sizes
3. **Cluster Split-Brain**: Check network connectivity, adjust Raft timeouts
4. **State Corruption**: Restore from checkpoint, check disk health

### Logs

Enable debug logging:

```bash
RUST_LOG=debug ./streamforge
```

## Security Considerations

- Use TLS for cluster communication (coming soon)
- Restrict network access to cluster ports
- Use secrets management for sensitive configuration
- Regular security updates

## Next Steps

- See [README.md](README.md) for API documentation
- See [CLUSTER_GUIDE.md](CLUSTER_GUIDE.md) for cluster architecture
- See examples/ for code examples

