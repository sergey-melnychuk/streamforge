# StreamForge Demo

**Standalone demo** that uses the StreamForge binary.

## Quick Start

```bash
# 1. Build StreamForge binary (from project root)
cd /path/to/streamforge
cargo build --release --bin streamforge

# 2. Run demo
cd demo
./run.sh
```

Press `Ctrl+C` to stop all services.

## Structure

```
demo/
├── load/                  # Load generator binaries
│   ├── Cargo.toml
│   └── src/
│       ├── ad_event_server.rs
│       └── price_event_server.rs
├── jobs/                  # Job configuration files
│   ├── ad_analytics.toml
│   └── price_oracle.toml
├── etc/                   # Configuration files (for Docker)
│   ├── node1.toml         # Cluster node 1 config
│   ├── node2.toml         # Cluster node 2 config
│   ├── node3.toml         # Cluster node 3 config
│   ├── prometheus/        # Prometheus configs
│   └── grafana/           # Grafana configs
├── docker-compose.yml     # Monitoring stack
├── run.sh                 # Single orchestration script
└── README.md
```

## What `run.sh` Does

1. **Builds binaries**: StreamForge and load generators
2. **Starts load generators**: Ad and price event servers
3. **Starts cluster nodes**: 3-node cluster (ports 9001-9003)
4. **Submits jobs**: Ad analytics and price oracle jobs
5. **Waits for Ctrl+C**: Keeps everything running
6. **Cleans up**: Stops all services on exit

## Event Servers

- **Ad Event Server**: `http://127.0.0.1:8091/event`
- **Price Event Server**: `http://127.0.0.1:8092/event`

## Job Configurations

### Ad Analytics

- **Source**: HTTP polling from `http://127.0.0.1:8091/event`
- **SQL**: Groups by campaign_id, calculates COUNT, SUM, AVG
- **Sink**: Prometheus metrics at `http://127.0.0.1:9090/metrics`

### Price Oracle

- **Source**: HTTP polling from `http://127.0.0.1:8092/event`
- **SQL**: Groups by symbol, calculates MEDIAN, AVG, MIN, MAX
- **Sink**: Prometheus metrics at `http://127.0.0.1:9091/metrics`

## Metrics Endpoints

- **Ad Analytics**: `http://127.0.0.1:9090/metrics`
- **Price Oracle**: `http://127.0.0.1:9091/metrics`

## Monitoring

```bash
docker-compose up -d
```

Starts Prometheus and Grafana with pre-configured dashboards.
