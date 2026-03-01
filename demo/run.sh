#!/bin/bash
# StreamForge Demo Orchestration Script
# Starts 3 cluster nodes, load generators, submits jobs, and waits for Ctrl+C

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$SCRIPT_DIR"
PROJECT_ROOT="$(dirname "$DEMO_DIR")"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# PIDs storage
PIDS=()

cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down all services...${NC}"
    
    # Kill all background processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    
    # Kill by process name as fallback
    pkill -f "ad_event_server" || true
    pkill -f "price_event_server" || true
    pkill -f "simple_counter_server" || true
    pkill -f "streamforge.*node" || true
    pkill -f "streamforge.*submit" || true
    
    # Stop monitoring
    cd "$DEMO_DIR"
    if command -v docker-compose > /dev/null 2>&1 || command -v docker > /dev/null 2>&1; then
        docker-compose down > /dev/null 2>&1 || docker compose down > /dev/null 2>&1
    fi
    cd "$PROJECT_ROOT"
    
    sleep 2
    echo -e "${GREEN}All services stopped.${NC}"
    exit 0
}

trap cleanup INT TERM

# Cleanup any existing processes first
echo -e "${YELLOW}Cleaning up any existing processes...${NC}"
pkill -f "ad_event_server" 2>/dev/null || true
pkill -f "price_event_server" 2>/dev/null || true
pkill -f "simple_counter_server" 2>/dev/null || true
pkill -f "streamforge.*node" 2>/dev/null || true
pkill -f "streamforge.*submit" 2>/dev/null || true
sleep 1

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}StreamForge Demo${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Step 1: Build everything
echo -e "${BLUE}Step 1: Building binaries...${NC}"
cd "$PROJECT_ROOT"
cargo build --release --bin streamforge || {
    echo -e "${RED}Failed to build streamforge${NC}"
    exit 1
}

cd "$DEMO_DIR"
cargo build --release --bin simple_counter_server || {
    echo -e "${RED}Failed to build simple_counter_server${NC}"
    exit 1
}
cd "$PROJECT_ROOT"

# Commented out: Other demo load generators
# cd "$DEMO_DIR/load"
# cargo build --release || {
#     echo -e "${RED}Failed to build load generators${NC}"
#     exit 1
# }
cd "$PROJECT_ROOT"

echo ""

# Step 2: Start load generators
echo -e "${BLUE}Step 2: Starting load generators...${NC}"

cd "$DEMO_DIR/load"
./target/release/ad_event_server > /dev/null 2>&1 &
AD_PID=$!
PIDS+=($AD_PID)
echo "  ✓ Ad Event Server (PID: $AD_PID)"
 
./target/release/price_event_server > /dev/null 2>&1 &
PRICE_PID=$!
PIDS+=($PRICE_PID)
echo "  ✓ Price Event Server (PID: $PRICE_PID)"

# Start simple counter server
cd "$DEMO_DIR"
"$DEMO_DIR/target/release/simple_counter_server" > /tmp/counter_server.log 2>&1 &
COUNTER_PID=$!
PIDS+=($COUNTER_PID)
echo "  ✓ Counter Server (PID: $COUNTER_PID)"

sleep 2
cd "$PROJECT_ROOT"

# Step 3: Start cluster nodes
echo ""
echo -e "${BLUE}Step 3: Starting cluster nodes...${NC}"

# Start nodes as background processes
for i in 1 2 3; do
    "$PROJECT_ROOT/target/release/streamforge" node --config "$DEMO_DIR/etc/node$i.toml" --daemon > /tmp/node$i.log 2>&1 &
    NODE_PID=$!
    PIDS+=($NODE_PID)
    echo "  ✓ Node $i (PID: $NODE_PID)"
    sleep 1
done

# Wait for nodes to be ready (they need to bind and start RPC servers)
echo -e "${YELLOW}Waiting for nodes to be ready...${NC}"
sleep 3

# Check if nodes are ready
"$PROJECT_ROOT/target/release/streamforge" ready --nodes "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" || {
    echo -e "${YELLOW}Nodes not ready yet, waiting a bit more...${NC}"
    sleep 5
    "$PROJECT_ROOT/target/release/streamforge" ready --nodes "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" || {
        echo -e "${RED}Warning: Some nodes may not be ready, but continuing anyway...${NC}"
    }
}

# Step 4: Start monitoring
echo ""
echo -e "${BLUE}Step 4: Starting monitoring stack...${NC}"
cd "$DEMO_DIR"
if command -v docker-compose > /dev/null 2>&1 || command -v docker > /dev/null 2>&1; then
    docker-compose up -d > /dev/null 2>&1 || docker compose up -d > /dev/null 2>&1
    echo "  ✓ Prometheus and Grafana started"
else
    echo -e "${YELLOW}  ⚠ Docker not found, skipping monitoring${NC}"
fi
cd "$PROJECT_ROOT"

# Step 5: Submit jobs
echo ""
echo -e "${BLUE}Step 5: Submitting jobs...${NC}"

"$PROJECT_ROOT/target/release/streamforge" submit --config "$DEMO_DIR/jobs/ad_analytics.toml" --daemon --nodes "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" 2>&1 | tee /tmp/ad_analytics_submit.log &
JOB1_PID=$!
PIDS+=($JOB1_PID)
echo "  ✓ Ad Analytics job submitted (PID: $JOB1_PID)"

sleep 2

"$PROJECT_ROOT/target/release/streamforge" submit --config "$DEMO_DIR/jobs/price_oracle.toml" --daemon --nodes "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" 2>&1 | tee /tmp/price_oracle_submit.log &
JOB2_PID=$!
PIDS+=($JOB2_PID)
echo "  ✓ Price Oracle job submitted (PID: $JOB2_PID)"

# Submit simple counter job
"$PROJECT_ROOT/target/release/streamforge" submit --config "$DEMO_DIR/jobs/simple_counter.toml" --daemon --nodes "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" 2>&1 | tee /tmp/simple_counter_submit.log &
COUNTER_JOB_PID=$!
PIDS+=($COUNTER_JOB_PID)
echo "  ✓ Simple Counter job submitted (PID: $COUNTER_JOB_PID)"

sleep 5
echo -e "${YELLOW}Waiting for jobs to initialize and start processing...${NC}"
echo -e "${YELLOW}Note: Jobs process events in 60-second windows, metrics may take up to 60s to appear${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}All services running!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Event Servers:"
echo "  📡 Counter:      http://127.0.0.1:8093/counter"
echo "  📡 Ad Events:    http://127.0.0.1:8091/event"
echo "  📡 Price Events: http://127.0.0.1:8092/event"
echo ""
echo "Output File:"
echo "  📄 Counter Output: /tmp/streamforge_counter_output.jsonl"
echo ""
echo "Metrics:"
echo "  📊 Ad Analytics: http://127.0.0.1:9100/metrics"
echo "  📊 Price Oracle: http://127.0.0.1:9101/metrics"
echo ""
echo "Cluster Nodes:"
echo "  🖥️  Node 1: 127.0.0.1:9001 (metrics: 127.0.0.1:9081)"
echo "  🖥️  Node 2: 127.0.0.1:9002 (metrics: 127.0.0.1:9082)"
echo "  🖥️  Node 3: 127.0.0.1:9003 (metrics: 127.0.0.1:9083)"
echo ""
echo "Monitoring:"
echo "  📈 Prometheus: http://localhost:9090"
echo "  📈 Grafana:    http://localhost:3000 (admin/admin)"
echo ""
echo -e "${YELLOW}Troubleshooting:${NC}"
echo "  - Check job status: streamforge list --nodes \"127.0.0.1:9001\""
echo "  - Check output file: tail -f /tmp/streamforge_counter_output.jsonl"
echo "  - Check logs: /tmp/simple_counter_submit.log, /tmp/node1.log"
echo "  - Job uses 10-second windows, output should appear every 10 seconds"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
echo ""

# Wait for interrupt (wait for background jobs or signal)
# The submit commands exit immediately after submitting, so we wait for the signal
while true; do
    sleep 1
done
