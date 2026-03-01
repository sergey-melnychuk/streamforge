#!/bin/bash
# Test script for price oracle job (GROUP BY + MEDIAN/AVG/MIN/MAX/SUM/COUNT)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Testing price oracle job (GROUP BY + 6 aggregations)..."

export STREAMFORGE_DATA_DIR="$PROJECT_ROOT/target/test-state"

# Clean up
pkill -f "price_event_server" 2>/dev/null || true
pkill -f "streamforge" 2>/dev/null || true
sleep 1
lsof -ti:9001 | xargs kill -9 2>/dev/null || true
rm -f /tmp/streamforge_oracle_output.jsonl
rm -rf "$STREAMFORGE_DATA_DIR"

# Build
echo "Building..."
cd "$PROJECT_ROOT"
cargo build --bin streamforge
cd "$SCRIPT_DIR/load"
cargo build --bin price_event_server
cd "$SCRIPT_DIR"

# Start price event server
echo "Starting price event server..."
"$SCRIPT_DIR/load/target/debug/price_event_server" > /tmp/price_event_server.log 2>&1 &
PRICE_PID=$!
sleep 2

# Start one node
echo "Starting node..."
"$PROJECT_ROOT/target/debug/streamforge" node --config "$SCRIPT_DIR/etc/node1.toml" --daemon > /tmp/node1.log 2>&1 &
NODE_PID=$!
sleep 3

# Submit job
echo "Submitting job..."
JOB_OUTPUT=$("$PROJECT_ROOT/target/debug/streamforge" submit --config "$SCRIPT_DIR/jobs/price_oracle.toml" --daemon --nodes "127.0.0.1:9001" 2>&1 | tee /tmp/job_submit.log)
echo "$JOB_OUTPUT"

# Wait for output
echo "Waiting 10 seconds for output..."
sleep 10

# Check output
if [ -f /tmp/streamforge_oracle_output.jsonl ]; then
    echo ""
    echo "✅ Output file created!"
    echo "Contents:"
    cat /tmp/streamforge_oracle_output.jsonl
else
    echo "❌ No output file found"
    echo "Node log tail:"
    tail -20 /tmp/node1.log
fi

# Cleanup
echo ""
echo "Cleaning up..."
kill $PRICE_PID 2>/dev/null || true
kill $NODE_PID 2>/dev/null || true
pkill -f "price_event_server" 2>/dev/null || true
pkill -f "streamforge" 2>/dev/null || true
lsof -ti:9001 | xargs kill -9 2>/dev/null || true
