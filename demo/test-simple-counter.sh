#!/bin/bash
# Quick test script for simple counter job

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

_RAW_CONFIG="${1:-"$SCRIPT_DIR/jobs/simple_counter.toml"}"
JOB_CONFIG="$(cd "$(dirname "$_RAW_CONFIG")" && pwd)/$(basename "$_RAW_CONFIG")"
OUTPUT_FILE=$(grep 'sink' "$JOB_CONFIG" | grep -oE 'path = "[^"]+"' | head -1 | sed 's/path = "//;s/"//')
OUTPUT_FILE="${OUTPUT_FILE:-/tmp/streamforge_counter_output.jsonl}"

echo "Testing: $JOB_CONFIG"

# Point all streamforge state into the project's target dir (stays inside project, easy to clean)
export STREAMFORGE_DATA_DIR="$PROJECT_ROOT/target/test-state"

# Clean up
pkill -f "simple_counter_server" 2>/dev/null || true
pkill -f "streamforge" 2>/dev/null || true
sleep 1
# Also kill anything still holding port 9001
lsof -ti:9001 | xargs kill -9 2>/dev/null || true
rm -f "$OUTPUT_FILE" /tmp/streamforge_counter_source.jsonl
rm -rf "$STREAMFORGE_DATA_DIR"

# Build
echo "Building..."
cd "$PROJECT_ROOT"
cargo build --bin streamforge
cd "$SCRIPT_DIR/load"
cargo build --bin simple_counter_server
cd "$SCRIPT_DIR"

# Start counter server
echo "Starting counter server..."
"$SCRIPT_DIR/load/target/debug/simple_counter_server" > /tmp/counter_server.log 2>&1 &
COUNTER_PID=$!
sleep 2

# Start one node
echo "Starting node..."
"$PROJECT_ROOT/target/debug/streamforge" node --config "$SCRIPT_DIR/etc/node1.toml" --daemon > /tmp/node1.log 2>&1 &
NODE_PID=$!
sleep 3

# Submit job
echo "Submitting job..."
JOB_OUTPUT=$("$PROJECT_ROOT/target/debug/streamforge" submit --config "$JOB_CONFIG" --daemon --nodes "127.0.0.1:9001" 2>&1 | tee /tmp/job_submit.log)
echo "$JOB_OUTPUT"

# Extract job ID if available
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}" | head -1)

# Wait for output
echo "Waiting 10 seconds for output..."
sleep 10

# Check output
if [ -f "$OUTPUT_FILE" ]; then
    echo ""
    echo "✅ Output file created!"
    echo "Contents:"
    cat "$OUTPUT_FILE"
else
    echo "❌ No output file found ($OUTPUT_FILE)"
    echo "Node log tail:"
    tail -20 /tmp/node1.log
fi

# Cleanup
echo ""
echo "Cleaning up..."
kill $COUNTER_PID 2>/dev/null || true
kill $NODE_PID 2>/dev/null || true
pkill -f "simple_counter_server" 2>/dev/null || true
pkill -f "streamforge" 2>/dev/null || true
lsof -ti:9001 | xargs kill -9 2>/dev/null || true

