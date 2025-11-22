#!/bin/bash
# Quick test script for simple counter job

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Testing simple counter job..."

# Clean up
pkill -f "simple_counter_server" 2>/dev/null || true
pkill -f "streamforge.*node" 2>/dev/null || true
rm -f /tmp/streamforge_counter_output.jsonl /tmp/streamforge_counter_source.jsonl
# Clean up job metadata
rm -f ~/.local/share/streamforge/jobs/*.json 2>/dev/null || true
rm -f /tmp/streamforge/jobs/*.json 2>/dev/null || true
rm -f ~/Library/Application\ Support/streamforge/jobs/*.json 2>/dev/null || true

# Build
echo "Building..."
cd "$PROJECT_ROOT"
cargo build --release --bin streamforge
cd "$SCRIPT_DIR"
cargo build --release

# Start counter server
echo "Starting counter server..."
"$SCRIPT_DIR/load/target/release/simple_counter_server" > /tmp/counter_server.log 2>&1 &
COUNTER_PID=$!
sleep 2

# Start one node
echo "Starting node..."
"$PROJECT_ROOT/target/release/streamforge" node --config "$SCRIPT_DIR/etc/node1.toml" --daemon > /tmp/node1.log 2>&1 &
NODE_PID=$!
sleep 3

# Submit job
echo "Submitting job..."
JOB_OUTPUT=$("$PROJECT_ROOT/target/release/streamforge" submit --config "$SCRIPT_DIR/jobs/simple_counter.toml" --daemon --nodes "127.0.0.1:9001" 2>&1 | tee /tmp/job_submit.log)
echo "$JOB_OUTPUT"

# Extract job ID if available
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}" | head -1)

# Wait for output
echo "Waiting 15 seconds for output..."
sleep 15

# Check output
if [ -f /tmp/streamforge_counter_output.jsonl ]; then
    echo ""
    echo "✅ Output file created!"
    echo "Contents:"
    cat /tmp/streamforge_counter_output.jsonl
else
    echo "❌ No output file found"
fi

# Cleanup
echo ""
echo "Cleaning up..."
kill $COUNTER_PID 2>/dev/null || true
kill $NODE_PID 2>/dev/null || true
pkill -f "simple_counter_server" 2>/dev/null || true
pkill -f "streamforge.*node" 2>/dev/null || true

