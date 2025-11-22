#!/bin/bash
# Prune all jobs: stop all running jobs and remove all job metadata

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STREAMFORGE="$PROJECT_ROOT/target/release/streamforge"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Pruning all jobs...${NC}"
echo ""

# Step 1: List all jobs and stop running ones
echo -e "${BLUE}Step 1: Stopping all running jobs...${NC}"
JOBS=$("$STREAMFORGE" list 2>/dev/null | grep -E "^[a-f0-9-]{36}" | awk '{print $1}' || true)

if [ -z "$JOBS" ]; then
    echo "  No jobs found."
else
    for JOB_ID in $JOBS; do
        # Check if job is running
        STATUS=$("$STREAMFORGE" list 2>/dev/null | grep "$JOB_ID" | awk '{print $3}' || echo "")
        if [ "$STATUS" = "Running" ]; then
            echo "  Stopping job: $JOB_ID"
            "$STREAMFORGE" stop "$JOB_ID" --force 2>/dev/null || echo "    Warning: Failed to stop job $JOB_ID"
        fi
    done
fi

echo ""

# Step 2: Find and remove all job metadata files
echo -e "${BLUE}Step 2: Removing all job metadata...${NC}"

# Try different possible storage locations
STORAGE_DIRS=(
    "$HOME/.local/share/streamforge/jobs"
    "/tmp/streamforge/jobs"
    "$HOME/Library/Application Support/streamforge/jobs"  # macOS
)

FOUND_DIR=""
for DIR in "${STORAGE_DIRS[@]}"; do
    if [ -d "$DIR" ]; then
        FOUND_DIR="$DIR"
        break
    fi
done

if [ -z "$FOUND_DIR" ]; then
    echo "  No job storage directory found."
else
    JOB_COUNT=$(find "$FOUND_DIR" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$JOB_COUNT" -eq 0 ]; then
        echo "  No job metadata files found."
    else
        echo "  Found $JOB_COUNT job metadata file(s) in $FOUND_DIR"
        find "$FOUND_DIR" -name "*.json" -type f -delete 2>/dev/null || true
        echo -e "  ${GREEN}✓ Removed all job metadata files${NC}"
    fi
fi

echo ""

# Step 3: Also clean up offset and sink tracking files
echo -e "${BLUE}Step 3: Cleaning up tracking data...${NC}"
for DIR in "${STORAGE_DIRS[@]}"; do
    if [ -d "$DIR" ]; then
        BASE_DIR=$(dirname "$DIR")
        OFFSETS_DIR="$BASE_DIR/offsets"
        SINKS_DIR="$BASE_DIR/sink_writes"
        
        if [ -d "$OFFSETS_DIR" ]; then
            OFFSET_COUNT=$(find "$OFFSETS_DIR" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
            if [ "$OFFSET_COUNT" -gt 0 ]; then
                find "$OFFSETS_DIR" -name "*.json" -type f -delete 2>/dev/null || true
                echo "  Removed $OFFSET_COUNT offset tracking file(s)"
            fi
        fi
        
        if [ -d "$SINKS_DIR" ]; then
            SINK_COUNT=$(find "$SINKS_DIR" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
            if [ "$SINK_COUNT" -gt 0 ]; then
                find "$SINKS_DIR" -name "*.json" -type f -delete 2>/dev/null || true
                echo "  Removed $SINK_COUNT sink tracking file(s)"
            fi
        fi
    fi
done

echo ""
echo -e "${GREEN}✓ All jobs pruned successfully${NC}"

