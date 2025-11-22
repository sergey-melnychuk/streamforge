#!/bin/bash
# Script to check if sources are being polled and load generators are responding

echo "=== Checking Load Generators ==="
echo ""

# Check ad event server
echo "1. Ad Event Server (http://127.0.0.1:8091/event):"
if curl -s -f http://127.0.0.1:8091/event > /dev/null 2>&1; then
    echo "   ✓ Server is responding"
    curl -s http://127.0.0.1:8091/event | jq . 2>/dev/null || curl -s http://127.0.0.1:8091/event
else
    echo "   ✗ Server is NOT responding (connection refused or timeout)"
fi
echo ""

# Check price event server
echo "2. Price Event Server (http://127.0.0.1:8092/event):"
if curl -s -f http://127.0.0.1:8092/event > /dev/null 2>&1; then
    echo "   ✓ Server is responding"
    curl -s http://127.0.0.1:8092/event | jq . 2>/dev/null || curl -s http://127.0.0.1:8092/event
else
    echo "   ✗ Server is NOT responding (connection refused or timeout)"
fi
echo ""

echo "=== Checking Job Metrics Endpoints ==="
echo ""

# Check ad analytics metrics
echo "3. Ad Analytics Metrics (http://127.0.0.1:9100/metrics):"
if curl -s -f http://127.0.0.1:9100/metrics > /dev/null 2>&1; then
    echo "   ✓ Metrics endpoint is responding"
    METRICS_COUNT=$(curl -s http://127.0.0.1:9100/metrics | grep -c "^ad_" || echo "0")
    echo "   Found $METRICS_COUNT ad_* metrics"
    curl -s http://127.0.0.1:9100/metrics | grep "^ad_" | head -5
else
    echo "   ✗ Metrics endpoint is NOT responding"
fi
echo ""

# Check price oracle metrics
echo "4. Price Oracle Metrics (http://127.0.0.1:9101/metrics):"
if curl -s -f http://127.0.0.1:9101/metrics > /dev/null 2>&1; then
    echo "   ✓ Metrics endpoint is responding"
    METRICS_COUNT=$(curl -s http://127.0.0.1:9101/metrics | grep -c "^oracle_" || echo "0")
    echo "   Found $METRICS_COUNT oracle_* metrics"
    curl -s http://127.0.0.1:9101/metrics | grep "^oracle_" | head -5
else
    echo "   ✗ Metrics endpoint is NOT responding"
fi
echo ""

echo "=== Checking Running Jobs ==="
echo ""
echo "To check if jobs are running, look for 'streamforge submit' processes:"
ps aux | grep "streamforge submit" | grep -v grep || echo "   No job processes found"
echo ""

echo "=== Tips ==="
echo "1. Enable debug logging: RUST_LOG=debug streamforge submit ..."
echo "2. Check job logs in /tmp/ad_analytics_submit.log and /tmp/price_oracle_submit.log"
echo "3. Verify load generators are running: ps aux | grep 'ad_event_server\\|price_event_server'"
echo "4. Wait 60+ seconds for windowed aggregations to emit metrics"

