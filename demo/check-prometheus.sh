#!/bin/bash
# Script to check if ad and price data is in Prometheus

echo "=== Prometheus Status ==="
echo ""

# Check if Prometheus is running
if curl -s http://127.0.0.1:9090/-/healthy > /dev/null 2>&1; then
    echo "✓ Prometheus is running"
else
    echo "✗ Prometheus is NOT running"
    exit 1
fi

echo ""
echo "=== Metrics Endpoints Status ==="
echo ""

# Check ad analytics metrics endpoint
echo "1. Ad Analytics Metrics (http://127.0.0.1:9100/metrics):"
if curl -s -f http://127.0.0.1:9100/metrics > /dev/null 2>&1; then
    echo "   ✓ Endpoint is accessible"
    METRICS=$(curl -s http://127.0.0.1:9100/metrics | grep -c "^ad_" 2>/dev/null || echo "0")
    if [ "$METRICS" -gt 0 ] && [ -n "$METRICS" ]; then
        echo "   ✓ Found $METRICS ad_* metrics"
        echo "   Sample metrics:"
        curl -s http://127.0.0.1:9100/metrics | grep "^ad_" | head -5 | sed 's/^/      /'
    else
        echo "   ⚠ Endpoint accessible but NO ad_* metrics found"
        echo "   (Jobs may not be running or no data processed yet)"
    fi
else
    echo "   ✗ Endpoint is NOT accessible"
fi
echo ""

# Check price oracle metrics endpoint
echo "2. Price Oracle Metrics (http://127.0.0.1:9101/metrics):"
if curl -s -f http://127.0.0.1:9101/metrics > /dev/null 2>&1; then
    echo "   ✓ Endpoint is accessible"
    METRICS=$(curl -s http://127.0.0.1:9101/metrics | grep -c "^oracle_" 2>/dev/null || echo "0")
    if [ "$METRICS" -gt 0 ] && [ -n "$METRICS" ]; then
        echo "   ✓ Found $METRICS oracle_* metrics"
        echo "   Sample metrics:"
        curl -s http://127.0.0.1:9101/metrics | grep "^oracle_" | head -5 | sed 's/^/      /'
    else
        echo "   ⚠ Endpoint accessible but NO oracle_* metrics found"
        echo "   (Jobs may not be running or no data processed yet)"
    fi
else
    echo "   ✗ Endpoint is NOT accessible (connection refused)"
    echo "   (Job may not be running)"
fi
echo ""

echo "=== Prometheus Targets Status ==="
echo ""

# Check Prometheus targets
TARGETS=$(curl -s "http://127.0.0.1:9090/api/v1/targets" 2>/dev/null)

echo "3. Ad Analytics Target:"
AD_STATUS=$(echo "$TARGETS" | python3 -c "import sys, json; data = json.load(sys.stdin); targets = data.get('data', {}).get('activeTargets', []); [print(t['health']) for t in targets if 'ad-analytics' in t['labels'].get('job', '')]" 2>/dev/null)
if [ "$AD_STATUS" = "up" ]; then
    echo "   ✓ Target is UP"
else
    echo "   ✗ Target is $AD_STATUS"
fi

echo ""
echo "4. Price Oracle Target:"
PRICE_STATUS=$(echo "$TARGETS" | python3 -c "import sys, json; data = json.load(sys.stdin); targets = data.get('data', {}).get('activeTargets', []); [print(t['health']) for t in targets if 'price-oracle' in t['labels'].get('job', '')]" 2>/dev/null)
if [ "$PRICE_STATUS" = "up" ]; then
    echo "   ✓ Target is UP"
else
    echo "   ✗ Target is $PRICE_STATUS"
fi

echo ""
echo "=== Prometheus Metrics Query ==="
echo ""

# Query Prometheus for ad metrics
echo "5. Querying Prometheus for ad_* metrics:"
AD_QUERY=$(curl -s "http://127.0.0.1:9090/api/v1/query?query=ad_impressions_total" 2>/dev/null)
AD_COUNT=$(echo "$AD_QUERY" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data.get('data', {}).get('result', [])))" 2>/dev/null || echo "0")
if [ "$AD_COUNT" -gt 0 ]; then
    echo "   ✓ Found $AD_COUNT ad_impressions_total time series"
    echo "$AD_QUERY" | python3 -m json.tool 2>/dev/null | head -15 | sed 's/^/      /'
else
    echo "   ⚠ No ad_impressions_total metrics found in Prometheus"
fi

echo ""
echo "6. Querying Prometheus for oracle_* metrics:"
ORACLE_QUERY=$(curl -s "http://127.0.0.1:9090/api/v1/query?query=oracle_price_median" 2>/dev/null)
ORACLE_COUNT=$(echo "$ORACLE_QUERY" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data.get('data', {}).get('result', [])))" 2>/dev/null || echo "0")
if [ "$ORACLE_COUNT" -gt 0 ]; then
    echo "   ✓ Found $ORACLE_COUNT oracle_price_median time series"
    echo "$ORACLE_QUERY" | python3 -m json.tool 2>/dev/null | head -15 | sed 's/^/      /'
else
    echo "   ⚠ No oracle_price_median metrics found in Prometheus"
fi

echo ""
echo "=== Summary ==="
echo ""
echo "To see data in Prometheus:"
echo "1. Ensure jobs are running: ./demo/run.sh (or manually submit jobs)"
echo "2. Wait 60+ seconds for windowed aggregations to emit metrics"
echo "3. Check job logs: tail -f /tmp/ad_analytics_submit.log"
echo "4. Verify sources are polling: ./demo/check-sources.sh"
echo ""
echo "Prometheus UI: http://127.0.0.1:9090"
echo "Grafana UI: http://127.0.0.1:3000 (if running)"

