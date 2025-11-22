#!/bin/bash
# Test script to demonstrate how metrics sink reports data

echo "=== Metrics Sink Data Reporting Test ==="
echo ""

echo "1. Expected Event Structure from SQL Queries"
echo ""

echo "Ad Analytics Query Result Event:"
cat << 'EOF'
{
  "campaign_id": "campaign_holiday_sale",  // String → Label
  "event_count": 150,                       // Number → Metric: ad_event_count
  "total_cost": 45.23,                     // Number → Metric: ad_total_cost
  "avg_cost": 0.3015,                      // Number → Metric: ad_avg_cost
  "window_start": 1234567890,              // Metadata (ignored)
  "window_end": 1234567950                 // Metadata (ignored)
}
EOF

echo ""
echo "Price Oracle Query Result Event:"
cat << 'EOF'
{
  "symbol": "BTC/USD",                     // String → Label
  "median_price": 43000.5,                 // Number → Metric: oracle_median_price
  "mean_price": 43050.2,                   // Number → Metric: oracle_mean_price
  "min_price": 42900.0,                     // Number → Metric: oracle_min_price
  "max_price": 43200.0,                     // Number → Metric: oracle_max_price
  "total_volume": 1250.5,                   // Number → Metric: oracle_total_volume
  "num_sources": 7                          // Number → Metric: oracle_num_sources
}
EOF

echo ""
echo "2. Metrics Created"
echo ""

echo "Ad Analytics Metrics (prefix: 'ad'):"
echo "  - ad_event_count{campaign_id=\"...\"} = <value>  (gauge)"
echo "  - ad_total_cost{campaign_id=\"...\"} = <value>   (gauge)"
echo "  - ad_avg_cost{campaign_id=\"...\"} = <value>      (gauge)"
echo "  - ad_events_total{campaign_id=\"...\"} = <value> (counter, from event_count)"
echo ""

echo "Price Oracle Metrics (prefix: 'oracle'):"
echo "  - oracle_median_price{symbol=\"...\"} = <value>  (gauge)"
echo "  - oracle_mean_price{symbol=\"...\"} = <value>    (gauge)"
echo "  - oracle_min_price{symbol=\"...\"} = <value>      (gauge)"
echo "  - oracle_max_price{symbol=\"...\"} = <value>      (gauge)"
echo "  - oracle_total_volume{symbol=\"...\"} = <value>   (gauge)"
echo "  - oracle_num_sources{symbol=\"...\"} = <value>   (gauge)"
echo ""

echo "3. Conversion Rules"
echo ""
echo "  String fields → Labels"
echo "  Numeric fields → Gauge metrics"
echo "  Field 'event_count' → Also creates counter 'ad_events_total'"
echo "  Metric name = {prefix}_{field_name}"
echo ""

echo "4. Example Prometheus Output"
echo ""
echo "For Ad Analytics (/metrics endpoint):"
cat << 'EOF'
# HELP ad_event_count 
# TYPE ad_event_count gauge
ad_event_count{campaign_id="campaign_holiday_sale"} 150.0

# HELP ad_total_cost 
# TYPE ad_total_cost gauge
ad_total_cost{campaign_id="campaign_holiday_sale"} 45.23

# HELP ad_avg_cost 
# TYPE ad_avg_cost gauge
ad_avg_cost{campaign_id="campaign_holiday_sale"} 0.3015

# HELP ad_events_total 
# TYPE ad_events_total counter
ad_events_total{campaign_id="campaign_holiday_sale"} 150
EOF

echo ""
echo "5. Important Notes"
echo ""
echo "  - Metrics are only emitted when windows close (every 60 seconds)"
echo "  - If no events processed, metrics endpoint returns empty"
echo "  - Each unique label combination creates a new time series"
echo "  - Metrics use Prometheus Gauge type (can go up/down)"
echo "  - Counter is only created for 'event_count' field"
echo ""

