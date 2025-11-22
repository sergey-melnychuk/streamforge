#!/bin/bash
# Script to check job logs and status

echo "=== Job Logs Checker ==="
echo ""

# Find all job log files
LOG_DIR="/tmp"
LOG_FILES=$(ls -t ${LOG_DIR}/streamforge_job_*.log 2>/dev/null)

if [ -z "$LOG_FILES" ]; then
    echo "⚠ No job log files found in ${LOG_DIR}"
    echo ""
    echo "Job logs are written to: /tmp/streamforge_job_{name}_{id}.log"
    echo ""
    echo "To see logs, ensure:"
    echo "1. Jobs are running (check with: ps aux | grep streamforge)"
    echo "2. RUST_LOG environment variable is set (e.g., RUST_LOG=info)"
    exit 0
fi

echo "Found job log files:"
echo "$LOG_FILES" | while read -r logfile; do
    if [ -n "$logfile" ]; then
        echo "  📄 $(basename $logfile)"
    fi
done
echo ""

# Show recent logs from each file
for logfile in $LOG_FILES; do
    if [ -f "$logfile" ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "📄 $(basename $logfile)"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        tail -20 "$logfile"
        echo ""
    fi
done

echo ""
echo "To follow logs in real-time:"
echo "  tail -f /tmp/streamforge_job_*.log"
echo ""
echo "To see all logs for a specific job:"
echo "  cat /tmp/streamforge_job_{name}_{id}.log"

