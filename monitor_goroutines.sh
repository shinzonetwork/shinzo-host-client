#!/bin/bash
INTERVAL=${1:-10}  # Default: check every 10 seconds
LOG_FILE="goroutine_monitor_$(date +%Y%m%d_%H%M%S).log"

echo "Press Ctrl+C to stop" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Timestamp | Total Goroutines | Memory Allocated" | tee -a "$LOG_FILE"

while true; do
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
    
    PPROF_OUTPUT=$(curl -s 'http://localhost:6060/debug/pprof/goroutine?debug=1' | head -20)
    TOTAL=$(echo "$PPROF_OUTPUT" | grep "goroutine profile: total" | awk '{print $4}')    
    TOTAL=${TOTAL:-0}

    curl -s http://localhost:6060/debug/pprof/heap > /tmp/heap.pprof
    INUSE_MB=$(go tool pprof -top -sample_index=inuse_space /tmp/heap.pprof \
        | grep "Showing nodes accounting for" \
        | awk '{print $5}' \
        | sed 's/,//')
    INUSE_MB=${INUSE_MB:-0}
    
    printf "%s - %d - %s\n" "$TIMESTAMP" "$TOTAL" "$INUSE_MB"| tee -a "$LOG_FILE"
    
    sleep "$INTERVAL"
done
