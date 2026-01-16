#!/bin/bash
INTERVAL=${1:-10}  # Default: check every 10 seconds
PORT=${2:-6060}    # Default: pprof port
LOG_FILE="goroutine_monitor_$(date +%Y%m%d_%H%M%S).log"

echo "Monitoring localhost:$PORT every ${INTERVAL}s" | tee -a "$LOG_FILE"
echo "Press Ctrl+C to stop" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Timestamp | Goroutines | Memory | CPU% | Top Functions" | tee -a "$LOG_FILE"

while true; do
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

    PPROF_OUTPUT=$(curl -s "http://localhost:$PORT/debug/pprof/goroutine?debug=1" | head -20)
    TOTAL=$(echo "$PPROF_OUTPUT" | grep "goroutine profile: total" | awk '{print $4}')
    TOTAL=${TOTAL:-0}

    curl -s "http://localhost:$PORT/debug/pprof/heap" > /tmp/heap.pprof
    INUSE_MB=$(go tool pprof -top -sample_index=inuse_space /tmp/heap.pprof 2>/dev/null \
        | grep "Showing nodes accounting for" \
        | awk '{print $5}' \
        | sed 's/,//')
    INUSE_MB=${INUSE_MB:-0}

    curl -s "http://localhost:$PORT/debug/pprof/profile?seconds=2" > /tmp/cpu.pprof 2>/dev/null
    if [ -s /tmp/cpu.pprof ]; then
        PPROF_TOP=$(go tool pprof -top -nodecount=20 /tmp/cpu.pprof 2>/dev/null)

        CPU_PCT=$(echo "$PPROF_TOP" | grep "Total samples" | sed -E 's/.*\(([0-9.]+)%\).*/\1/')
        CPU_PCT=${CPU_PCT:-"N/A"}

        CPU_TOP=$(echo "$PPROF_TOP" | grep -E "^\s+[0-9]" | awk '{print $NF}' | grep -v "^runtime\." | grep -v "^syscall\." | head -3 | tr '\n' ',' | sed 's/,$//')
        if [ -z "$CPU_TOP" ]; then
            CPU_TOP=$(echo "$PPROF_TOP" | grep -E "^\s+[0-9]" | head -3 | awk '{print $NF}' | tr '\n' ',' | sed 's/,$//')
        fi
        CPU_TOP=${CPU_TOP:-"N/A"}
    else
        CPU_PCT="N/A"
        CPU_TOP="N/A"
    fi

    printf "%s - %d - %s - %s%% - %s\n" "$TIMESTAMP" "$TOTAL" "$INUSE_MB" "$CPU_PCT" "$CPU_TOP" | tee -a "$LOG_FILE"

    sleep "$INTERVAL"
done
