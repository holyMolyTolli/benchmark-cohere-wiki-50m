#!/bin/bash
# sidecar_v4_raw_collector.sh

# --- 1. SETUP WORKSPACE ---
RUN_FOLDER="${BASE_OUTPUT_DIR}"

# Create specific subfolders to keep the directory clean
mkdir -p "$RUN_FOLDER/raw_metrics"      # For Qdrant internal metrics
mkdir -p "$RUN_FOLDER/raw_sys_metrics"  # For Container/System metrics
mkdir -p "$RUN_FOLDER/telemetry"        # For full cluster telemetry

# We will still keep a simple CSV for the CLIENT stats (Load Generator), 
# because that is local and easy to parse now.
CLIENT_LOG="$RUN_FOLDER/client_stats.csv"
# ass units to the column names
echo "timestamp,client_cpu_%,client_net_in_bytes,client_net_out_bytes,client_memory_mb" > "$CLIENT_LOG"

echo "----------------------------------------------------------------"
echo "RAW DATA COLLECTOR STARTED"
echo "Output Folder: $RUN_FOLDER"
echo "----------------------------------------------------------------"

# Detect network interface (handles eth0, ens4, etc.)
IFACE=$(ip route get 8.8.8.8 | awk '{ print $5; exit }')

# Ensure QDRANT_CLUSTER_URL does not have https:// prefix for this specific formatting
# (Assuming your previous config worked with port 6333)
BASE_URL="${QDRANT_CLUSTER_URL}:6333"

ITERATION=0

while true; do
    # Current Time for Display and Filenames
    TS_PRINT=$(date +%Y-%m-%d\ %H:%M:%S)
    TS_FILE=$(date +%Y%m%d_%H%M%S)
    
    # --- 1. CLIENT LAYER (Local Machine Stats) ---
    # We parse this now because it's simple and local
    CLIENT_CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print 100 - $8}')
    NET_STAT=$(cat /proc/net/dev | grep "$IFACE")
    NET_IN=$(echo $NET_STAT | awk '{print $2}')
    NET_OUT=$(echo $NET_STAT | awk '{print $10}')
    CLIENT_MEMORY=$(free -m | grep "Mem:" | awk '{print $3}')

    # Write local stats to CSV
    echo "$TS_PRINT,$CLIENT_CPU,$NET_IN,$NET_OUT,$CLIENT_MEMORY" >> "$CLIENT_LOG"

    # --- 2. SERVER LAYER (Save RAW Data) ---
    # Instead of parsing, we dump the whole response to a text file.
    
    # Save Qdrant Metrics (RPS, Latency counts, Indexing info)
    curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/metrics" \
    > "$RUN_FOLDER/raw_metrics/metrics_${TS_FILE}.txt"

    # Save System Metrics (CPU, RAM, Disk I/O, Throttling)
    curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/sys_metrics" \
    > "$RUN_FOLDER/raw_sys_metrics/sys_metrics_${TS_FILE}.txt"

    # --- 3. TELEMETRY SNAPSHOT (Every 5 loops / ~10 seconds) ---
    if (( ITERATION % 5 == 0 )); then
        curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/telemetry?details_level=10" \
        > "$RUN_FOLDER/telemetry/telemetry_${TS_FILE}.json"
        
        echo "[$TS_PRINT] Snapshot Saved | Client CPU: $CLIENT_CPU%"
    fi

    ((ITERATION++))
    sleep 5
done