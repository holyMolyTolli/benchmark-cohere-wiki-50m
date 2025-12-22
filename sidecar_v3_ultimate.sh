#!/bin/bash
# sidecar_v3_ultimate.sh

# 1. Organize workspace
START_TIME=$(date +%Y%m%d_%H%M%S)
TELEMETRY_FOLDER="run_$START_TIME"
mkdir -p "$TELEMETRY_FOLDER"
LOG_FILE="$TELEMETRY_FOLDER/dermador_master_log.csv"

# 2. Header
echo "Timestamp,Client_CPU,Client_Net_In,Client_Net_Out,Server_CPU_Usage,Server_Throttled,Server_RSSAnon_GB,Segments_Count,Active_Optimizations,Disk_Read_MBs,Traefik_Latency" > "$LOG_FILE"

echo "ULTIMATE MONITORING ENABLED. Output folder: $TELEMETRY_FOLDER"

# Detect network interface (handles eth0, ens4, etc.)
IFACE=$(ip route get 8.8.8.8 | awk '{ print $5; exit }')

ITERATION=0
while true; do
    TS=$(date +%H:%M:%S)
    
    # 1. CLIENT LAYER (Kirstin Instance)
    CLIENT_CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print 100 - $8}')
    NET_STAT=$(cat /proc/net/dev | grep "$IFACE")
    NET_IN=$(echo $NET_STAT | awk '{print $2}')
    NET_OUT=$(echo $NET_STAT | awk '{print $10}')

    # 2. SERVER LAYER (Using Port 6333 for REST/Metrics)
    # Ensure QDRANT_CLUSTER_URL does not have https:// prefix for this specific formatting
    BASE_URL="${QDRANT_CLUSTER_URL}:6333"
    
    M_DATA=$(curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/metrics")
    S_DATA=$(curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/sys_metrics")

    # Extract Stats
    SERVER_CPU=$(echo "$S_DATA" | grep "container_cpu_usage_seconds_total" | awk '{print $2}')
    THROTTLE=$(echo "$S_DATA" | grep "container_cpu_cfs_throttled_periods_total" | awk '{print $2}')
    RSS_ANON=$(echo "$M_DATA" | grep "qdrant_node_rssanon_bytes" | awk '{print $2 / 1024 / 1024 / 1024}')
    SEG_COUNT=$(echo "$M_DATA" | grep "collection_segments_count" | tail -n 1 | awk '{print $2}')
    OPTIMIZING=$(echo "$M_DATA" | grep "collection_running_optimizations" | tail -n 1 | awk '{print $2}')
    DISK_READ=$(echo "$M_DATA" | grep "storage_io_read_bytes_total" | awk '{print $2 / 1024 / 1024}')
    TRAEFIK=$(echo "$S_DATA" | grep "traefik_service_request_duration_seconds_sum" | awk '{print $2}')

    # 3. LOG TO CSV
    echo "$TS,$CLIENT_CPU,$NET_IN,$NET_OUT,$SERVER_CPU,$THROTTLE,$RSS_ANON,$SEG_COUNT,$OPTIMIZING,$DISK_READ,$TRAEFIK" >> "$LOG_FILE"
    
    # 4. TELEMETRY SNAPSHOT (Every 5 iterations = ~10 seconds)
    if (( ITERATION % 5 == 0 )); then
        FILE_TS=$(date +%H%M%S)
        curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/telemetry?details_level=10" > "$TELEMETRY_FOLDER/telemetry_snap_${FILE_TS}.json"
        echo "[$TS] Snapshot saved | Client CPU: $CLIENT_CPU%"
    fi

    ((ITERATION++))
    sleep 2
done