#!/bin/bash

source .env
source .venv/bin/activate

# --- 1. SETUP WORKSPACE ---

# remove BASE_OUTPUT_DIR if it exists
if [ -d "$BASE_OUTPUT_DIR" ]; then
	rm -rf "$BASE_OUTPUT_DIR"
fi

mkdir -p "$BASE_OUTPUT_DIR/raw_metrics"
mkdir -p "$BASE_OUTPUT_DIR/raw_sys_metrics"
mkdir -p "$BASE_OUTPUT_DIR/telemetry"

CLIENT_LOG="$BASE_OUTPUT_DIR/client_stats.csv"
echo "timestamp,client_cpu_%,client_net_in_bytes,client_net_out_bytes,client_memory_mb" >"$CLIENT_LOG"

echo "----------------------------------------------------------------"
echo "RAW DATA COLLECTOR STARTED (1s Resolution)"
echo "Output Folder: $BASE_OUTPUT_DIR"
echo "----------------------------------------------------------------"

IFACE=$(ip route get 8.8.8.8 | awk '{ print $5; exit }')
BASE_URL="${QDRANT_CLUSTER_URL}:6333"

ITERATION=0

while true; do
	# Current Time
	TS_PRINT=$(date +%Y-%m-%d\ %H:%M:%S)
	TS_FILE=$(date +%Y%m%d_%H%M%S)

	# ======================================================
	# 1. CLIENT LAYER - Runs EVERY SECOND (High Resolution)
	# ======================================================

	# Get CPU usage (100 - idle)
	CLIENT_CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print 100 - $8}')

	# Get Network stats
	NET_STAT=$(cat /proc/net/dev | grep "$IFACE")
	NET_IN=$(echo $NET_STAT | awk '{print $2}')
	NET_OUT=$(echo $NET_STAT | awk '{print $10}')

	# Get Memory usage
	CLIENT_MEMORY=$(free -m | grep "Mem:" | awk '{print $3}')

	# Write to CSV immediately
	echo "$TS_PRINT,$CLIENT_CPU,$NET_IN,$NET_OUT,$CLIENT_MEMORY" >>"$CLIENT_LOG"

	# ======================================================
	# 2. SERVER LAYER - Runs EVERY 5 SECONDS
	# ======================================================
	if ((ITERATION % 5 == 0)); then
		# Save Qdrant Metrics
		curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/metrics" \
			>"$BASE_OUTPUT_DIR/raw_metrics/metrics_${TS_FILE}.txt"

		# Save System Metrics
		curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/sys_metrics" \
			>"$BASE_OUTPUT_DIR/raw_sys_metrics/sys_metrics_${TS_FILE}.txt"
	fi

	# ======================================================
	# 3. TELEMETRY - Runs EVERY 30 SECONDS
	# ======================================================
	if ((ITERATION % 30 == 0)); then
		curl -s -H "Authorization: Bearer $QDRANT_API_KEY" "${BASE_URL}/telemetry?details_level=10" \
			>"$BASE_OUTPUT_DIR/telemetry/telemetry_${TS_FILE}.json"

		echo "[$TS_PRINT] Snapshot Saved | Client CPU: $CLIENT_CPU%"
	fi

	# Increment and Sleep for only 1 second
	((ITERATION++))
	sleep 1
done
