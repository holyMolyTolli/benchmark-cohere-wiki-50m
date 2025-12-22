#!/bin/bash

# Configuration - Ensure these are exported in your terminal
# export QDRANT_API_KEY='...'
# export QDRANT_CLUSTER_URL='your-id.cloud.qdrant.io'

OUTPUT_FILE="benchmark_results_$(date +%Y%m%d_%H%M%S).csv"

# Write CSV Header
echo "Parallel,Threads,HNSW_EF,Avg_RPS,Server_Latency_Avg,Search_Latency_Avg" > "$OUTPUT_FILE"

# Define the Grid
PARALLELS=(32 64) # 128 256 384 512 640)
THREADS_LIST=(1) # 2 4 8)
EF_LIST=(32) # 64 128 256)

echo "Starting Grid Search Strategy: Project Dermador"
echo "Results will be saved to: $OUTPUT_FILE"
echo "------------------------------------------------"

for P in "${PARALLELS[@]}"; do
    for T in "${THREADS_LIST[@]}"; do
        for EF in "${EF_LIST[@]}"; do
            
            # DYNAMIC NUM_VECTORS: Ensures the test runs for a meaningful duration.
            # We want each 'parallel user' to perform at least 100 queries.
            NUM_VECTORS=$((P * 100))
            if [ $NUM_VECTORS -lt 5000 ]; then NUM_VECTORS=5000; fi

            echo "[RUNNING] Parallel: $P, Threads: $T, EF: $EF (Queries: $NUM_VECTORS)"

            # Run BFB and capture output
            # We use --json to make parsing the results reliable
            RESULT_JSON="tmp_res.json"
            
            sudo docker run --rm \
              -e QDRANT_API_KEY=$QDRANT_API_KEY \
              qdrant/bfb:latest \
              ./bfb \
              --uri "${QDRANT_CLUSTER_URL}:6334" \
              --collection-name "benchmark" \
              --dim 768 \
              --skip-create --skip-upload --skip-wait-index \
              --search \
              --num-vectors "$NUM_VECTORS" \
              --parallel "$P" \
              --threads "$T" \
              --search-limit 10 \
              --search-hnsw-ef "$EF" \
              --timing-threshold 30.0 \
              --json "$RESULT_JSON" > /dev/null 2>&1

            # Check if JSON was created (Docker run was successful)
            if [ -f "$RESULT_JSON" ]; then
                # Extract Avg RPS and Latencies using Python (since jq might not be installed)
                # BFB JSON format: {"server_timings": [...], "rps": [...], "full_timings": [...]}
                STATS=$(python3 -c "
import json
try:
    with open('$RESULT_JSON') as f:
        d = json.load(f)
        avg_rps = sum(d['rps']) / len(d['rps'])
        avg_server = sum(d['server_timings']) / len(d['server_timings'])
        avg_full = sum(d['full_timings']) / len(d['full_timings'])
        print(f'{avg_rps:.2f},{avg_server:.4f},{avg_full:.4f}')
except:
    print('error,error,error')
")
                echo "$P,$T,$EF,$STATS" >> "$OUTPUT_FILE"
                echo "[SUCCESS] RPS: $(echo $STATS | cut -d',' -f1)"
                rm "$RESULT_JSON"
            else
                echo "[FAILED] Run timed out or connection failed."
                echo "$P,$T,$EF,FAIL,FAIL,FAIL" >> "$OUTPUT_FILE"
            fi

            # THE SLEEP: Settles the Load Balancer and makes the 'Sidecar' logs distinct
            echo "Waiting 15 seconds for cool-down..."
            sleep 15
        done
    done
done

echo "------------------------------------------------"
echo "Grid Search Complete. Data saved to $OUTPUT_FILE"