#!/bin/bash

# --- ENSURE VARIABLES ARE PRESENT ---
if [[ -z "$QDRANT_API_KEY" || -z "$QDRANT_CLUSTER_URL" ]]; then
    echo "ERROR: Environment variables not found!"
    exit 1
fi

CLEAN_URL=$(echo $QDRANT_CLUSTER_URL | sed -e 's|^[^/]*//||' -e 's|/||g')
OUTPUT_FILE="benchmark_results_$(date +%Y%m%d_%H%M%S).csv"
echo "Parallel,Threads,HNSW_EF,Avg_RPS,Server_Latency_Avg,Search_Latency_Avg" > "$OUTPUT_FILE"

PARALLELS=(32 64 128 256)
THREADS_LIST=(1 2 4)
EF_LIST=(32 64 128)

for P in "${PARALLELS[@]}"; do
    for T in "${THREADS_LIST[@]}"; do
        for EF in "${EF_LIST[@]}"; do
            
            NUM_VECTORS=$((P * 150))
            if [ $NUM_VECTORS -lt 5000 ]; then NUM_VECTORS=5000; fi

            echo "[RUNNING] Parallel: $P, Threads: $T, EF: $EF"

            RESULT_JSON="tmp_res.json"
            rm -f "$RESULT_JSON"
            
            # --- THE FIX IS HERE: -v mapping and the full path for --json ---
            sudo docker run --rm \
              -v "$(pwd):/out" \
              -e QDRANT_API_KEY="$QDRANT_API_KEY" \
              qdrant/bfb:latest \
              ./bfb \
              --uri "https://${CLEAN_URL}:6334" \
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
              --json "/out/$RESULT_JSON"

            # Give the OS a split second to sync the file
            sleep 1

            if [ -f "$RESULT_JSON" ]; then
                # (Permissions fix: Docker/sudo makes it root-owned)
                sudo chmod 666 "$RESULT_JSON"
                
                STATS=$(python3 -c "
import json
try:
    with open('$RESULT_JSON') as f:
        d = json.load(f)
        if not d['rps']: print('0,0,0')
        else:
            avg_rps = sum(d['rps']) / len(d['rps'])
            avg_server = sum(d['server_timings']) / len(d['server_timings'])
            avg_full = sum(d['full_timings']) / len(d['full_timings'])
            print(f'{avg_rps:.2f},{avg_server:.4f},{avg_full:.4f}')
except Exception as e:
    print('error,error,error')
")
                echo "$P,$T,$EF,$STATS" >> "$OUTPUT_FILE"
                echo ">> [SUCCESS] Avg RPS: $(echo $STATS | cut -d',' -f1)"
                rm "$RESULT_JSON"
            else
                echo ">> [FAILED] JSON file not found at $(pwd)/$RESULT_JSON"
                echo "$P,$T,$EF,FAIL,FAIL,FAIL" >> "$OUTPUT_FILE"
            fi

            echo "Sleeping 10s for cooldown..."
            echo "------------------------------------------------"
            sleep 10
        done
    done
done