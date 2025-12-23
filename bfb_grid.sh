#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# --- CONFIGURATION ---
BASE_OUTPUT_DIR="${BASE_OUTPUT_DIR}"
BATCH_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAW_DATA_DIR="${BASE_OUTPUT_DIR}/raw_benchmark_data"
TEMP_JSON="tmp_res.json"

# Settings Grid
PARALLELS=(16 32 48 64 128)
THREADS_LIST=(4 16 32)
EF_LIST=(16 32 48 64 128)
# --search-with-payload
# --keywords

# --- PRE-FLIGHT CHECKS ---
if [[ -z "$QDRANT_API_KEY" || -z "$QDRANT_CLUSTER_URL" ]]; then
    echo "❌ ERROR: Environment variables QDRANT_API_KEY or QDRANT_CLUSTER_URL not found."
    exit 1
fi

# Clean URL (remove protocol and trailing slashes using bash parameter expansion)
CLEAN_URL="${QDRANT_CLUSTER_URL#*://}" # Remove http:// or https://
CLEAN_URL="${CLEAN_URL%/}"             # Remove trailing slash

# Create Output Directory
mkdir -p "$RAW_DATA_DIR"

# Ensure cleanup of temp file on exit or interrupt
trap "rm -f $TEMP_JSON" EXIT

echo "========================================================"
echo "🚀 STARTING BENCHMARK GRID"
echo "📂 Output Directory: $RAW_DATA_DIR"
echo "🎯 Target: $CLEAN_URL"
echo "========================================================"

# --- MAIN LOOP ---
for P in "${PARALLELS[@]}"; do
    for T in "${THREADS_LIST[@]}"; do
        for EF in "${EF_LIST[@]}"; do
            
            # heuristic: scale vectors based on parallelism, min 1000
            NUM_VECTORS=$((P * 150))
            if [ "$NUM_VECTORS" -lt 5000 ]; then NUM_VECTORS=5000; fi

            echo -n "👉 Running [P:$P | T:$T | EF:$EF] ... "

            # Capture Start Time (Format: YYYYMMDD_HHMMSS)
            START_TIME=$(date +%Y%m%d_%H%M%S)

            # --- RUN DOCKER ---
            # Using an array for arguments is cleaner and safer
            sudo docker run --rm \
              -v "$(pwd):/out" \
              -e QDRANT_API_KEY="$QDRANT_API_KEY" \
              qdrant/bfb:latest \
              ./bfb \
              --uri "https://${CLEAN_URL}:6334" \
              --collection-name "benchmark" \
              --dim 768 \
              --skip-create \
              --skip-upload \
              --skip-wait-index \
              --search \
              --num-vectors "$NUM_VECTORS" \
              --parallel "$P" \
              --threads "$T" \
              --search-limit 10 \
              --search-hnsw-ef "$EF" \
              --timing-threshold 30.0 \
              --json "/out/$TEMP_JSON" > /dev/null 2>&1

            # Capture End Time
            END_TIME=$(date +%Y%m%d_%H%M%S)

            # Process Result
            if [ -f "$TEMP_JSON" ]; then
                # Docker often creates files as root, fix permissions
                sudo chmod 666 "$TEMP_JSON"
                
                # Construct Filename with metadata
                FILENAME="run_P${P}_T${T}_EF${EF}_start${START_TIME}_end${END_TIME}.json"
                mv "$TEMP_JSON" "${RAW_DATA_DIR}/${FILENAME}"
                
                echo "✅ Done. Saved."
            else
                echo "❌ FAILED. No output generated."
            fi

            # Short cooldown to let the server breathe
            sleep 2
        done
    done
done

echo "========================================================"
echo "🎉 Benchmark Grid Complete!"
echo "Files located in: $RAW_DATA_DIR"