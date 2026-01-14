#!/bin/bash
# set -e

source .env
source .venv/bin/activate

# --- CONFIGURATION ---
RAW_DATA_DIR="${BASE_OUTPUT_DIR}/raw_benchmark_data"
TEMP_JSON="tmp_res.json"

# --- GRID ---
MAX_OPTIMIZATION_THREADS_LIST=(\"auto\" 2) # \"auto\" 0 1 2 4
# If null - have no limit and choose dynamically to saturate CPU.
# If 0 - no optimization threads, optimizations will be disabled.

MAX_INDEXING_THREADS_LIST=(0 4) # 0 1 2 4 8
# If 0 - automatically select.

DEFAULT_SEGMENT_NUMBER_LIST=(0) #  0 1 2 100 200 300
# If `default_segment_number = 0`, will be automatically selected by the number of available CPUs

MAX_SEGMENT_SIZE_LIST=("null") # **null????** 20000 150000 200000 2000000
# If not set, will be automatically selected considering the number of available CPUs.

INDEXING_THRESHOLD_LIST=(20000) # 0 2000 20000 200000
# To explicitly disable vector indexing, set to `0`.

OPTIMIZER_CPU_BUDGET_LIST=(0 1 2 4 8)
# If 0 - auto selection, keep 1 or more CPUs unallocated depending on CPU size
# If negative - subtract this number of CPUs from the available CPUs.
# If positive - use this exact number of CPUs.

ASYNC_SCORER_LIST=(false)
# AsyncScorer enables io_uring when rescoring

PARALLEL_LIST=(8)
THREADS_LIST=(2)
SEARCH_HNSW_EF_LIST=(32)
# --search-with-payload
# --keywords

# --- PRE-FLIGHT ---
if [[ -z "$QDRANT_API_KEY" || -z "$QDRANT_CLUSTER_URL" ]]; then
    echo "ERROR: Missing API Key or URL."
    exit 1
fi

CLEAN_URL="${QDRANT_CLUSTER_URL#*://}"
CLEAN_URL="${CLEAN_URL%/}"
mkdir -p "$RAW_DATA_DIR"

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

cleanup() {
    echo -e "\nScript interrupted. Cleaning up..."
    rm -f "$TEMP_JSON"
    pkill -f "prepare_data.py" 2>/dev/null
    exit
}
trap cleanup SIGINT SIGTERM EXIT

wait_for_green() {
    echo -n "   Waiting for collection Green status..."
    while true; do
        STATUS=$(curl -sS -X GET "https://${CLEAN_URL}:6333/collections/${COLLECTION_NAME}" -H "api-key: ${QDRANT_API_KEY}" | jq -r '.result.status')
        if [ "$STATUS" == "green" ]; then
            echo " OK."
            return 0
        fi
        sleep 2
    done
}

get_point_count() {
    curl -sS -X GET "https://${CLEAN_URL}:6333/collections/${COLLECTION_NAME}" \
        -H "api-key: ${QDRANT_API_KEY}" | jq -r '.result.points_count'
}

wait_for_cluster_ready() {
    echo -n "   Waiting for Cloud Rolling Update..."
    sleep 5 # Give the API a moment to register the PATCH

    while true; do
        # 1. Check Cloud API Status
        # Correct URL: /api/cluster/v1/accounts/{acc}/clusters/{id}
        RESPONSE=$(curl -sS -X GET "https://api.cloud.qdrant.io/api/cluster/v1/accounts/${ACCOUNT_ID}/clusters/${CLUSTER_ID}" -H "Authorization: apikey ${QDRANT_MANAGEMENT_KEY}")

        # Parse the nested JSON structure
        # State is inside: .state.phase
        PHASE=$(echo "$RESPONSE" | jq -r '.cluster.state.phase')

        # We look for "CLUSTER_PHASE_HEALTHY"
        if [ "$PHASE" == "CLUSTER_PHASE_HEALTHY" ]; then
            # 2. Cloud says Healthy, now check if DB Port 6333 is accepting connections
            # We use a simple curl to the collections endpoint to see if Nginx/Qdrant is up
            HTTP_CODE=$(curl -o /dev/null -w "%{http_code}" -H "api-key: ${QDRANT_API_KEY}" "https://${CLEAN_URL}:6333/collections")

            if [ "$HTTP_CODE" == "200" ]; then
                echo " Ready (Healthy & Reachable)."
                break
            fi
        fi

        echo -n "."
        sleep 5
    done
}

apply_tuning() {
    local max_segment_size=$1
    local default_segment_number=$2
    local max_optimization_threads=$3
    local max_indexing_threads=$4
    local indexing_threshold=$5
    local optimizer_cpu_budget=$6
    local async_scorer=$7

    echo ">>> Applying Collection Params (Hot Update)..."

    # # Handle "auto" vs integer logic for JSON
    # if [ "$max_optimization_threads" == "auto" ] || [ "$max_optimization_threads" == "\"auto\"" ]; then
    #     JSON_OPT_THREADS="null"
    # else
    #     JSON_OPT_THREADS=$max_optimization_threads
    # fi

    raw_request="{
            \"optimizers_config\": {
                \"max_optimization_threads\": $max_optimization_threads,
                \"max_segment_size\": $max_segment_size,
                \"default_segment_number\": $default_segment_number,
                \"indexing_threshold\": $indexing_threshold
            },
            \"hnsw_config\": {
                \"max_indexing_threads\": $max_indexing_threads
            }
        }"


    # retry 3 times
    for i in {1..3}; do
        # 1. Apply Collection Level Settings (Instant, hits Port 6333)
        RETURN_COLLECTION_UPDATE=$(curl -sS -X PATCH "https://${CLEAN_URL}:6333/collections/${COLLECTION_NAME}" \
                -H "api-key: ${QDRANT_API_KEY}" \
                -H "Content-Type: application/json" \
                --data-raw "$raw_request"
        )
        # handle errors like {"status":{"error":"Format error in JSON body: invalid type: string \"auto\", expected usize at line 4 column 42"},"time":0.0}
        if echo "$RETURN_COLLECTION_UPDATE" | grep -q "\"error\""; then
            echo "ERROR: Failed to apply collection update. Response: $RETURN_COLLECTION_UPDATE. Raw request: $raw_request"
            exit 1
        fi

        # if success, break
        if [ "$RETURN_COLLECTION_UPDATE" == "200" ]; then
            break
        fi
    done

    # handle errors like {"status":{"error":"Format error in JSON body: invalid type: string \"auto\", expected usize at line 4 column 42"},"time":0.0}
    if echo "$RETURN_COLLECTION_UPDATE" | grep -q "\"error\""; then
        exit 1
    fi

    echo ""
    echo ">>> Applying Cluster Params (Requires Cloud API + Rolling Update)..."


    CURRENT_STATE=$(curl -sS -X GET "https://api.cloud.qdrant.io/api/cluster/v1/accounts/${ACCOUNT_ID}/clusters/${CLUSTER_ID}" \
        -H "Authorization: apikey ${QDRANT_MANAGEMENT_KEY}")

    if [[ -z "$CURRENT_STATE" ]] || [[ "$CURRENT_STATE" == *"Not Found"* ]]; then
        echo "CRITICAL ERROR: Failed to fetch Cluster State. Check Account/Cluster ID."
        exit 1
    fi

    NEW_PAYLOAD=$(echo "$CURRENT_STATE" | jq \
            --arg cpu "$optimizer_cpu_budget" \
            --arg async "$async_scorer" \
            '
        .cluster.configuration.databaseConfiguration.storage.performance = {
            "optimizerCpuBudget": ($cpu | tonumber),
            "asyncScorer": (if $async == "true" then true else false end)
        }
        | del(.cluster.state)
        | del(.cluster.id)
        | del(.cluster.createdAt)
        | del(.cluster.configuration.lastModifiedAt)
    ')

    # 5. PUT Update
    UPDATE_RES=$(curl -sS -X PUT "https://api.cloud.qdrant.io/api/cluster/v1/accounts/${ACCOUNT_ID}/clusters/${CLUSTER_ID}" \
            -H "Authorization: apikey ${QDRANT_MANAGEMENT_KEY}" \
            -H "Content-Type: application/json" \
        -d "$NEW_PAYLOAD")

    # Check for immediate failure
    if echo "$UPDATE_RES" | grep -q "\"code\":"; then
        CODE=$(echo "$UPDATE_RES" | jq -r '.code')
        if [ "$CODE" != "null" ] && [ "$CODE" != "0" ]; then
            echo "CRITICAL ERROR: PUT Failed. Response: $UPDATE_RES"
            exit 1
        fi
    fi

    # 3. CRITICAL: Wait for the rolling update to finish
    wait_for_cluster_ready

    # 2. GET Current Cluster State
    CURRENT_STATE=$(curl -sS -X GET "https://api.cloud.qdrant.io/api/cluster/v1/accounts/${ACCOUNT_ID}/clusters/${CLUSTER_ID}" \
        -H "Authorization: apikey ${QDRANT_MANAGEMENT_KEY}")

    # check optimizerCpuBudget
    # --- SAFETY FIX: Normalize null to 0 ---
    OPTIMIZER_CPU_BUDGET=$(echo $CURRENT_STATE | jq -r '.cluster.configuration.databaseConfiguration.storage.performance.optimizerCpuBudget')
    if [ "$OPTIMIZER_CPU_BUDGET" == "null" ]; then OPTIMIZER_CPU_BUDGET=0; fi

    if [ "$OPTIMIZER_CPU_BUDGET" != "$optimizer_cpu_budget" ]; then
        echo "ERROR: optimizer_cpu_budget not applied. Expected $optimizer_cpu_budget, got $OPTIMIZER_CPU_BUDGET"
        exit 1
    fi

    # check asyncScorer
    ASYNC_SCORER=$(echo $CURRENT_STATE | jq -r '.cluster.configuration.databaseConfiguration.storage.performance.asyncScorer')
    if [ "$ASYNC_SCORER" == "null" ]; then ASYNC_SCORER=false; fi


    if [ "$ASYNC_SCORER" != "$async_scorer" ]; then
        echo "ERROR: async_scorer not applied. Expected $async_scorer, got $ASYNC_SCORER"
        exit 1
    fi

    # 4. Verify Collection Settings Only
    # (We cannot verify Cluster settings via /collections endpoint, so I removed those checks)
    COLLECTION_INFO=$(curl -sS -X GET "https://${CLEAN_URL}:6333/collections/${COLLECTION_NAME}" \
        -H "api-key: ${QDRANT_API_KEY}")

    # check max_optimization_threads
    MAX_OPTIMIZATION_THREADS=$(echo $COLLECTION_INFO | jq -r '.result.config.optimizer_config.max_optimization_threads')
    # if max_optimization_threads is auto, convert to null
    if [ "$max_optimization_threads" == \"auto\" ]; then
        max_optimization_threads=null
    fi
    if [ "$MAX_OPTIMIZATION_THREADS" != "$max_optimization_threads" ]; then
        echo "ERROR: max_optimization_threads not applied. Expected $max_optimization_threads, got $MAX_OPTIMIZATION_THREADS"
        exit 1
    fi

    # check max_segment_size
    MAX_SEGMENT_SIZE=$(echo $COLLECTION_INFO | jq -r '.result.config.optimizer_config.max_segment_size')
    # if max_segment_size is auto, convert to null
    if [ "$max_segment_size" == \"auto\" ]; then
        max_segment_size=null
    fi

    if [ "$MAX_SEGMENT_SIZE" != "$max_segment_size" ]; then
        echo "ERROR: max_segment_size not applied. Expected $max_segment_size, got $MAX_SEGMENT_SIZE"
        exit 1
    fi

    # check default_segment_number
    DEFAULT_SEGMENT_NUMBER=$(echo $COLLECTION_INFO | jq -r '.result.config.optimizer_config.default_segment_number')
    if [ "$DEFAULT_SEGMENT_NUMBER" != "$default_segment_number" ]; then
        echo "ERROR: default_segment_number not applied. Expected $default_segment_number, got $DEFAULT_SEGMENT_NUMBER"
        exit 1
    fi

    # check indexing_threshold
    INDEXING_THRESHOLD=$(echo $COLLECTION_INFO | jq -r '.result.config.optimizer_config.indexing_threshold')
    if [ "$INDEXING_THRESHOLD" != "$indexing_threshold" ]; then
        echo "ERROR: indexing_threshold not applied. Expected $indexing_threshold, got $INDEXING_THRESHOLD"
        exit 1
    fi

    # check max_indexing_threads
    MAX_INDEXING_THREADS=$(echo $COLLECTION_INFO | jq -r '.result.config.hnsw_config.max_indexing_threads')
    if [ "$MAX_INDEXING_THREADS" != "$max_indexing_threads" ]; then
        echo "ERROR: max_indexing_threads not applied. Expected $max_indexing_threads, got $MAX_INDEXING_THREADS"
        exit 1
    fi
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

echo "========================================================"
echo "STARTING BENCHMARK GRID"
echo "========================================================"

pkill -f "prepare_data.py"

# Initial Reset
echo "Resetting DB to 49M baseline..."
export LIMIT_POINTS=49000000
wait_for_cluster_ready # otherwise prepare_data fails
python3 upload/prepare_data.py
sleep 2
# wait_for_green

# --- MAIN LOOP ---
for optimizer_cpu_budget in "${OPTIMIZER_CPU_BUDGET_LIST[@]}"; do
    for async_scorer in "${ASYNC_SCORER_LIST[@]}"; do
        for max_segment_size in "${MAX_SEGMENT_SIZE_LIST[@]}"; do
            for default_segment_number in "${DEFAULT_SEGMENT_NUMBER_LIST[@]}"; do
                for max_optimization_threads in "${MAX_OPTIMIZATION_THREADS_LIST[@]}"; do
                    for max_indexing_threads in "${MAX_INDEXING_THREADS_LIST[@]}"; do
                        for indexing_threshold in "${INDEXING_THRESHOLD_LIST[@]}"; do
                            echo "----------------------------------------------------------------"
                            echo "NEW CONFIGURATION: max_segment_size=$max_segment_size | default_segment_number=$default_segment_number | max_optimization_threads=$max_optimization_threads | max_indexing_threads=$max_indexing_threads | indexing_threshold=$indexing_threshold | optimizer_cpu_budget=$optimizer_cpu_budget | async_scorer=$async_scorer"
                            echo "----------------------------------------------------------------"

                            # ---------------------------------------------------------
                            # STEP 1: APPLY TUNING (Server Side)
                            # ---------------------------------------------------------
                            echo "Applying Tuning Parameters..."
                            # # first value for each parameter in the loop:
                            # max_segment_size=${MAX_SEGMENT_SIZE_LIST[0]}
                            # default_segment_number=${DEFAULT_SEGMENT_NUMBER_LIST[0]}
                            # max_optimization_threads=${MAX_OPTIMIZATION_THREADS_LIST[0]}
                            # max_indexing_threads=${MAX_INDEXING_THREADS_LIST[0]}
                            # indexing_threshold=${INDEXING_THRESHOLD_LIST[0]}
                            # optimizer_cpu_budget=${OPTIMIZER_CPU_BUDGET_LIST[0]}
                            # async_scorer=${ASYNC_SCORER_LIST[0]}
                            apply_tuning "$max_segment_size" "$default_segment_number" "$max_optimization_threads" "$max_indexing_threads" "$indexing_threshold" "$optimizer_cpu_budget" "$async_scorer"

                            # ---------------------------------------------------------
                            # STEP 2: RESET TO BASELINE (Blocking)
                            # ---------------------------------------------------------
                            echo "Resetting DB to 50M baseline..."
                            export LIMIT_POINTS=50000000

                            # We run this in foreground (no &) because we must wait for it to finish cleaning
                            # I do this here ech time i reset the configuration because i want to be sure the db is clean and ready for the next experiment and to make experiments comparable. deleting is relatively fast and i have to wait for green status anyway after upodating ther config.
                            wait_for_cluster_ready # otherwise prepare_data fails
                            python3 upload/prepare_data.py
                            sleep 2

                            # ---------------------------------------------------------
                            # STEP 3: WAIT FOR GREEN STATUS
                            # ---------------------------------------------------------
                            wait_for_green

                            # ---------------------------------------------------------
                            # STEP 4: START BACKGROUND WRITES
                            # ---------------------------------------------------------
                            CURRENT_VECTORS=$(get_point_count)

                            echo "Starting Concurrent Writes..."
                            export LIMIT_POINTS=100000000 # 100M vectors is just a large number. I just want to upload concurrently in background and will stop the upload once the experiment is done.

                            # Start in background & save PID
                            wait_for_cluster_ready # otherwise prepare_data fails
                            # python3 upload/prepare_data.py >/dev/null 2>&1 &
                            python3 upload/prepare_data.py > ${BASE_OUTPUT_DIR}/write_output.log 2>&1 &
                            BG_PID=$!

                            # ---------------------------------------------------------
                            # STEP 3: WAIT FOR 5XM VECTORS
                            # ---------------------------------------------------------
                            TARGET_VECTORS=$((CURRENT_VECTORS + 1000000))
                            echo -n "   Waiting for $TARGET_VECTORS vectors..."
                            while true; do
                                VECTORS=$(get_point_count)
                                # SAFETY CHECK: Did the python script die?
                                if ! kill -0 $BG_PID 2>/dev/null; then
                                    echo " ERROR: Background uploader died! Stopping this run."
                                    break
                                fi
                                if [ "$VECTORS" -ge "$TARGET_VECTORS" ]; then
                                    echo " OK."
                                    break
                                fi
                                sleep 1
                            done

                            # ---------------------------------------------------------
                            # STEP 5: RUN READ BENCHMARK
                            # ---------------------------------------------------------
                            for P in "${PARALLEL_LIST[@]}"; do
                                for T in "${THREADS_LIST[@]}"; do
                                    for EF in "${SEARCH_HNSW_EF_LIST[@]}"; do
                                        # heuristic: scale vectors based on parallelism, min 5000
                                        NUM_VECTORS=$((P * 150))
                                        if [ "$NUM_VECTORS" -lt 5000 ]; then NUM_VECTORS=5000; fi

                                        echo -n "Running [P:$P | T:$T | EF:$EF] ... "

                                        # Capture Start Time (Format: YYYYMMDD_HHMMSS)
                                        START_TIME=$(date +%Y%m%d_%H%M%S)
                                        START_VECTORS=$(get_point_count)

                                        # --- RUN DOCKER ---
                                        sudo docker run --rm \
                                            --network host \
                                            -v "$(pwd):/out" \
                                            -e QDRANT_API_KEY="$QDRANT_API_KEY" \
                                            -e RUST_BACKTRACE=1 \
                                            qdrant/bfb:latest \
                                            ./bfb \
                                            --uri "https://${CLEAN_URL}:6334" \
                                            --collection-name "${COLLECTION_NAME}" \
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
                                            --timing-threshold 300.0 \
                                            --retry 5 \
                                            --retry-interval 2 \
                                            --timeout 600 \
                                            --json "/out/$TEMP_JSON" \
                                            >>"$BASE_OUTPUT_DIR/benchmark_logs.txt" 2>&1

                                        # Capture the exit code of docker
                                        DOCKER_EXIT_CODE=$?

                                        if [ $DOCKER_EXIT_CODE -ne 0 ]; then
                                            echo "DOCKER FAILED with exit code $DOCKER_EXIT_CODE"
                                            # Continue to next loop iteration instead of crashing
                                            continue
                                        fi

                                        # Capture End Time
                                        END_TIME=$(date +%Y%m%d_%H%M%S)
                                        END_VECTORS=$(get_point_count)

                                        # Process Result
                                        if [ -f "$TEMP_JSON" ]; then
                                            # Docker often creates files as root, fix permissions
                                            sudo chmod 666 "$TEMP_JSON"

                                            # Construct Filename with metadata
                                            FILENAME="run_Opt${max_optimization_threads}_Idx${max_indexing_threads}_Seg${max_segment_size}_SegNum${default_segment_number}_IndTh${indexing_threshold}_P${P}_T${T}_EF${EF}_OptimizerCpuBudget${optimizer_cpu_budget}_AsyncScorer${async_scorer}_start${START_TIME}_end${END_TIME}.json"
                                            mv "$TEMP_JSON" "${RAW_DATA_DIR}/${FILENAME}"

                                            echo "Done. Saved."
                                        else
                                            echo "FAILED. No output generated."
                                        fi

                                        # calculate vectors per second
                                        VECTORS_PER_SECOND=$(( (END_VECTORS - START_VECTORS) / (END_TIME - START_TIME) ))
                                        echo "Vectors per second: $VECTORS_PER_SECOND"

                                        # Short cooldown to let the server breathe
                                        sleep 10
                                    done
                                done
                            done
                            # ---------------------------------------------------------
                            # STEP 6: STOP WRITES (THE "NUCLEAR" CLEANUP option)
                            # ---------------------------------------------------------
                            echo "Stopping background writes and ensuring TOTAL cleanup..."

                            # The common string shared by parent AND all multiprocessing children
                            # based on your logs:
                            TARGET_PROCESS_PATTERN=".venv/bin/python3"

                            # 1. Send polite terminate signal (SIGTERM) to EVERYTHING matching the venv python
                            # This hits the parent prepare_data.py AND all multiprocessing.forkserver children
                            pkill -f "$TARGET_PROCESS_PATTERN"

                            # 2. ACTIVELY WAIT for processes to actually disappear from the process list
                            echo -n "   Waiting for all venv python processes to release memory and exit..."
                            WAIT_CYCLES=0
                            # pgrep -f returns true (0) as long as it finds ANY process matching the pattern
                            while pgrep -f "$TARGET_PROCESS_PATTERN" >/dev/null; do
                                sleep 1
                                WAIT_CYCLES=$((WAIT_CYCLES + 1))

                                # If they haven't died after 20 seconds, get aggressive
                                if [ "$WAIT_CYCLES" -ge 20 ]; then
                                    echo ""
                                    echo "WARNING: Processes taking too long to die. Issuing SIGKILL... force kill..."
                                    # -9 sends SIGKILL, which cannot be ignored.
                                    pkill -9 -f "$TARGET_PROCESS_PATTERN"
                                    # Give the OS a moment to reclaim the shredded memory
                                    sleep 5
                                    break
                                fi
                                echo -n "."
                            done
                            echo " Done. All python processes from this venv are dead."

                            # Clear the PID variable just in case, though the process is definitely gone now.
                            BG_PID=""
                            # A final cooldown to ensure OS memory counters update before next run
                            sleep 5
                        done
                    done
                done
            done
        done
    done
done

echo "========================================================"
echo "Benchmark Grid Complete!"
echo "Files located in: $RAW_DATA_DIR"
