#!/bin/bash

set -euo pipefail

# Wavefront Profiling Job Submission Script
# Submits PySpark job to Dataproc Serverless

# Configuration
PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
SUBNET="${SUBNET:-projects/${PROJECT_ID}/regions/${REGION}/subnetworks/loadgen-subnet}"

# Paths
INPUT_PATH="${INPUT_PATH:-gs://loadgen-capture-bucket/capture}"
OUTPUT_PATH="${OUTPUT_PATH:-gs://loadgen-recipes-bucket/recipes}"
TEMP_PATH="${TEMP_PATH:-gs://loadgen-temp-bucket/profiling}"

# Job configuration
BATCH_ID="wavefront-profiling-$(date +%Y%m%d-%H%M%S)"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-loadgen-profiling@${PROJECT_ID}.iam.gserviceaccount.com}"

# Spark configuration
MAX_EXECUTORS="${MAX_EXECUTORS:-50}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-15g}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"

# Logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [PROFILING] $1"
}

# Validation
if [[ -z "$PROJECT_ID" ]]; then
    echo "ERROR: PROJECT_ID environment variable is required"
    exit 1
fi

# Check for required date parameter
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <capture-date> [options]"
    echo "  capture-date: Date in YYYY-MM-DD format"
    echo ""
    echo "Environment variables:"
    echo "  PROJECT_ID: GCP project ID (required)"
    echo "  REGION: GCP region (default: us-central1)"
    echo "  INPUT_PATH: Capture data path (default: gs://loadgen-capture-bucket/capture)"
    echo "  OUTPUT_PATH: Output recipes path (default: gs://loadgen-recipes-bucket/recipes)" 
    echo "  TEMP_PATH: Temporary processing path (default: gs://loadgen-temp-bucket/profiling)"
    echo "  SERVICE_ACCOUNT: Service account email"
    echo "  MAX_EXECUTORS: Max Spark executors (default: 50)"
    exit 1
fi

CAPTURE_DATE="$1"

# Validate date format
if ! date -d "$CAPTURE_DATE" >/dev/null 2>&1; then
    echo "ERROR: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

# Construct paths
FULL_INPUT_PATH="${INPUT_PATH}/dt=${CAPTURE_DATE}"
FULL_OUTPUT_PATH="${OUTPUT_PATH}/v1"
FULL_TEMP_PATH="${TEMP_PATH}/${CAPTURE_DATE}"

log "Starting Wavefront profiling job..."
log "  Batch ID: $BATCH_ID"
log "  Capture Date: $CAPTURE_DATE"
log "  Input: $FULL_INPUT_PATH"
log "  Output: $FULL_OUTPUT_PATH"
log "  Temp: $FULL_TEMP_PATH"

# Check if input data exists
log "Checking input data availability..."
if ! gsutil ls "$FULL_INPUT_PATH/**/*.wf.zst" > /dev/null 2>&1; then
    echo "ERROR: No capture data found at $FULL_INPUT_PATH"
    echo "Make sure capture completed successfully for date $CAPTURE_DATE"
    exit 1
fi

# Count input files
INPUT_FILES=$(gsutil ls "$FULL_INPUT_PATH/**/*.wf.zst" | wc -l)
log "Found $INPUT_FILES compressed capture files"

# Estimate data size
INPUT_SIZE=$(gsutil du -s "$FULL_INPUT_PATH" | awk '{print $1}')
INPUT_SIZE_GB=$((INPUT_SIZE / 1024 / 1024 / 1024))
log "Input data size: ${INPUT_SIZE_GB} GB"

# Adjust Spark configuration based on data size
if [[ $INPUT_SIZE_GB -gt 1000 ]]; then
    MAX_EXECUTORS=100
    EXECUTOR_MEMORY="30g"
    DRIVER_MEMORY="16g"
    log "Large dataset detected, scaling up to $MAX_EXECUTORS executors"
elif [[ $INPUT_SIZE_GB -gt 100 ]]; then
    MAX_EXECUTORS=50
    EXECUTOR_MEMORY="15g"
    log "Medium dataset, using $MAX_EXECUTORS executors"
else
    MAX_EXECUTORS=20
    EXECUTOR_MEMORY="7g"
    log "Small dataset, using $MAX_EXECUTORS executors"
fi

# Create batch configuration
BATCH_CONFIG=$(cat <<EOF
{
  "runtimeConfig": {
    "version": "2.0",
    "properties": {
      "spark.app.name": "$BATCH_ID",
      "spark.executor.instances": "$MAX_EXECUTORS",
      "spark.executor.cores": "$EXECUTOR_CORES", 
      "spark.executor.memory": "$EXECUTOR_MEMORY",
      "spark.driver.memory": "$DRIVER_MEMORY",
      "spark.sql.adaptive.enabled": "true",
      "spark.sql.adaptive.coalescePartitions.enabled": "true",
      "spark.sql.adaptive.skewJoin.enabled": "true",
      "spark.sql.adaptive.localShuffleReader.enabled": "true",
      "spark.sql.execution.arrow.pyspark.enabled": "true",
      "spark.sql.parquet.compression.codec": "zstd",
      "spark.sql.parquet.columnarReaderBatchSize": "8192",
      "spark.hadoop.fs.gs.block.size": "134217728",
      "spark.hadoop.fs.gs.inputstream.buffer.size": "8388608",
      "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
  },
  "pysparkBatch": {
    "mainPythonFileUri": "gs://loadgen-scripts-bucket/profiling/wavefront_profiler.py",
    "args": [
      "--input", "$FULL_INPUT_PATH",
      "--output", "$FULL_OUTPUT_PATH", 
      "--temp", "$FULL_TEMP_PATH",
      "--app-name", "$BATCH_ID"
    ],
    "pythonFileUris": [
      "gs://loadgen-scripts-bucket/profiling/requirements.txt"
    ]
  },
  "environmentConfig": {
    "executionConfig": {
      "serviceAccount": "$SERVICE_ACCOUNT",
      "subnetworkUri": "$SUBNET",
      "idleTtl": "1800s"
    }
  },
  "labels": {
    "component": "loadgen-profiling",
    "capture-date": "$CAPTURE_DATE",
    "environment": "production"
  }
}
EOF
)

# Upload profiling script to GCS if needed
SCRIPT_GCS_PATH="gs://loadgen-scripts-bucket/profiling/wavefront_profiler.py"
log "Uploading profiling script to $SCRIPT_GCS_PATH"
gsutil cp "$(dirname "$0")/wavefront_profiler.py" "$SCRIPT_GCS_PATH"

# Upload requirements if exists
if [[ -f "$(dirname "$0")/requirements.txt" ]]; then
    gsutil cp "$(dirname "$0")/requirements.txt" "gs://loadgen-scripts-bucket/profiling/requirements.txt"
fi

# Submit the batch job
log "Submitting Dataproc Serverless batch job..."
echo "$BATCH_CONFIG" > "/tmp/${BATCH_ID}-config.json"

gcloud dataproc batches submit pyspark \
    --batch="$BATCH_ID" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --batch-config="/tmp/${BATCH_ID}-config.json"

if [[ $? -eq 0 ]]; then
    log "Batch job submitted successfully!"
    log "Batch ID: $BATCH_ID"
    log "Monitor progress with:"
    log "  gcloud dataproc batches describe $BATCH_ID --region=$REGION --project=$PROJECT_ID"
    log "  gcloud dataproc batches list --region=$REGION --project=$PROJECT_ID --filter=\"batchId:$BATCH_ID\""
else
    log "ERROR: Failed to submit batch job"
    exit 1
fi

# Wait for job completion (optional)
if [[ "${WAIT_FOR_COMPLETION:-false}" == "true" ]]; then
    log "Waiting for job completion..."
    
    while true; do
        STATUS=$(gcloud dataproc batches describe "$BATCH_ID" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(state)" 2>/dev/null || echo "UNKNOWN")
        
        case "$STATUS" in
            "SUCCEEDED")
                log "Job completed successfully!"
                break
                ;;
            "FAILED"|"CANCELLED")
                log "ERROR: Job failed with status $STATUS"
                log "Check logs with:"
                log "  gcloud dataproc batches describe $BATCH_ID --region=$REGION --project=$PROJECT_ID"
                exit 1
                ;;
            "RUNNING"|"PENDING")
                log "Job status: $STATUS"
                sleep 60
                ;;
            *)
                log "Unknown job status: $STATUS"
                sleep 30
                ;;
        esac
    done
    
    # Verify output
    log "Verifying profiling output..."
    if gsutil ls "$FULL_OUTPUT_PATH/_PROFILE_OK" > /dev/null 2>&1; then
        log "Profiling completed successfully!"
        
        # Count generated recipes
        RECIPE_COUNT=$(gsutil ls "$FULL_OUTPUT_PATH/recipes/*.json.zst" 2>/dev/null | wc -l || echo "0")
        log "Generated $RECIPE_COUNT recipe files"
        
        # Show QA report location
        QA_REPORT="$FULL_OUTPUT_PATH/reports/profile_qa.html"
        if gsutil ls "$QA_REPORT" > /dev/null 2>&1; then
            log "QA report available at: $QA_REPORT"
        fi
        
    else
        log "WARNING: _PROFILE_OK marker not found. Check job logs for issues."
        exit 1
    fi
fi

# Cleanup temp config
rm -f "/tmp/${BATCH_ID}-config.json"

log "Profiling job submission completed!"
log "Next steps:"
log "  1. Monitor job progress in GCP Console or using gcloud commands above"
log "  2. Once completed, verify _PROFILE_OK marker exists at: $FULL_OUTPUT_PATH/_PROFILE_OK"
log "  3. Review QA reports at: $FULL_OUTPUT_PATH/reports/"
log "  4. If successful, raw capture data will be scheduled for deletion"