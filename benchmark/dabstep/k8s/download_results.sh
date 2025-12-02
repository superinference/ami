#!/bin/bash
# Download SuperInference Benchmark Results
# Downloads all chunk results, MCP logs, and benchmarks into timestamped folder

set -e

# Load environment variables from .env file if it exists
# Try project root first, then current directory
ENV_FILE=""
if [ -f "../../../.env" ]; then
    ENV_FILE="../../../.env"
elif [ -f "../../.env" ]; then
    ENV_FILE="../../.env"
elif [ -f "../.env" ]; then
    ENV_FILE="../.env"
elif [ -f ".env" ]; then
    ENV_FILE=".env"
fi

if [ -n "$ENV_FILE" ]; then
    echo "📋 Loading environment variables from: $ENV_FILE"
    # Export variables from .env file (skip comments and empty lines)
    # Use the same method as in the YAML files
    if [ -f "$ENV_FILE" ]; then
        export $(cat "$ENV_FILE" | grep -v '^#' | grep -v '^$' | xargs)
    fi
else
    echo "⚠️  No .env file found, using defaults or existing environment variables"
fi

# Get pod name
POD=$(oc get pods -n bench -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$POD" ]; then
    echo "❌ No pod found in bench namespace"
    echo "Check: oc get pods -n bench"
    exit 1
fi

# Create timestamped directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="./superinference_results_${TIMESTAMP}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📥 SuperInference Benchmark Results Download"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Pod: $POD"
echo "Timestamp: $TIMESTAMP"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Downloading All Chunks (auto-detecting)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Auto-detect number of benchmark chunks in the pod
MAX_CHUNKS=$(oc get pod $POD -n bench -o jsonpath='{.spec.containers[*].name}' 2>/dev/null | tr ' ' '\n' | grep -c "benchmark-chunk" || echo "10")
echo "Detected $MAX_CHUNKS benchmark chunks in pod"
echo ""

# Download all chunks with retry logic
download_chunk_with_retry() {
    local chunk_num=$1
    local max_retries=5
    local retry_count=0
    local success=false
    
    while [ $retry_count -lt $max_retries ] && [ "$success" = "false" ]; do
        if [ $retry_count -gt 0 ]; then
            local wait_time=$((2 ** retry_count))  # Exponential backoff: 2, 4, 8, 16 seconds
            echo "  ⏳ Retry $retry_count/$max_retries after ${wait_time}s..."
            sleep $wait_time
        fi
        
        # Try download
        echo -n "  📥 Downloading chunk$chunk_num (attempt $((retry_count + 1))/$max_retries)... "
        
        # Create temp directory
        local temp_dir="${OUTPUT_DIR}/chunk${chunk_num}.tmp"
        rm -rf "$temp_dir"
        
        # Download to temp location (capture output; don't exit on non-zero)
        set +e
        oc cp bench/$POD:/output/chunk$chunk_num "$temp_dir" -c sidecar > /tmp/oc_cp_chunk${chunk_num}.log 2>&1
        cp_status=$?
        set -e
        if [ $cp_status -ne 0 ]; then
            echo "⚠️  oc cp exited with code $cp_status"
            tail -n 10 /tmp/oc_cp_chunk${chunk_num}.log
            retry_count=$((retry_count + 1))
            continue
        fi
        
        # Detect real errors in log while ignoring benign tar warnings
        if grep -qiE "(error|invalid|failed|no such file|permission denied|connection reset|broken pipe|unexpected EOF|timed out)" /tmp/oc_cp_chunk${chunk_num}.log; then
            if ! grep -qi "tar: removing leading '/'" /tmp/oc_cp_chunk${chunk_num}.log; then
                echo "⚠️  Error detected"
                tail -n 10 /tmp/oc_cp_chunk${chunk_num}.log
                retry_count=$((retry_count + 1))
                continue
            fi
        fi
        
        # Verify download
        if [ -d "$temp_dir" ]; then
            local file_count=$(find "$temp_dir" -type f 2>/dev/null | wc -l)
            
            # Expected minimum files per chunk (at least 100 for a valid chunk)
            if [ $file_count -lt 100 ]; then
                echo "⚠️  Too few files ($file_count), incomplete download"
                retry_count=$((retry_count + 1))
                continue
            fi
            
            # Verify critical files exist
            local has_checkpoint=false
            local has_mcp_log=false
            
            if [ -f "$temp_dir/checkpoint_superinference.json" ] || find "$temp_dir" -name "checkpoint_superinference.json" -type f | grep -q .; then
                has_checkpoint=true
            fi
            
            if [ -f "$temp_dir/mcp_server.log" ] || find "$temp_dir" -name "mcp_server.log" -type f | grep -q .; then
                has_mcp_log=true
            fi
            
            if [ "$has_checkpoint" = "false" ]; then
                echo "⚠️  Missing checkpoint file"
                retry_count=$((retry_count + 1))
                continue
            fi
            
            # Download successful - move to final location
            rm -rf "${OUTPUT_DIR}/chunk${chunk_num}"
            mv "$temp_dir" "${OUTPUT_DIR}/chunk${chunk_num}"
            
            local size=$(du -sh "${OUTPUT_DIR}/chunk${chunk_num}" 2>/dev/null | cut -f1)
            echo "✅ $file_count files, $size"
            [ "$has_mcp_log" = "true" ] && echo "     (MCP log: ✅)" || echo "     (MCP log: ⚠️  missing)"
            success=true
        else
            echo "⚠️  Download failed, directory not created"
            retry_count=$((retry_count + 1))
        fi
    done
    
    if [ "$success" = "false" ]; then
        echo "  ❌ FAILED after $max_retries attempts"
        echo "     Check pod status: oc get pod $POD -n bench"
        echo "     Check logs: oc logs $POD -n bench -c sidecar | tail -50"
        return 1
    fi
    
    return 0
}

# Download all chunks with retry
failed_chunks=()
for i in $(seq 1 $MAX_CHUNKS); do
    echo "Chunk $i:"
    if ! download_chunk_with_retry $i; then
        failed_chunks+=($i)
    fi
    echo ""
done

# Report any failures
if [ ${#failed_chunks[@]} -gt 0 ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚠️  DOWNLOAD FAILURES"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Failed chunks: ${failed_chunks[@]}"
    echo "Retry manually: oc cp bench/$POD:/output/chunkN $OUTPUT_DIR/chunkN -c sidecar"
    echo ""
    fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Downloaded Files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# List key files
echo "MCP Server Logs:"
find "$OUTPUT_DIR" -name "mcp_server.log" -exec ls -lh {} \; | awk '{print "  " $9 " - " $5}'

echo ""
echo "Benchmark Logs:"
find "$OUTPUT_DIR" -name "benchmark.log" -exec ls -lh {} \; | awk '{print "  " $9 " - " $5}'

echo ""
echo "Checkpoints:"
find "$OUTPUT_DIR" -name "checkpoint_superinference.json" -exec ls -lh {} \; | awk '{print "  " $9 " - " $5}'

echo ""
echo "Results:"
find "$OUTPUT_DIR" -name "dabstep_results_*.json" -exec ls -lh {} \; | awk '{print "  " $9 " - " $5}'

echo ""
echo "HuggingFace Submissions:"
SUBMISSION_COUNT=$(find "$OUTPUT_DIR" -type d -name "huggingface_submission" | wc -l)
if [ "$SUBMISSION_COUNT" -gt "0" ]; then
    find "$OUTPUT_DIR" -name "answers.jsonl" -o -name "submission_metadata.json" | while read file; do
        ls -lh "$file" | awk '{print "  " $9 " - " $5}'
    done
    echo "  Found in $SUBMISSION_COUNT chunks"
else
    echo "  (Not created yet - generated when each chunk completes all 45 tasks)"
    echo "  Will be in: chunk*/dabstep_run_*/huggingface_submission/"
fi

echo ""
echo "Incremental Results:"
INCREMENTAL=$(find "$OUTPUT_DIR" -type d -name "incremental_results" | wc -l)
echo "  $INCREMENTAL chunks have incremental_results/ (task-by-task checkpoints)"

echo ""
echo "Debug Artifacts:"
DEBUG_TASKS=$(find "$OUTPUT_DIR" -type d -name "task_*" -path "*/debug_artifacts/*" | wc -l)
if [ $DEBUG_TASKS -gt 0 ]; then
    echo "  ✅ $DEBUG_TASKS tasks have debug artifacts (code, output, plan, metrics)"
    echo "  Location: chunk*/debug_artifacts/task_<id>/"
    echo "  Files per task: final_code.py, execution_output.txt, plan_steps.txt, metrics.json, full_response.json"
else
    echo "  (No debug artifacts yet - created as tasks complete)"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Quick Statistics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Summarize results from checkpoints
TOTAL_TASKS=0
TOTAL_CORRECT=0

# Use the same detected chunk count
for i in $(seq 1 $MAX_CHUNKS); do
    CHECKPOINT="$OUTPUT_DIR/chunk$i/checkpoint_superinference.json"
    if [ -f "$CHECKPOINT" ]; then
        TASKS=$(jq '. | length' "$CHECKPOINT" 2>/dev/null || echo "0")
        CORRECT=$(jq '[.[] | select(.correct == true)] | length' "$CHECKPOINT" 2>/dev/null || echo "0")
        TOTAL_TASKS=$((TOTAL_TASKS + TASKS))
        TOTAL_CORRECT=$((TOTAL_CORRECT + CORRECT))
        
        if [ "$TASKS" -gt "0" ]; then
            ACC=$(awk "BEGIN {printf \"%.1f\", ($CORRECT / $TASKS) * 100}")
            echo "Chunk $i: $TASKS tasks completed, $CORRECT correct ($ACC%)"
        fi
    fi
done

echo ""
if [ $TOTAL_TASKS -gt 0 ]; then
    OVERALL_ACC=$(awk "BEGIN {printf \"%.1f\", ($TOTAL_CORRECT / $TOTAL_TASKS) * 100}")
    # Calculate expected total based on chunk count (auto-detected)
    EXPECTED_TOTAL=$((MAX_CHUNKS * 113))  # Average ~113 tasks per chunk for balanced distribution
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🎯 OVERALL RESULTS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Total Tasks: $TOTAL_TASKS / $EXPECTED_TOTAL (detected from $MAX_CHUNKS chunks)"
    echo "Accuracy: $OVERALL_ACC%"
    echo "  ✅ Correct: $TOTAL_CORRECT"
    echo "  ❌ Incorrect: $((TOTAL_TASKS - TOTAL_CORRECT))"
    echo ""
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Creating Unified HuggingFace Submission"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Auto-detect number of chunks
DETECTED_CHUNKS=$(ls -d "$OUTPUT_DIR"/chunk* 2>/dev/null | wc -l)
echo "Detected $DETECTED_CHUNKS chunks to merge"
echo ""

# Create merged HuggingFace submission directory
MERGED_DIR="$OUTPUT_DIR/huggingface_submission"
mkdir -p "$MERGED_DIR"

# Merge all answers.jsonl files
echo "Merging answers from all chunks..."
MERGED_ANSWERS="$MERGED_DIR/answers.jsonl"
> "$MERGED_ANSWERS"  # Clear file

TOTAL_MERGED=0
for i in $(seq 1 $DETECTED_CHUNKS); do
    CHUNK_ANSWERS="$OUTPUT_DIR/chunk$i/dabstep_run_*/huggingface_submission/answers.jsonl"
    if ls $CHUNK_ANSWERS 1>/dev/null 2>&1; then
        CHUNK_LINES=$(cat $CHUNK_ANSWERS 2>/dev/null | wc -l)
        cat $CHUNK_ANSWERS >> "$MERGED_ANSWERS" 2>/dev/null
        TOTAL_MERGED=$((TOTAL_MERGED + CHUNK_LINES))
        echo "  Chunk $i: $CHUNK_LINES answers"
    else
        echo "  Chunk $i: ⚠️  No submission file yet"
    fi
done

echo ""
if [ $TOTAL_MERGED -gt 0 ]; then
    echo "✅ Merged $TOTAL_MERGED answers into: $MERGED_ANSWERS"
    
    # Create merged metadata
    echo ""
    echo "Creating merged submission metadata..."
    
    # Calculate aggregate stats
    MERGED_ACCURACY=$(awk "BEGIN {printf \"%.4f\", $TOTAL_CORRECT / $TOTAL_TASKS}")
    
    # Extract model configuration from environment variables (with defaults)
    DEFAULT_PROVIDER="${DEFAULT_PROVIDER:-gemini}"
    
    # Get model names based on provider
    if [ "$DEFAULT_PROVIDER" = "gemini" ]; then
        INFERENCE_MODEL="${GEMINI_MODEL:-gemini-2.5-pro}"
        CRITIC_MODEL="${GEMINI_CRITIC_MODEL:-gemini-2.5-flash-lite}"
        EMBEDDING_MODEL="${GEMINI_EMBEDDING_MODEL:-gemini-embedding-001}"
        PROVIDER_DISPLAY="Gemini"
    elif [ "$DEFAULT_PROVIDER" = "openai" ]; then
        INFERENCE_MODEL="${OPENAI_MODEL:-gpt-4}"
        CRITIC_MODEL="${OPENAI_CRITIC_MODEL:-gpt-3.5-turbo}"
        EMBEDDING_MODEL="${OPENAI_EMBEDDING_MODEL:-text-embedding-ada-002}"
        PROVIDER_DISPLAY="OpenAI"
    elif [ "$DEFAULT_PROVIDER" = "deepseek" ]; then
        INFERENCE_MODEL="${DEEPSEEK_MODEL:-deepseek-chat}"
        CRITIC_MODEL="${DEEPSEEK_CRITIC_MODEL:-deepseek-chat}"
        EMBEDDING_MODEL="${DEEPSEEK_EMBEDDING_MODEL:-none}"
        PROVIDER_DISPLAY="DeepSeek"
    elif [ "$DEFAULT_PROVIDER" = "vllm" ]; then
        INFERENCE_MODEL="${VLLM_MODEL:-meta-llama/Llama-3.3-70B-Instruct}"
        CRITIC_MODEL="${VLLM_CRITIC_MODEL:-meta-llama/Llama-3.3-70B-Instruct}"
        EMBEDDING_MODEL="${VLLM_EMBEDDING_MODEL:-none}"
        PROVIDER_DISPLAY="vLLM"
    else
        # Fallback to Gemini defaults
        INFERENCE_MODEL="${GEMINI_MODEL:-gemini-2.5-pro}"
        CRITIC_MODEL="${GEMINI_CRITIC_MODEL:-gemini-2.5-flash-lite}"
        EMBEDDING_MODEL="${GEMINI_EMBEDDING_MODEL:-gemini-embedding-001}"
        PROVIDER_DISPLAY="Gemini"
    fi
    
    # Construct model_name from inference model
    # Handle model names with slashes (e.g., "meta-llama/Llama-3.3-70B-Instruct")
    MODEL_BASE=$(echo "$INFERENCE_MODEL" | cut -d'/' -f2)
    if [ -z "$MODEL_BASE" ]; then
        MODEL_BASE="$INFERENCE_MODEL"
    fi
    
    # Convert "gemini-2.5-pro" -> "Gemini-2.5-Pro"
    # Preserve numbers and common abbreviations
    MODEL_NAME_PARTS=$(echo "$MODEL_BASE" | tr '-' ' ' | awk '{
        for(i=1;i<=NF;i++) {
            word = $i
            # Check if it is a number like 2.5 or 70B
            if (word ~ /^[0-9]+\.?[0-9]*[A-Za-z]*$/) {
                $i = word  # Keep numbers as-is
            } else {
                # Capitalize first letter, lowercase rest
                $i = toupper(substr(word,1,1)) tolower(substr(word,2))
            }
        }
        print
    }')
    MODEL_NAME_DISPLAY=$(echo "$MODEL_NAME_PARTS" | tr ' ' '-')
    MODEL_NAME_FULL="SuperInference-${PROVIDER_DISPLAY}-${MODEL_NAME_DISPLAY}"
    
    # Create model description
    MODEL_DESCRIPTION="SuperInference with Unified SUPER-INFERENCE framework using ${PROVIDER_DISPLAY} ${MODEL_NAME_PARTS}"
    
    echo "  📋 Model Configuration:"
    echo "     Provider: $DEFAULT_PROVIDER ($PROVIDER_DISPLAY)"
    echo "     Inference Model: $INFERENCE_MODEL"
    echo "     Critic Model: $CRITIC_MODEL"
    echo "     Embedding Model: $EMBEDDING_MODEL"
    echo "     Model Name: $MODEL_NAME_FULL"
    
    # Create merged metadata JSON
    cat > "$MERGED_DIR/submission_metadata.json" << EOF
{
  "model_name": "$MODEL_NAME_FULL",
  "model_description": "$MODEL_DESCRIPTION",
  "submission_timestamp": "$TIMESTAMP",
  "framework": "SuperInference MCP Server",
  "total_tasks": $TOTAL_MERGED,
  "chunks_merged": $DETECTED_CHUNKS,
  "methods_used": ["superinference"],
  "performance_summary": {
    "overall_accuracy": $MERGED_ACCURACY,
    "correct_answers": $TOTAL_CORRECT,
    "incorrect_answers": $((TOTAL_TASKS - TOTAL_CORRECT))
  },
  "technical_details": {
    "architecture": "SuperInference - Supervised Inference for Partially Observable Environments",
    "inference_model": "$INFERENCE_MODEL",
    "critic_model": "$CRITIC_MODEL",
    "vector_embeddings": "$EMBEDDING_MODEL",
    "code_execution": "Server-side Python execution with safety checks",
    "parallel_chunks": $DETECTED_CHUNKS
  }
}
EOF
    
    echo "✅ Merged metadata created: $MERGED_DIR/submission_metadata.json"
    
    # Create README
    cat > "$MERGED_DIR/README.md" << 'EOFREADME'
# SuperInference HuggingFace Submission

This directory contains the merged results from all benchmark chunks.

## Files

- **answers.jsonl**: All task answers in HuggingFace format
- **submission_metadata.json**: Combined statistics and configuration
- **README.md**: This file

## Format

Each line in `answers.jsonl` contains:
```json
{
  "task_id": "123",
  "agent_answer": "the answer",
  "reasoning_trace": "explanation"
}
```

## Submission

To submit to DABStep leaderboard:
1. Verify all tasks are present: `wc -l answers.jsonl`
2. Upload to HuggingFace dataset following DABStep guidelines
3. Include submission_metadata.json for reproducibility

## Statistics

See `submission_metadata.json` for detailed performance metrics.
EOFREADME
    
    echo "✅ README created: $MERGED_DIR/README.md"
else
    echo "⚠️  No answers to merge yet - chunks may still be running"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Download Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo "Total size: $(du -sh "$OUTPUT_DIR" | cut -f1)"
echo ""

if [ $TOTAL_MERGED -gt 0 ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🎯 UNIFIED SUBMISSION READY FOR HUGGINGFACE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📄 Submission file: $MERGED_DIR/answers.jsonl"
    echo "   Total answers: $TOTAL_MERGED"
    echo "   Accuracy: $(awk "BEGIN {printf \"%.1f%%\", $MERGED_ACCURACY * 100}")"
    echo "   Correct: $TOTAL_CORRECT / $TOTAL_TASKS"
    echo ""
    echo "📊 Metadata: $MERGED_DIR/submission_metadata.json"
    echo ""
    echo "To submit:"
    echo "  cd $MERGED_DIR"
    echo "  # Upload answers.jsonl to HuggingFace dataset"
    echo ""
fi

echo "Next steps:"
echo "  1. Review merged submission: cat $MERGED_DIR/answers.jsonl | head -20"
echo "  2. Check metadata: cat $MERGED_DIR/submission_metadata.json"
echo "  3. Review MCP logs: ls $OUTPUT_DIR/chunk*/mcp_server.log"
echo "  4. Review benchmark logs: ls $OUTPUT_DIR/chunk*/benchmark.log"
echo "  5. Analyze debug artifacts: ls $OUTPUT_DIR/chunk*/debug_artifacts/task_*/"
echo "  6. Compare failed tasks: grep '❌ Incorrect' $OUTPUT_DIR/chunk*/dabstep_run*/dabstep_benchmark*.log"
echo ""
