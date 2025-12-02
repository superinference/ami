#!/bin/bash
# Monitor SuperInference Benchmark Progress with Detailed Logs

POD=$(oc get pods -n bench -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$POD" ]; then
    echo "❌ No pod found in bench namespace"
    exit 1
fi

# Argument for detail level
DETAIL=${1:-summary}  # summary, full, or -f for continuous

show_status() {
    clear
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 SuperInference Benchmark Monitor - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Pod: $POD"
    echo ""
    
    echo "Job & Pod Status:"
    oc get job,pod -n bench 2>/dev/null | grep superinference
    echo ""
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📈 CHUNK PROGRESS & LOGS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    TOTAL_CORRECT=0
    TOTAL_INCORRECT=0
    TOTAL_FILE_ERRORS=0
    TOTAL_EXEC_ERRORS=0
    TOTAL_TASKS_DETECTED=0
    
    # Auto-detect number of chunks by checking which containers exist
    CHUNK_COUNT=$(oc get pod $POD -n bench -o jsonpath='{.spec.containers[*].name}' 2>/dev/null | tr ' ' '\n' | grep -c "benchmark-chunk" || echo "4")
    
    for i in $(seq 1 $CHUNK_COUNT); do
        PORT=$((3000 + i))
        
        # Auto-detect chunk size from logs (look for "Task X/Y" pattern)
        # Extract the maximum Y value which is the chunk size
        CHUNK_SIZE=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep -oE "Task [0-9]+/[0-9]+" | grep -oE "/[0-9]+" | tr -d '/' | sort -n | tail -1)
        
        # If we couldn't detect from logs, try to infer from container args
        if [ -z "$CHUNK_SIZE" ] || [ "$CHUNK_SIZE" -eq "0" ]; then
            # Extract from job args: --start-index X --end-index Y
            START_IDX=$(oc get pod $POD -n bench -o jsonpath="{.spec.containers[?(@.name=='benchmark-chunk-$i')].args[0]}" 2>/dev/null | grep -oE "start-index [0-9]+" | grep -oE "[0-9]+")
            END_IDX=$(oc get pod $POD -n bench -o jsonpath="{.spec.containers[?(@.name=='benchmark-chunk-$i')].args[0]}" 2>/dev/null | grep -oE "end-index [0-9]+" | grep -oE "[0-9]+")
            
            if [ -n "$START_IDX" ] && [ -n "$END_IDX" ]; then
                CHUNK_SIZE=$((END_IDX - START_IDX))
            else
                # Fallback: assume equal distribution
                CHUNK_SIZE=113
            fi
        fi
        
        TOTAL_TASKS_DETECTED=$((TOTAL_TASKS_DETECTED + CHUNK_SIZE))
        
        # Get completion stats
        CORRECT=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep "✅ Correct" | wc -l)
        INCORRECT=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep "❌ Incorrect" | wc -l)
        COMPLETED=$((CORRECT + INCORRECT))
        
        # Check for errors
        FILE_ERRORS=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep "No such file or directory" | wc -l)
        EXEC_ERRORS=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep "EXECUTION ERROR" | wc -l)
        
        TOTAL_CORRECT=$((TOTAL_CORRECT + CORRECT))
        TOTAL_INCORRECT=$((TOTAL_INCORRECT + INCORRECT))
        TOTAL_FILE_ERRORS=$((TOTAL_FILE_ERRORS + FILE_ERRORS))
        TOTAL_EXEC_ERRORS=$((TOTAL_EXEC_ERRORS + EXEC_ERRORS))
        
        # Get current task (adjust pattern for task count)
        LATEST_TASK=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep "Task [0-9]" | tail -1)
        
        # Check for MCP errors
        MCP_ERRORS=$(oc exec $POD -c sidecar -n bench -- tail -20 /output/chunk$i/mcp_server.log 2>/dev/null | grep -i "error\|timeout\|failed" | wc -l || echo "0")
        
        # Header with error indicators
        echo "┌─────────────────────────────────────────────────────────┐"
        printf "│ CHUNK %2d (Port %d) - %3d/%3d tasks (✅%3d ❌%2d)  │\n" $i $PORT $COMPLETED $CHUNK_SIZE $CORRECT $INCORRECT
        if [ "$FILE_ERRORS" -gt "0" ] || [ "$EXEC_ERRORS" -gt "0" ] || [ "$MCP_ERRORS" -gt "0" ]; then
            printf "│ ⚠️  ERRORS: File: %d, Exec: %d, MCP: %d              │\n" $FILE_ERRORS $EXEC_ERRORS $MCP_ERRORS
        fi
        echo "└─────────────────────────────────────────────────────────┘"
        
        if [ "$DETAIL" = "full" ] || [ "$DETAIL" = "-f" ] || [ "$DETAIL" = "--follow" ]; then
            # Show benchmark tail
            echo "  📋 Benchmark (last 50 lines):"
            oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | tail -50 | sed 's/^/    /'
            echo ""
            
            # Show MCP server tail
            echo "  🔧 MCP Server (last 50 lines):"
            oc exec $POD -c sidecar -n bench -- tail -50 /output/chunk$i/mcp_server.log 2>&1 | grep -v "tar:" | sed 's/^/    /' || echo "    (Log not available yet)"
            echo ""
            
            # Show MCP log size and error count
            MCP_SIZE=$(oc exec $POD -c sidecar -n bench -- wc -c /output/chunk$i/mcp_server.log 2>/dev/null | awk '{printf "%.1fMB", $1/1024/1024}' || echo "0MB")
            MCP_LINE_COUNT=$(oc exec $POD -c sidecar -n bench -- wc -l /output/chunk$i/mcp_server.log 2>/dev/null | awk '{print $1}' || echo "0")
            echo "  📊 MCP Stats: $MCP_SIZE ($MCP_LINE_COUNT lines)"
            
            # Show recent errors if any
            if [ "$MCP_ERRORS" -gt "0" ]; then
                echo "  ⚠️  Recent MCP Errors (from last 50 lines):"
                oc exec $POD -c sidecar -n bench -- tail -50 /output/chunk$i/mcp_server.log 2>/dev/null | grep -i "error\|timeout\|failed" | tail -5 | sed 's/^/    /' || echo "    (None detected)"
            fi
        else
            # Summary mode - show current task and brief log tails
            if [ -n "$LATEST_TASK" ]; then
                TASK_NUM=$(echo "$LATEST_TASK" | grep -oE "Task [0-9]+/[0-9]+")
                TASK_ID=$(echo "$LATEST_TASK" | grep -oE ": [0-9]+" | tr -d ': ')
                echo "  Current: $TASK_NUM (ID: $TASK_ID)"
            else
                PHASE=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | grep -E "Normalizing|SUPER-INFERENCE|Populating|Server.*healthy" | tail -1 | cut -c50-100)
                echo "  Setup: ${PHASE}..."
            fi
            
            # Show last benchmark line
            LAST_BENCH=$(oc logs $POD -c benchmark-chunk-$i -n bench 2>&1 | tail -1 | cut -c1-70)
            echo "  📋 Latest: ${LAST_BENCH}..."
            
            # Show last MCP line
            LAST_MCP=$(oc exec $POD -c sidecar -n bench -- tail -1 /output/chunk$i/mcp_server.log 2>/dev/null | cut -c1-70 || echo "N/A")
            echo "  🔧 MCP: ${LAST_MCP}..."
        fi
        echo ""
    done
    
    # Overall statistics
    TOTAL=$((TOTAL_CORRECT + TOTAL_INCORRECT))
    
    # Use detected total, or fallback to a reasonable estimate
    if [ "$TOTAL_TASKS_DETECTED" -gt "0" ]; then
        TOTAL_TASKS=$TOTAL_TASKS_DETECTED
    else
        # Fallback: assume 4 chunks with ~113 tasks each
        TOTAL_TASKS=$((CHUNK_COUNT * 113))
    fi
    
    REMAINING=$((TOTAL_TASKS - TOTAL))
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🎯 OVERALL STATISTICS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Configuration: $CHUNK_COUNT chunks, $TOTAL_TASKS total tasks (auto-detected)"
    echo ""
    
    if [ $TOTAL -gt 0 ]; then
        # Calculate accuracy
        ACC=$(awk "BEGIN {printf \"%.1f\", ($TOTAL_CORRECT / $TOTAL) * 100}")
        COMPLETION_PCT=$(awk "BEGIN {printf \"%.1f\", ($TOTAL / $TOTAL_TASKS) * 100}")
        
        # Progress bar (ASCII characters for universal compatibility)
        PROGRESS_FILLED=$((TOTAL * 50 / TOTAL_TASKS))
        PROGRESS_EMPTY=$((50 - PROGRESS_FILLED))
        PROGRESS_BAR=$(printf "%${PROGRESS_FILLED}s" | tr ' ' '#')
        PROGRESS_BAR="${PROGRESS_BAR}$(printf "%${PROGRESS_EMPTY}s" | tr ' ' '.')"
        
        echo "Progress: [$PROGRESS_BAR] $COMPLETION_PCT%"
        echo "Completed: $TOTAL/$TOTAL_TASKS tasks ($REMAINING remaining)"
        echo "Accuracy: $ACC% ($TOTAL_CORRECT correct, $TOTAL_INCORRECT incorrect)"
        echo ""
        
        # Calculate runtime and ETA
        POD_START=$(oc get pod $POD -n bench -o jsonpath='{.status.startTime}' 2>/dev/null)
        if [ -n "$POD_START" ]; then
            START_SEC=$(date -d "$POD_START" +%s 2>/dev/null || echo "0")
            NOW_SEC=$(date +%s)
            ELAPSED=$((NOW_SEC - START_SEC))
            ELAPSED_HOURS=$((ELAPSED / 3600))
            ELAPSED_MINS=$(((ELAPSED % 3600) / 60))
            ELAPSED_SECS=$((ELAPSED % 60))
            
            if [ $ELAPSED -gt 0 ]; then
                # Calculate pace and ETA
                TASKS_PER_HOUR=$(awk "BEGIN {printf \"%.2f\", ($TOTAL / $ELAPSED) * 3600}")
                TASKS_PER_MIN=$(awk "BEGIN {printf \"%.2f\", ($TOTAL / $ELAPSED) * 60}")
                AVG_SECONDS_PER_TASK=$(awk "BEGIN {printf \"%.1f\", $ELAPSED / $TOTAL}")
                
                # Estimate remaining time
                ETA_SECS=$(awk "BEGIN {printf \"%.0f\", $REMAINING * $AVG_SECONDS_PER_TASK}")
                ETA_HOURS=$((ETA_SECS / 3600))
                ETA_MINS=$(((ETA_SECS % 3600) / 60))
                
                # Calculate estimated completion time
                COMPLETION_TIMESTAMP=$((NOW_SEC + ETA_SECS))
                COMPLETION_TIME=$(date -d "@$COMPLETION_TIMESTAMP" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "N/A")
                
                echo "⏱️  TIMING ANALYSIS"
                echo "Elapsed time: ${ELAPSED_HOURS}h ${ELAPSED_MINS}m ${ELAPSED_SECS}s"
                echo "Execution pace: $TASKS_PER_MIN tasks/min ($TASKS_PER_HOUR tasks/hour)"
                echo "Avg time per task: ${AVG_SECONDS_PER_TASK}s"
                echo ""
                echo "📅 ESTIMATED COMPLETION"
                echo "Remaining time: ~${ETA_HOURS}h ${ETA_MINS}m"
                echo "Expected finish: $COMPLETION_TIME"
            fi
        fi
        
        # Show errors if any
        if [ $TOTAL_FILE_ERRORS -gt 0 ] || [ $TOTAL_EXEC_ERRORS -gt 0 ]; then
            echo ""
            echo "⚠️  ERROR SUMMARY:"
            if [ $TOTAL_FILE_ERRORS -gt 0 ]; then
                echo "   File errors: $TOTAL_FILE_ERRORS occurrences"
                echo "   → Check DATA_CONTEXT_DIR and volume mounts"
            fi
            if [ $TOTAL_EXEC_ERRORS -gt 0 ]; then
                echo "   Execution errors: $TOTAL_EXEC_ERRORS occurrences"
                echo "   → Check: oc logs $POD -c benchmark-chunk-1 -n bench | grep 'EXECUTION ERROR' | tail -1"
            fi
        fi
    else
        echo "No tasks completed yet - all chunks in setup phase"
        echo ""
        echo "⏳ Waiting for tasks to start..."
        POD_START=$(oc get pod $POD -n bench -o jsonpath='{.status.startTime}' 2>/dev/null)
        if [ -n "$POD_START" ]; then
            START_SEC=$(date -d "$POD_START" +%s 2>/dev/null || echo "0")
            NOW_SEC=$(date +%s)
            ELAPSED=$((NOW_SEC - START_SEC))
            ELAPSED_MINS=$((ELAPSED / 60))
            echo "Setup time: ${ELAPSED_MINS}m (pods initializing...)"
        fi
    fi
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
}

# Main execution
if [ "$DETAIL" = "-f" ] || [ "$DETAIL" = "--follow" ]; then
    echo "📊 Continuous monitoring mode (Ctrl+C to exit)"
    echo "Refreshing every 30 seconds..."
    echo ""
    while true; do
        show_status
        echo "Next update in 30 seconds... (Ctrl+C to exit)"
        sleep 30
    done
else
    show_status
    echo "Usage:"
    echo "  ./monitor_benchmark.sh           # Summary view"
    echo "  ./monitor_benchmark.sh full      # Detailed with log tails"
    echo "  ./monitor_benchmark.sh -f        # Continuous mode (summary)"
    echo ""
    echo "Other commands:"
    echo "  Download results: ./download_results.sh"
    echo "  Live chunk 1: oc logs $POD -c benchmark-chunk-1 -n bench -f | grep 'Task\\|Correct'"
    echo "  MCP server: oc exec $POD -c sidecar -n bench -- tail -f /output/chunk1/mcp_server.log"
fi
