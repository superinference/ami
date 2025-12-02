#!/bin/bash
# Copyright (C) 2025 SuperInference contributors
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program. If not, see https://www.gnu.org/licenses/.

# SuperInference Benchmark Runner Script
# Runs the DABStep benchmark with proper environment and configuration

set -e

# Get script directory and navigate to benchmark folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/../benchmark/dabstep" && pwd)"

# Change to benchmark directory
cd "$BENCHMARK_DIR"

# Set benchmark mode environment variable
export BENCHMARK_MODE=true

# Default values
DEFAULT_PROBLEMS=10
DEFAULT_DIFFICULTY="both"
DEFAULT_MCP_PORT=3000

# Parse command line arguments
PROBLEMS=${1:-$DEFAULT_PROBLEMS}
DIFFICULTY=${2:-$DEFAULT_DIFFICULTY}
MCP_PORT=${3:-$DEFAULT_MCP_PORT}
OUTPUT_DIR=${4:-}

# Generate timestamped output directory if not provided
if [ -z "$OUTPUT_DIR" ]; then
    # Create timestamped directory: results/dabstep_results_YYYYMMDD_HHMMSS/
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    OUTPUT_DIR="$BENCHMARK_DIR/results/dabstep_results_${TIMESTAMP}"
elif [[ ! "$OUTPUT_DIR" =~ ^/ ]]; then
    # Relative path: make it relative to benchmark directory
    OUTPUT_DIR="$BENCHMARK_DIR/$OUTPUT_DIR"
fi
# Absolute paths are used as-is

# Validate difficulty
if [[ ! "$DIFFICULTY" =~ ^(easy|hard|both)$ ]]; then
    echo "❌ Invalid difficulty: $DIFFICULTY"
    echo "Usage: $0 [problems] [difficulty] [mcp-port] [output-dir]"
    echo "  problems: Number of problems per difficulty level (default: $DEFAULT_PROBLEMS)"
    echo "  difficulty: easy, hard, or both (default: $DEFAULT_DIFFICULTY)"
    echo "  mcp-port: Port for MCP server (default: $DEFAULT_MCP_PORT)"
    echo "  output-dir: Output directory for results (default: auto-generated timestamped dir in results/)"
    exit 1
fi

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 not found"
    echo "Please install Python 3 to run the benchmark"
    exit 1
fi

# Check if required Python packages are installed
echo "🔍 Checking Python dependencies..."
if ! python3 -c "import datasets" 2>/dev/null; then
    echo "⚠️  Warning: 'datasets' package not found"
    echo "💡 Install with: pip install datasets huggingface_hub"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Display configuration
echo "🚀 Starting SuperInference DABStep Benchmark..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Configuration:"
echo "   Problems:      $PROBLEMS per difficulty level"
echo "   Difficulty:    $DIFFICULTY"
echo "   MCP Port:      $MCP_PORT"
echo "   Output Dir:    $OUTPUT_DIR"
echo "   BENCHMARK_MODE: $BENCHMARK_MODE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Run the benchmark
python3 dabstep_benchmark.py \
    --problems "$PROBLEMS" \
    --difficulty "$DIFFICULTY" \
    --mcp-port "$MCP_PORT" \
    --output-dir "$OUTPUT_DIR"

echo ""
echo "✅ Benchmark completed!"
echo "📁 Results saved to: $OUTPUT_DIR"

