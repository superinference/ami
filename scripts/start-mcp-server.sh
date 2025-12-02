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

# SuperInference MCP Server Startup Script
# Starts the MCP server with proper environment and configuration

set -e

# Get script directory and navigate to mcp folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MCP_DIR="$(cd "$SCRIPT_DIR/../mcp" && pwd)"

# Change to mcp directory
cd "$MCP_DIR"

# Kill any process using port 3000 (MCP server default port)
echo "🔍 Checking for processes on port 3000..."
PORT=3000

# Temporarily disable exit on error for port checking
set +e

# Try lsof first (most common on Linux)
PID=$(lsof -ti:$PORT 2>/dev/null)

# Fallback to fuser if lsof didn't find anything
if [ -z "$PID" ]; then
    PID=$(fuser $PORT/tcp 2>/dev/null | awk '{print $1}')
fi

# Re-enable exit on error
set -e

if [ ! -z "$PID" ] && [ -n "$PID" ]; then
    echo "⚠️  Found process $PID using port $PORT, killing it..."
    kill -9 $PID 2>/dev/null || true
    sleep 1
    echo "✅ Port $PORT is now free"
else
    echo "✅ Port $PORT is available"
fi

echo "🚀 Starting SuperInference MCP Server..."

# Check command line arguments
MODE=${1:-http}

if [ "$MODE" = "stdio" ]; then
    echo "🔌 Starting MCP server in STDIO mode..."
    python3 mcp_server.py
elif [ "$MODE" = "http" ]; then
    echo "🌐 Starting MCP server in HTTP mode..."
    python3 mcp_server.py --http
else
    echo "❌ Invalid mode: $MODE"
    echo "Usage: $0 [http|stdio]"
    exit 1
fi 