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

# Dockerfile for SuperInference Agent
# Unified SuperInference-STAR implementation with full DABStep benchmark support
# Optimized for Kubernetes deployment

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    libgomp1 \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY mcp/requirements.txt /app/agent_requirements.txt
COPY benchmark/requirements.txt /app/benchmark_requirements.txt

# Install Python dependencies
# Install agent requirements first, then benchmark (some overlap is ok)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r agent_requirements.txt && \
    pip install --no-cache-dir -r benchmark_requirements.txt

# Copy all
COPY . .

# Set environment variables with defaults
ENV DEFAULT_PROVIDER=vllm \
    DEFAULT_TEMPERATURE=0.1 \
    DEFAULT_MAX_TOKENS=100000 \
    DEFAULT_TOP_P=0.8 \
    DEFAULT_TOP_K=40 \
    BENCHMARK_MODE=true \
    PYTHONUNBUFFERED=1

# Create necessary directories with permissions for OpenShift non-root users
RUN mkdir -p /app/benchmark/dabstep/results \
             /app/benchmark/dabstep/data/context \
             /app/logs && \
    chmod -R 777 /app/benchmark/dabstep/data \
                 /app/benchmark/dabstep/results \
                 /app/logs

# Set matplotlib config directory to /tmp (writable in OpenShift)
ENV MPLCONFIGDIR=/tmp/matplotlib

# Expose MCP server port (for server mode)
EXPOSE 3000

# No USER - OpenShift assigns non-root UID automatically
# No ENTRYPOINT - Kubernetes job will specify command
CMD ["/bin/bash"]

