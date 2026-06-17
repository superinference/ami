# Copyright (C) 2025 SuperInference contributors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0

# openshell-ami: SuperInference AMI agent on OpenShell Community base
#
# Built on the NVIDIA OpenShell Community base image which provides
# Python 3.14.3 (uv), build-essential, git, gh, and more.
# The base ships Node.js 22, but AMI CLI requires Node >= 26, so this
# image upgrades the toolchain before installing the agent binary.
# The base ships with uv and npm — the agent uses these to install
# additional dependencies on demand into the writable /sandbox volume,
# even when the container rootfs is read-only.
#
# AMI binary installed from superinference.org at build time (Apache 2.0).
# Runs exclusively in detached mode — no REPL, no TUI.

ARG BASE_IMAGE=ghcr.io/nvidia/openshell-community/sandboxes/base:latest
FROM ${BASE_IMAGE}

# ── Node.js 22 → 26 upgrade ──────────────────────────────────────────
# The OpenShell Community base ships Node.js 22 (LTS) via NodeSource.
# AMI CLI requires Node >= 26 for native TypeScript type-stripping,
# stable node:test enhancements, and URLPattern support.
#
# Fully replace the 22.x toolchain: purge the old package, remove
# stale global node_modules (native addons compiled against the
# Node 22 ABI), swap the NodeSource apt source, and install 26.x.
# npm is pinned to 11.11.0 to match the base image convention.
USER root
RUN apt-get purge -y nodejs && \
    rm -rf /usr/lib/node_modules \
           /etc/apt/sources.list.d/nodesource.list \
           /etc/apt/keyrings/nodesource.gpg \
           /usr/share/keyrings/nodesource-repo.gpg-armored.gpg && \
    curl -fsSL https://deb.nodesource.com/setup_26.x | bash - && \
    apt-get install -y --no-install-recommends nodejs jq && \
    npm install -g npm@11.11.0 && \
    rm -rf /var/lib/apt/lists/* && \
    node --version | grep -qE '^v26\.' || \
        { echo "FATAL: expected Node 26, got $(node --version)" >&2; exit 1; }

# AMI-specific directories
USER sandbox
RUN mkdir -p /sandbox/.config/superinference \
             /sandbox/.superinference

# Install AMI binary (Apache 2.0 — safe to redistribute)
RUN curl -fsSL https://www.superinference.org/install.sh | bash

# Agent install script for OpenShell entrypoint discovery
USER root
RUN mkdir -p /etc/openshell/agents
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Default policy for OpenShell sandbox
COPY policy.yaml /etc/openshell/policy.yaml

# ── OpenShift / arbitrary UID support ────────────────────────────────
# OpenShift assigns random UIDs in GID 0 (root group). All writable
# paths must be group-writable and owned by GID 0 so any UID can write.
RUN chgrp -R 0 /sandbox && chmod -R g=u /sandbox && \
    chmod -R g+w /sandbox/.local /sandbox/.config /sandbox/.superinference \
                 /sandbox/.venv /sandbox/.uv /sandbox/.npm 2>/dev/null; \
    chmod g+w /sandbox

# OCI labels for operator discovery
LABEL io.openshell.sandbox.harness="ami" \
      io.openshell.sandbox.runtime="binary" \
      io.openshell.sandbox.license="Apache-2.0" \
      org.opencontainers.image.title="openshell-ami" \
      org.opencontainers.image.description="SuperInference AMI autonomous coding agent for OpenShell" \
      org.opencontainers.image.source="https://github.com/superinference/site" \
      org.opencontainers.image.licenses="Apache-2.0"

USER sandbox
WORKDIR /sandbox
ENV PATH="/sandbox/.local/bin:${PATH}" \
    HOME=/sandbox \
    AGENT_NAME=ami \
    UV_NO_SANDBOX=1 \
    UV_CACHE_DIR=/sandbox/.cache/uv \
    PIP_CACHE_DIR=/sandbox/.cache/pip \
    PIP_NO_BUILD_ISOLATION=1 \
    npm_config_cache=/sandbox/.cache/npm

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["ami"]
