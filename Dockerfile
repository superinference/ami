# Copyright (C) 2025 SuperInference contributors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0

# openshell-ami: SuperInference AMI agent, baked in (Apache 2.0)
# Follows the OpenShell thin-base + flavor pattern.
# Binary installed from superinference.org at build time (license-clean).
# Runs exclusively in detached mode — no REPL, no TUI.
# Target size: 200-250 MB

ARG BASE_IMAGE=registry.access.redhat.com/ubi10/ubi-minimal:latest
FROM ${BASE_IMAGE}

ARG TARGETARCH

# System deps + dev toolchain for coding agent tasks
USER root
RUN microdnf install -y --nodocs --setopt=install_weak_deps=0 \
        ca-certificates \
        curl \
        wget \
        git \
        jq \
        tar \
        gzip \
        xz \
        unzip \
        zip \
        libatomic \
        procps-ng \
        shadow-utils \
        findutils \
        diffutils \
        patch \
        make \
        gcc \
        gcc-c++ \
        python3 \
        python3-pip \
    && microdnf clean all \
    && rm -rf /var/cache/yum

# Node.js runtime — needed by the agent to run tests, scripts, etc.
ARG NODE_VERSION=26.3.0
RUN ARCH=$(uname -m | sed 's/x86_64/x64/;s/aarch64/arm64/') \
    && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${ARCH}.tar.xz" \
       | tar -xJ --strip-components=1 -C /usr/local \
    && node --version && npm --version

# Users: supervisor (system, non-login) and sandbox (interactive)
RUN useradd -r -s /usr/sbin/nologin supervisor \
    && useradd -m -s /bin/bash -d /sandbox sandbox

# Sandbox home structure
RUN mkdir -p /sandbox/.local/bin \
             /sandbox/.config/superinference \
             /sandbox/.superinference \
    && chown -R sandbox:sandbox /sandbox

# Install AMI binary (Apache 2.0 — safe to bake in)
USER sandbox
RUN curl -fsSL https://www.superinference.org/install.sh | bash

# Agent install script for OpenShell entrypoint discovery
USER root
RUN mkdir -p /etc/openshell/agents
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Default policy for OpenShell sandbox
COPY policy.yaml /etc/openshell/policy.yaml

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
    AGENT_NAME=ami

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["ami"]
