#!/bin/bash
set -euo pipefail

AGENT="${1:-${AGENT_NAME:-bash}}"

case "$AGENT" in
    bash)
        shift 2>/dev/null || true
        exec /bin/bash "$@"
        ;;
    ami)
        if ! command -v ami >/dev/null 2>&1; then
            echo "[openshell] AMI not found, installing..." >&2
            curl -fsSL https://www.superinference.org/install.sh | bash
        fi

        # Signal readiness to kubelet / kagenti operator
        touch /tmp/agent-ready

        shift 2>/dev/null || true

        # Default to detached mode if --prompt is not already in args
        if [ $# -eq 0 ]; then
            if [ -n "${AGENT_PROMPT:-}" ]; then
                exec ami --prompt "$AGENT_PROMPT" \
                         --yolo \
                         --output-format jsonl
            else
                echo "[openshell] AMI ready. Provide --prompt or set AGENT_PROMPT." >&2
                exec ami --help
            fi
        else
            exec ami "$@"
        fi
        ;;
    *)
        if command -v "$AGENT" >/dev/null 2>&1; then
            exec "$@"
        else
            echo "[openshell] Unknown agent: ${AGENT}" >&2
            echo "[openshell] Available: ami, bash" >&2
            exit 1
        fi
        ;;
esac
