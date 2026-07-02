#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"

if ! command -v kind >/dev/null 2>&1; then
  echo "missing required command: kind" >&2
  exit 1
fi

kind delete cluster --name "$CLUSTER_NAME"
