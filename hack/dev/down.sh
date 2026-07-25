#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=hack/dev/common.sh
source "${SCRIPT_DIR}/common.sh"

CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"

validate_name_token CLUSTER_NAME

if ! command -v kind >/dev/null 2>&1; then
  echo "missing required command: kind" >&2
  exit 1
fi

kind delete cluster --name "$CLUSTER_NAME"
