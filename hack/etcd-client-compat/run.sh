#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENDPOINT="${KUBEBRAIN_ETCD_ENDPOINT:-${ENDPOINT:-127.0.0.1:3379}}"
REFERENCE_DIFFERENTIAL_FLAGS=(
  REFERENCE_ETCD_ENDPOINT
  REFERENCE_ETCD_GATEWAY_ENDPOINT
  REFERENCE_QUOTA_ETCD_ENDPOINT
  REFERENCE_JWT_ETCD_ENDPOINT
  ETCD_AUTH_DIFF_ENDPOINT
  KUBEBRAIN_AUTH_DIFF_ENDPOINT
)

reject_reference_differential_flags() {
  local name
  for name in "${REFERENCE_DIFFERENTIAL_FLAGS[@]}"; do
    if [ -n "${!name:-}" ]; then
      echo "run.sh refuses reference/differential opt-in ${name}; use hack/etcd-client-compat/run-differential.sh or go test directly against disposable endpoints" >&2
      exit 2
    fi
  done
}

reject_reference_differential_flags

cd "$ROOT_DIR/hack/etcd-client-compat"
KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" go test -count=1 -v ./...
