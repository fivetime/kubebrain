#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENDPOINT="${KUBEBRAIN_ETCD_ENDPOINT:-${ENDPOINT:-127.0.0.1:3379}}"

cd "$ROOT_DIR/hack/etcd-client-compat"
KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" go test -count=1 -v ./...
