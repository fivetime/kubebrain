#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ENDPOINT="${KUBEBRAIN_ETCD_ENDPOINT:-${ENDPOINT:-}}"

if [[ -z "$ENDPOINT" ]]; then
  echo "memberlist sync smoke requires KUBEBRAIN_ETCD_ENDPOINT (or ENDPOINT)" >&2
  exit 2
fi
if ! command -v go >/dev/null 2>&1; then
  echo "missing required command: go" >&2
  exit 1
fi

cd "$ROOT_DIR/hack/etcd-client-compat"
KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
  KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIALABLE=1 \
  go test -count=1 -run '^TestMemberListSupportsOfficialClientSync$' .

echo "MemberList clientv3 Sync smoke completed: endpoint=$ENDPOINT"
