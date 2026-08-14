#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
PD_ENDPOINT="${KUBEBRAIN_TIKV_PD:-kb-pd.tidb-cluster.svc:2379}"
REMOTE_TEST=/tmp/kubebrain-tikv-large-key-smoke.test

if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required" >&2
  exit 2
fi
if [[ ! "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "invalid KUBEBRAIN_NAMESPACE: $NAMESPACE" >&2
  exit 2
fi
if [[ ! "$PD_ENDPOINT" =~ ^[A-Za-z0-9._:-]+(,[A-Za-z0-9._:-]+)*$ ]]; then
  echo "invalid KUBEBRAIN_TIKV_PD: $PD_ENDPOINT" >&2
  exit 2
fi
command -v "$KUBECTL" >/dev/null
command -v go >/dev/null

kubectl_args=("$KUBECTL" --context "$KUBE_CONTEXT" -n "$NAMESPACE")
mapfile -t pods < <("${kubectl_args[@]}" get pods \
  -l app.kubernetes.io/name=kubebrain \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
if [[ "${#pods[@]}" -eq 0 ]]; then
  echo "no KubeBrain pod found in $NAMESPACE" >&2
  exit 1
fi
pod="${pods[0]}"
"${kubectl_args[@]}" wait --for=condition=Ready "pod/$pod" --timeout=2m >/dev/null

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-tikv-large-key.XXXXXX")"
cleanup() {
  "${kubectl_args[@]}" exec "$pod" -- rm -f "$REMOTE_TEST" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

(
  cd "$ROOT_DIR"
  CGO_ENABLED=0 go test -c -o "$work_dir/storage-tikv.test" ./pkg/storage/tikv
)
"${kubectl_args[@]}" cp "$work_dir/storage-tikv.test" "$pod:$REMOTE_TEST"
"${kubectl_args[@]}" exec "$pod" -- env \
  "KUBEBRAIN_TIKV_PD=$PD_ENDPOINT" \
  "$REMOTE_TEST" -test.run '^TestLargeKeyRoundTripTiKV$' -test.count=1 -test.v
