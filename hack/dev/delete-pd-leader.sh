#!/usr/bin/env bash
# Delete the current PD leader and wait for a distinct replacement Pod.
# Intended only for explicit opt-in compatibility/failover tests.
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-kind-kubebrain-dbaas}"
NAMESPACE="${NAMESPACE:-tidb-cluster}"
PD_PROBE_POD="${PD_PROBE_POD:-kb-pd-0}"
PD_NAME_PREFIX="${PD_NAME_PREFIX:-kb-pd-}"
PD_CLIENT_URL="${PD_CLIENT_URL:-http://127.0.0.1:2379}"
READY_TIMEOUT="${READY_TIMEOUT:-90s}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}
need kubectl
need jq

KUBECTL=(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE")
leader_json="$("${KUBECTL[@]}" exec "$PD_PROBE_POD" -- /pd-ctl -u "$PD_CLIENT_URL" member leader show)"
leader="$(jq -er '.name | select(type == "string" and length > 0)' <<<"$leader_json")"
case "$leader" in
  "$PD_NAME_PREFIX"[0-9]*) ;;
  *)
    echo "refusing to delete unexpected PD leader pod: $leader" >&2
    exit 1
    ;;
esac

old_uid="$("${KUBECTL[@]}" get pod "$leader" -o jsonpath='{.metadata.uid}')"
if [[ -z "$old_uid" ]]; then
  echo "PD leader pod has empty UID: $leader" >&2
  exit 1
fi

echo "deleting PD leader pod=$leader uid=$old_uid" >&2
"${KUBECTL[@]}" delete pod "$leader" --wait=true
"${KUBECTL[@]}" wait --for=condition=Ready "pod/$leader" --timeout="$READY_TIMEOUT"
new_uid="$("${KUBECTL[@]}" get pod "$leader" -o jsonpath='{.metadata.uid}')"
if [[ -z "$new_uid" || "$new_uid" == "$old_uid" ]]; then
  echo "PD leader replacement UID did not change: pod=$leader old=$old_uid new=$new_uid" >&2
  exit 1
fi
echo "PD leader replacement ready pod=$leader uid=$new_uid" >&2
