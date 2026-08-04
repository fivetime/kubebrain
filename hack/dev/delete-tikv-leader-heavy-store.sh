#!/usr/bin/env bash
# Delete the Up TiKV store currently hosting the most region leaders, then wait
# for a distinct replacement Pod. Intended only for explicit opt-in failover tests.
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-kind-kubebrain-dbaas}"
NAMESPACE="${NAMESPACE:-tidb-cluster}"
PD_PROBE_POD="${PD_PROBE_POD:-kb-pd-0}"
TIKV_NAME_PREFIX="${TIKV_NAME_PREFIX:-kb-tikv-}"
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
stores_json="$("${KUBECTL[@]}" exec "$PD_PROBE_POD" -- /pd-ctl -u "$PD_CLIENT_URL" store)"
candidate="$(jq -er '
  [.stores[] | select(.store.state_name == "Up")]
  | max_by(.status.leader_count)
  | select(.status.leader_count > 0)
  | [.store.address, .status.leader_count]
  | @tsv
' <<<"$stores_json")"
IFS=$'\t' read -r store_address leader_count <<<"$candidate"
pod="${store_address%%.*}"
case "$pod" in
  "$TIKV_NAME_PREFIX"[0-9]*) ;;
  *)
    echo "refusing to delete unexpected TiKV pod from store address: $store_address" >&2
    exit 1
    ;;
esac
if [[ ! "$leader_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "selected TiKV store has invalid leader count: $leader_count" >&2
  exit 1
fi

old_uid="$("${KUBECTL[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')"
if [[ -z "$old_uid" ]]; then
  echo "TiKV pod has empty UID: $pod" >&2
  exit 1
fi

echo "deleting TiKV pod=$pod uid=$old_uid region_leaders=$leader_count" >&2
"${KUBECTL[@]}" delete pod "$pod" --wait=true
"${KUBECTL[@]}" wait --for=condition=Ready "pod/$pod" --timeout="$READY_TIMEOUT"
new_uid="$("${KUBECTL[@]}" get pod "$pod" -o jsonpath='{.metadata.uid}')"
if [[ -z "$new_uid" || "$new_uid" == "$old_uid" ]]; then
  echo "TiKV replacement UID did not change: pod=$pod old=$old_uid new=$new_uid" >&2
  exit 1
fi
echo "TiKV replacement ready pod=$pod uid=$new_uid" >&2
