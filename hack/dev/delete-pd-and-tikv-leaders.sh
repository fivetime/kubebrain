#!/usr/bin/env bash
# Delete the current PD leader and the Up TiKV store hosting the most region
# leaders in one fault window, then wait for distinct replacement Pods.
# Intended only for explicit opt-in compatibility/failover tests.
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-kind-kubebrain-dbaas}"
NAMESPACE="${NAMESPACE:-tidb-cluster}"
PD_PROBE_POD="${PD_PROBE_POD:-kb-pd-0}"
PD_NAME_PREFIX="${PD_NAME_PREFIX:-kb-pd-}"
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
pd_leader_json="$("${KUBECTL[@]}" exec "$PD_PROBE_POD" -- /pd-ctl -u "$PD_CLIENT_URL" member leader show)"
pd_pod="$(jq -er '.name | select(type == "string" and length > 0)' <<<"$pd_leader_json")"
case "$pd_pod" in
  "$PD_NAME_PREFIX"[0-9]*) ;;
  *)
    echo "refusing to delete unexpected PD leader pod: $pd_pod" >&2
    exit 1
    ;;
esac

stores_json="$("${KUBECTL[@]}" exec "$PD_PROBE_POD" -- /pd-ctl -u "$PD_CLIENT_URL" store)"
tikv_candidate="$(jq -er '
  [.stores[] | select(.store.state_name == "Up")]
  | max_by(.status.leader_count)
  | select(.status.leader_count > 0)
  | [.store.address, .status.leader_count]
  | @tsv
' <<<"$stores_json")"
IFS=$'\t' read -r tikv_address tikv_leader_count <<<"$tikv_candidate"
tikv_pod="${tikv_address%%.*}"
case "$tikv_pod" in
  "$TIKV_NAME_PREFIX"[0-9]*) ;;
  *)
    echo "refusing to delete unexpected TiKV pod from store address: $tikv_address" >&2
    exit 1
    ;;
esac
if [[ ! "$tikv_leader_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "selected TiKV store has invalid leader count: $tikv_leader_count" >&2
  exit 1
fi

pd_old_uid="$("${KUBECTL[@]}" get pod "$pd_pod" -o jsonpath='{.metadata.uid}')"
tikv_old_uid="$("${KUBECTL[@]}" get pod "$tikv_pod" -o jsonpath='{.metadata.uid}')"
if [[ -z "$pd_old_uid" || -z "$tikv_old_uid" ]]; then
  echo "fault target has empty UID: pd=$pd_pod/$pd_old_uid tikv=$tikv_pod/$tikv_old_uid" >&2
  exit 1
fi

echo "deleting PD leader pod=$pd_pod uid=$pd_old_uid and TiKV pod=$tikv_pod uid=$tikv_old_uid region_leaders=$tikv_leader_count" >&2
"${KUBECTL[@]}" delete pod "$pd_pod" "$tikv_pod" --wait=true
"${KUBECTL[@]}" wait --for=condition=Ready "pod/$pd_pod" "pod/$tikv_pod" --timeout="$READY_TIMEOUT"
pd_new_uid="$("${KUBECTL[@]}" get pod "$pd_pod" -o jsonpath='{.metadata.uid}')"
tikv_new_uid="$("${KUBECTL[@]}" get pod "$tikv_pod" -o jsonpath='{.metadata.uid}')"
if [[ -z "$pd_new_uid" || "$pd_new_uid" == "$pd_old_uid" ]]; then
  echo "PD leader replacement UID did not change: pod=$pd_pod old=$pd_old_uid new=$pd_new_uid" >&2
  exit 1
fi
if [[ -z "$tikv_new_uid" || "$tikv_new_uid" == "$tikv_old_uid" ]]; then
  echo "TiKV replacement UID did not change: pod=$tikv_pod old=$tikv_old_uid new=$tikv_new_uid" >&2
  exit 1
fi
echo "combined replacements ready pd=$pd_pod/$pd_new_uid tikv=$tikv_pod/$tikv_new_uid" >&2
