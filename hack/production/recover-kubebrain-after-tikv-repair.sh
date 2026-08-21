#!/usr/bin/env bash
set -euo pipefail

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_TIDB_CLUSTER_UID="${EXPECTED_TIDB_CLUSTER_UID:-}"
EXPECTED_CLUSTER_ID="${EXPECTED_CLUSTER_ID:-}"
ENDPOINT="${ENDPOINT:-}"
RECOVERY_ATTEMPT_ID="${RECOVERY_ATTEMPT_ID:-}"
RECOVERY_REQUEST_ID="${RECOVERY_REQUEST_ID:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
POD_READY_TIMEOUT_SECONDS="${POD_READY_TIMEOUT_SECONDS:-300}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-10}"
ALLOW_KUBEBRAIN_RECOVERY="${ALLOW_KUBEBRAIN_RECOVERY:-false}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-timeout}"
JQ="${JQ:-jq}"
DATE="${DATE:-date}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REGION_HEALTH_COMMAND="${REGION_HEALTH_COMMAND:-${SCRIPT_DIR}/validate-tikv-region-health.sh}"
MAX_UINT64=18446744073709551615

die() { echo "$*" >&2; exit 1; }
is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$MAX_UINT64" ]]; then return 1; fi
}

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$ALLOW_KUBEBRAIN_RECOVERY" == "true" ]] || die "refusing KubeBrain recovery without ALLOW_KUBEBRAIN_RECOVERY=true"
[[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"
[[ -n "$EXPECTED_TIDB_CLUSTER_UID" ]] || die "EXPECTED_TIDB_CLUSTER_UID is required"
is_positive_uint64 "$EXPECTED_CLUSTER_ID" || die "EXPECTED_CLUSTER_ID must be a positive uint64"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$RECOVERY_ATTEMPT_ID" =~ ^[a-z0-9]([-a-z0-9]{0,28}[a-z0-9])?$ ]] || die "RECOVERY_ATTEMPT_ID must be a DNS label of at most 30 characters"
[[ "$RECOVERY_REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "RECOVERY_REQUEST_ID must be a DNS-compatible external decision ID"
[[ -n "$RECEIPT_OUTPUT" && "$RECEIPT_OUTPUT" == /* ]] || die "RECEIPT_OUTPUT must be an absolute path"
[[ ! -e "$RECEIPT_OUTPUT" ]] || die "RECEIPT_OUTPUT already exists"
[[ "$POD_READY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "POD_READY_TIMEOUT_SECONDS must be a positive integer"
[[ "$PROBE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "PROBE_TIMEOUT_SECONDS must be a positive integer"
(( POD_READY_TIMEOUT_SECONDS <= 1800 )) || die "POD_READY_TIMEOUT_SECONDS must be at most 1800"
(( PROBE_TIMEOUT_SECONDS <= 60 )) || die "PROBE_TIMEOUT_SECONDS must be at most 60"
[[ "$REGION_HEALTH_COMMAND" == /* && -x "$REGION_HEALTH_COMMAND" ]] || die "REGION_HEALTH_COMMAND must be an executable absolute path"
for variable in KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET TIDB_NAMESPACE TIDB_CLUSTER; do
  [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "$variable must be a DNS label"
done
[[ "$("$JQ" -jn --arg value "$MAX_UINT64" '$value | tonumber | tostring' 2>/dev/null)" == "$MAX_UINT64" ]] ||
  die "jq must preserve unsigned 64-bit decimal identities"

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$COMMAND_TIMEOUT" --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" "$KUBECTL" "${context_args[@]}" "$@"; }
kctl_rollout() {
  "$COMMAND_TIMEOUT" --signal=TERM "$((POD_READY_TIMEOUT_SECONDS + 10))s" \
    "$KUBECTL" "${context_args[@]}" "$@"
}

require_tidb_identity() {
  local cluster
  cluster="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)" || return 1
  "$JQ" -e --arg uid "$EXPECTED_TIDB_CLUSTER_UID" --arg cluster_id "$EXPECTED_CLUSTER_ID" '
    .metadata.uid == $uid and
    (.status.clusterID | tostring) == $cluster_id and
    .spec.pd.replicas == 3 and .spec.tikv.replicas == 3 and
    any(.status.conditions[]?; .type == "Ready" and .status == "True")
  ' >/dev/null <<<"$cluster"
}

require_kubebrain_state() {
  local desired="$1" ready="$2" statefulset
  statefulset="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o json)" || return 1
  "$JQ" -e --arg uid "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" \
    --argjson desired "$desired" --argjson ready "$ready" '
    .metadata.uid == $uid and .spec.replicas == $desired and
    (.status.readyReplicas // 0) == $ready
  ' >/dev/null <<<"$statefulset"
}

require_kubebrain_state 0 0 || die "KubeBrain zero-replica identity fence failed"
require_tidb_identity || die "TidbCluster identity/topology/Ready fence failed"

# This executable is an approved recovery primitive. The delegated health gate
# remains authoritative for PD members, TiKV stores, Regions, PVC/PV identity,
# filesystem isolation, and disk pressure. Run it both before and after scale-up
# so a control-plane change cannot hide in the recovery window.
"$REGION_HEALTH_COMMAND"
require_kubebrain_state 0 0 || die "KubeBrain zero-replica identity changed after the storage health gate"
require_tidb_identity || die "TidbCluster identity/topology changed after the storage health gate"

scaled=false
completed=false
rollback_on_failure() {
  local status="$?"
  if [[ "$scaled" == "true" && "$completed" != "true" ]]; then
    live_uid="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    if [[ "$live_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
      kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null 2>&1 || true
    fi
  fi
  return "$status"
}
trap rollback_on_failure EXIT

echo "storage health is stable; restoring KubeBrain replicas"
kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=3 >/dev/null
scaled=true
kctl_rollout -n "$KUBEBRAIN_NAMESPACE" rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
require_kubebrain_state 3 3 || die "KubeBrain did not reach the expected three-replica identity"
require_tidb_identity || die "TidbCluster identity/topology changed while restoring KubeBrain"
"$REGION_HEALTH_COMMAND"
require_kubebrain_state 3 3 || die "KubeBrain identity changed after the post-scale storage health gate"
require_tidb_identity || die "TidbCluster identity/topology changed after the post-scale storage health gate"

kb_pod="${KUBEBRAIN_STATEFULSET}-0"
kb_args="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={range .spec.template.spec.containers[?(@.name=="kubebrain")].args[*]}{.}{"\n"}{end}')"
etcdctl_tls_args=()
if [[ "$ENDPOINT" == https://* ]]; then
  cert_file="$(sed -n 's/^--cert-file=//p' <<<"$kb_args")"
  key_file="$(sed -n 's/^--key-file=//p' <<<"$kb_args")"
  ca_file="$(sed -n 's/^--trusted-ca-file=//p' <<<"$kb_args")"
  [[ "$cert_file" == /* && "$key_file" == /* && "$ca_file" == /* ]] || die "TLS probe paths are incomplete"
  etcdctl_tls_args=(--cacert="$ca_file" --cert="$cert_file" --key="$key_file")
fi

probe_key="/kubebrain-internal/tikv-repair-recovery/${EXPECTED_CLUSTER_ID}/${RECOVERY_ATTEMPT_ID}"
probe_value="recovered-${EXPECTED_TIDB_CLUSTER_UID}"
probe() {
  local output
  output="$(kctl -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" put "$probe_key" "$probe_value" 2>/dev/null)" || return 1
  [[ "$output" == *"OK"* ]] || return 1
  output="$(kctl -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" get "$probe_key" --print-value-only 2>/dev/null)" || return 1
  [[ "$output" == "$probe_value" ]] || return 1
  kctl -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" del "$probe_key" >/dev/null 2>&1
}
probe || die "KubeBrain recovery transaction verification failed; data plane was returned to zero replicas"

completed_at_unix="$($DATE +%s)"
[[ "$completed_at_unix" =~ ^[1-9][0-9]*$ ]] || die "completion time is invalid"
receipt_tmp="${RECEIPT_OUTPUT}.tmp.${RECOVERY_ATTEMPT_ID}"
umask 077
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":%s,"format":"kubebrain.tikv-repair-recovery.receipt.v1","kubebrain_statefulset_uid":"%s","ready_replicas":3,"request_id":"%s","storage_health_verified":true,"tidb_cluster_uid":"%s","transaction_verified":true}\n' \
  "$RECOVERY_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$completed_at_unix" \
  "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$RECOVERY_REQUEST_ID" "$EXPECTED_TIDB_CLUSTER_UID" >"$receipt_tmp" || \
  die "cannot write recovery receipt"
mv -f -- "$receipt_tmp" "$RECEIPT_OUTPUT" || die "cannot publish recovery receipt"
completed=true
echo "KubeBrain recovery succeeded: three replicas are Ready, storage health is stable, and Put/Get/Delete passed"
