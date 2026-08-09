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
REPAIR_ATTEMPT_ID="${REPAIR_ATTEMPT_ID:-}"
REQUIRED_FAILED_PROBES="${REQUIRED_FAILED_PROBES:-3}"
PROBE_INTERVAL_SECONDS="${PROBE_INTERVAL_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-10}"
POD_READY_TIMEOUT_SECONDS="${POD_READY_TIMEOUT_SECONDS:-300}"
ALLOW_TIKV_POD_REPAIR="${ALLOW_TIKV_POD_REPAIR:-false}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-timeout}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$ALLOW_TIKV_POD_REPAIR" == "true" ]] || die "refusing TiKV Pod repair without ALLOW_TIKV_POD_REPAIR=true"
[[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"
[[ -n "$EXPECTED_TIDB_CLUSTER_UID" ]] || die "EXPECTED_TIDB_CLUSTER_UID is required"
[[ "$EXPECTED_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]] || die "EXPECTED_CLUSTER_ID must be a positive integer"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$REPAIR_ATTEMPT_ID" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "REPAIR_ATTEMPT_ID must be a DNS label"
for variable in REQUIRED_FAILED_PROBES PROBE_TIMEOUT_SECONDS POD_READY_TIMEOUT_SECONDS; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
[[ "$PROBE_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "PROBE_INTERVAL_SECONDS must be a non-negative integer"
for variable in KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET TIDB_NAMESPACE TIDB_CLUSTER; do
  [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "$variable must be a DNS label"
done

kctl() { "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }

actual_kb_uid="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}')"
[[ "$actual_kb_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "KubeBrain StatefulSet UID fence failed"
actual_cluster_identity="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o jsonpath='{.metadata.uid}{"\t"}{.status.clusterID}{"\t"}{.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}')"
[[ "$actual_cluster_identity" == "$EXPECTED_TIDB_CLUSTER_UID"$'\t'"$EXPECTED_CLUSTER_ID"$'\t3\t3' ]] || die "TidbCluster identity/topology fence failed"
original_kb_replicas="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.spec.replicas}')"
[[ "$original_kb_replicas" == "3" ]] || die "repair requires exactly 3 desired KubeBrain replicas"

repair_lock="kubebrain-tikv-transaction-repair-lock"
kctl -n "$TIDB_NAMESPACE" create configmap "$repair_lock" \
  --from-literal="attempt-id=$REPAIR_ATTEMPT_ID" \
  --from-literal="tidb-cluster-uid=$EXPECTED_TIDB_CLUSTER_UID" >/dev/null ||
  die "another TiKV transaction-path repair holds $TIDB_NAMESPACE/$repair_lock"
cleanup_lock() {
  kctl -n "$TIDB_NAMESPACE" delete configmap "$repair_lock" --ignore-not-found --wait=true >/dev/null 2>&1 || true
}
trap cleanup_lock EXIT

tikv_status() {
  kctl -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}{"\n"}{end}' | sort
}

expected_names="$(printf '%s\n' "${TIDB_CLUSTER}-tikv-0" "${TIDB_CLUSTER}-tikv-1" "${TIDB_CLUSTER}-tikv-2")"
validate_tikv_ready() {
  local rows names
  rows="$(tikv_status)" || return 1
  names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$rows")"
  [[ "$names" == "$expected_names" ]]
}
validate_tikv_ready || die "TiKV quorum/PVC fence failed before repair"

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

probe_key="/kubebrain-internal/transaction-repair-probe/${EXPECTED_CLUSTER_ID}"
probe_value="repair-${EXPECTED_TIDB_CLUSTER_UID}"
transaction_probe() {
  local output
  output="$($COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" put "$probe_key" "$probe_value" 2>/dev/null)" || return 1
  [[ "$output" == *"OK"* ]] || return 1
  output="$($COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" get "$probe_key" --print-value-only 2>/dev/null)" || return 1
  [[ "$output" == "$probe_value" ]] || return 1
  $COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" del "$probe_key" >/dev/null 2>&1
}

for ((probe=1; probe<=REQUIRED_FAILED_PROBES; probe++)); do
  if transaction_probe; then
    die "transaction probe ${probe} succeeded; refusing repair of a healthy data plane"
  fi
  (( probe == REQUIRED_FAILED_PROBES )) || sleep "$PROBE_INTERVAL_SECONDS"
done

ready_kb="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.status.readyReplicas}')"
[[ -z "$ready_kb" || "$ready_kb" == "0" ]] || die "repair requires KubeBrain to be exactly 0 Ready after failed transaction probes"

echo "confirmed ${REQUIRED_FAILED_PROBES} consecutive transaction failures; scaling KubeBrain to zero"
kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null
kctl -n "$KUBEBRAIN_NAMESPACE" wait --for=delete pod -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${KUBEBRAIN_STATEFULSET}" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null

for ordinal in 2 1 0; do
  pod="${TIDB_CLUSTER}-tikv-${ordinal}"
  old_identity="$(kctl -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')"
  old_uid="${old_identity%%$'\t'*}"
  old_pvc="${old_identity#*$'\t'}"
  [[ -n "$old_uid" && -n "$old_pvc" ]] || die "$pod identity/PVC is incomplete"
  echo "rebuilding $pod on retained PVC $old_pvc"
  kctl -n "$TIDB_NAMESPACE" delete pod "$pod" --wait=true >/dev/null
  kctl -n "$TIDB_NAMESPACE" wait --for=condition=Ready "pod/$pod" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
  new_identity="$(kctl -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')"
  new_uid="${new_identity%%$'\t'*}"
  new_pvc="${new_identity#*$'\t'}"
  [[ "$new_uid" != "$old_uid" && "$new_pvc" == "$old_pvc" ]] || die "$pod same-PVC replacement fence failed"
  validate_tikv_ready || die "TiKV quorum did not recover after replacing $pod"
done

echo "TiKV replacements converged; restoring KubeBrain replicas"
kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas="$original_kb_replicas" >/dev/null
kctl -n "$KUBEBRAIN_NAMESPACE" rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
if ! transaction_probe; then
  kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null || true
  die "TiKV repair completed but end-to-end transaction verification failed; KubeBrain was returned to zero replicas"
fi

echo "TiKV transaction-path repair succeeded: every Pod retained its PVC and end-to-end Put/Get/Delete recovered"
