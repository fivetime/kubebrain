#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

OPERATION_ID="${OPERATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
CUTOVER_STATE_INPUT="${CUTOVER_STATE_INPUT:-}"
CUTOVER_RECEIPT_INPUT="${CUTOVER_RECEIPT_INPUT:-}"
SERVICE_NAMESPACE="${SERVICE_NAMESPACE:-kubebrain-system}"
SERVICE_NAME="${SERVICE_NAME:-kubebrain}"
TARGET_INSTANCE="${TARGET_INSTANCE:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
PUBLIC_ENDPOINT="${PUBLIC_ENDPOINT:-}"
AUDIT_DURATION_SECONDS="${AUDIT_DURATION_SECONDS:-3600}"
AUDIT_INTERVAL_SECONDS="${AUDIT_INTERVAL_SECONDS:-60}"
MIN_SAMPLES="${MIN_SAMPLES:-10}"
AUDIT_PREFIX="${AUDIT_PREFIX:-/__kubebrain/audit}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
PROBE="${PROBE:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"

usage() {
  cat >&2 <<'EOF'
Usage:
  OPERATION_ID=<id> INSTANCE=<instance> STATE_DIR=<durable-dir> \
  CUTOVER_STATE_INPUT=<a189.state> CUTOVER_RECEIPT_INPUT=<a189.receipt.json> \
  SERVICE_NAMESPACE=<namespace> SERVICE_NAME=<service> \
  TARGET_INSTANCE=<target> PUBLIC_ENDPOINT=<service-endpoint> \
    hack/production/audit-restored-instance.sh

Continuously fences Service, Pod, and EndpointSlice identities while probing
leased conditional write, linearizable read, conditional delete, and revoke
through the public endpoint. It publishes an immutable receipt only after the
full observation duration and minimum sample count complete without failure.
EOF
  exit 2
}

for variable in OPERATION_ID INSTANCE STATE_DIR CUTOVER_STATE_INPUT CUTOVER_RECEIPT_INPUT \
  SERVICE_NAMESPACE SERVICE_NAME TARGET_INSTANCE PUBLIC_ENDPOINT; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; usage; }
done
for variable in OPERATION_ID INSTANCE SERVICE_NAMESPACE SERVICE_NAME TARGET_INSTANCE; do
  [[ "${!variable}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "${variable} contains unsupported characters" >&2; exit 2; }
done
for variable in EXPECTED_REPLICAS AUDIT_DURATION_SECONDS MIN_SAMPLES; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] ||
    { echo "${variable} must be a positive integer" >&2; exit 2; }
done
[[ "$AUDIT_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] ||
  { echo "AUDIT_INTERVAL_SECONDS must be a non-negative integer" >&2; exit 2; }
[[ -f "$CUTOVER_STATE_INPUT" && -f "$CUTOVER_RECEIPT_INPUT" ]] ||
  { echo "cutover state and receipt inputs must exist" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }

umask 077
mkdir -p "$STATE_DIR"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${OPERATION_ID}.receipt.json}"
kubectl_args=()
[[ -n "$KUBE_CONTEXT" ]] && kubectl_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")

atomic_publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    rm -f "$temporary"
    echo "refusing to overwrite existing post-restore audit receipt: ${destination}" >&2
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}

IFS=$'\t' read -r state_kind state_format state_instance cutover_operation state_namespace \
  state_service source_instance state_target state_service_uid artifact_sha snapshot_revision \
  source_prefix target_prefix <"$CUTOVER_STATE_INPUT"
[[ "$state_kind" == "HEADER" && "$state_format" == "kubebrain.restore-cutover.state.v1" &&
  "$state_instance" == "$INSTANCE" && "$state_namespace" == "$SERVICE_NAMESPACE" &&
  "$state_service" == "$SERVICE_NAME" && "$state_target" == "$TARGET_INSTANCE" &&
  -n "$source_instance" && "$source_instance" != "$state_target" &&
  -n "$source_prefix" && -n "$target_prefix" && "$source_prefix" != "$target_prefix" &&
  -n "$state_service_uid" && "$artifact_sha" =~ ^[a-f0-9]{64}$ &&
  "$snapshot_revision" =~ ^[1-9][0-9]*$ ]] ||
  { echo "cutover state does not match the audit operation" >&2; exit 1; }
cutover_state_sha="$(sha256sum "$CUTOVER_STATE_INPUT" | cut -d " " -f1)"

"$JQ" -e --arg operation "$cutover_operation" --arg instance "$INSTANCE" \
  --arg namespace "$SERVICE_NAMESPACE" --arg service "$SERVICE_NAME" \
  --arg uid "$state_service_uid" --arg source "$source_instance" \
  --arg target "$TARGET_INSTANCE" --arg sha "$artifact_sha" \
  --arg state_sha "$cutover_state_sha" \
  --argjson revision "$snapshot_revision" '
    keys == ["artifact_sha256","completed_at_unix","cutover_state_sha256","endpoint_uids_matched","format","instance","operation_id","pod_uids_unchanged","public_data_verified","replicas","service_name","service_namespace","service_uid","snapshot_revision","source_instance","target_instance"] and
    .format == "kubebrain.restore-cutover.receipt.v1" and
    .operation_id == $operation and .instance == $instance and
    .service_namespace == $namespace and .service_name == $service and
    .service_uid == $uid and .source_instance == $source and .target_instance == $target and
    (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    .artifact_sha256 == $sha and .snapshot_revision == $revision and
    .cutover_state_sha256 == $state_sha and
    (.replicas | type == "number" and . > 0 and . == floor) and
    .pod_uids_unchanged == true and .endpoint_uids_matched == true and
    .public_data_verified == true and
    (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
  "$CUTOVER_RECEIPT_INPUT" >/dev/null ||
  { echo "cutover receipt does not match the frozen state" >&2; exit 1; }

expected_pods="$(awk -F '\t' '$1 == "POD" && $2 == "target" {print $3 "\t" $4 "\t" $5}' \
  "$CUTOVER_STATE_INPUT")"
[[ "$(sed '/^$/d' <<<"$expected_pods" | wc -l | tr -d ' ')" == "$EXPECTED_REPLICAS" ]] ||
  { echo "cutover state target Pod count does not match EXPECTED_REPLICAS" >&2; exit 1; }
expected_uids="$(cut -f2 <<<"$expected_pods" | LC_ALL=C sort)"

fence_topology() {
  local service current_pods endpoint_uids
  service="$("$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get service "$SERVICE_NAME" -o json |
    "$JQ" -er 'select(.spec.selector | length == 2) | [
      .metadata.uid, .spec.selector["app.kubernetes.io/name"],
      .spec.selector["app.kubernetes.io/instance"]
    ] | select(all(. != null and . != "")) | @tsv')"
  [[ "$service" == "${state_service_uid}"$'\t'"kubebrain"$'\t'"${TARGET_INSTANCE}" ]] ||
    { echo "Service UID or target selector changed during post-restore audit" >&2; exit 1; }

  current_pods="$("$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${TARGET_INSTANCE}" -o json |
    "$JQ" -er '
      [.items[] | select(
        ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
        ([.status.containerStatuses[]? | select(.ready == true)] | length) == (.status.containerStatuses | length)
      ) | [.metadata.name, .metadata.uid, ([.status.containerStatuses[].restartCount] | add // 0)]]
      | sort_by(.[0]) | .[] | @tsv')"
  [[ "$current_pods" == "$expected_pods" ]] ||
    { echo "target Pod UID/readiness/restart fence failed during post-restore audit" >&2; exit 1; }

  endpoint_uids="$("$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get endpointslices \
    -l "kubernetes.io/service-name=${SERVICE_NAME}" -o json |
    "$JQ" -er --arg uid "$state_service_uid" '
      select(.items | length > 0) |
      select(all(.items[]; any(.metadata.ownerReferences[]?;
        .kind == "Service" and .uid == $uid and .controller == true))) |
      [.items[].endpoints[] |
        select(.conditions.ready == true and .conditions.serving != false and .conditions.terminating != true) |
        select(.targetRef.kind == "Pod") | .targetRef.uid] | sort | .[]')"
  [[ "$endpoint_uids" == "$expected_uids" ]] ||
    { echo "EndpointSlice target Pod UID set changed during post-restore audit" >&2; exit 1; }
}

run_probe() {
  local output
  if [[ -n "$PROBE" ]]; then
    output="$(ENDPOINT="$PUBLIC_ENDPOINT" AUDIT_PREFIX="$AUDIT_PREFIX" "$PROBE")"
  else
    output="$(cd "$ROOT_DIR" && ENDPOINT="$PUBLIC_ENDPOINT" AUDIT_PREFIX="$AUDIT_PREFIX" \
      go run ./hack/production/cmd/etcd-audit-probe)"
  fi
  "$JQ" -e '
    .format == "kubebrain.etcd-audit-probe.v1" and
    (.put_revision > 0) and (.read_revision >= .put_revision) and
    (.delete_revision >= .read_revision) and (.lease_ttl > 0)' <<<"$output" >/dev/null ||
    { echo "etcd audit probe returned invalid evidence" >&2; exit 1; }
  "$JQ" -r '.delete_revision' <<<"$output"
}

if [[ -e "$receipt_file" ]]; then
  "$JQ" -e --arg operation "$OPERATION_ID" --arg instance "$INSTANCE" \
    --arg cutover "$cutover_operation" --arg uid "$state_service_uid" \
    --arg target "$TARGET_INSTANCE" --arg sha "$artifact_sha" \
    --argjson snapshot "$snapshot_revision" --argjson replicas "$EXPECTED_REPLICAS" \
    --argjson duration "$AUDIT_DURATION_SECONDS" --argjson interval "$AUDIT_INTERVAL_SECONDS" \
    --argjson min_samples "$MIN_SAMPLES" '
      keys == ["all_probes_succeeded","artifact_sha256","completed","completed_at_unix","cutover_operation_id","duration_seconds","first_probe_revision","format","instance","interval_seconds","last_probe_revision","operation_id","replicas","samples","service_uid","snapshot_revision","started_at_unix","target_instance","topology_unchanged"] and
      .format == "kubebrain.post-restore-audit.receipt.v1" and
      .operation_id == $operation and .instance == $instance and
      .cutover_operation_id == $cutover and .service_uid == $uid and
      .target_instance == $target and
      (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
      .artifact_sha256 == $sha and
      .snapshot_revision == $snapshot and .replicas == $replicas and
      .duration_seconds == $duration and .interval_seconds == $interval and
      .topology_unchanged == true and .all_probes_succeeded == true and
      .completed == true and
      (.samples | type == "number" and . >= $min_samples and . == floor) and
      (.first_probe_revision | type == "number" and . > 0 and . == floor) and
      (.last_probe_revision | type == "number" and . > 0 and . == floor) and
      (.last_probe_revision >= .first_probe_revision) and
      (.started_at_unix | type == "number" and . > 0 and . == floor) and
      (.completed_at_unix | type == "number" and . > 0 and . == floor) and
      (.completed_at_unix >= .started_at_unix)' "$receipt_file" >/dev/null ||
    { echo "existing post-restore audit receipt does not match the operation" >&2; exit 1; }
  fence_topology
  run_probe >/dev/null
  exit 0
fi

started_at="$(date +%s)"
started_monotonic="$SECONDS"
deadline_monotonic=$((started_monotonic + AUDIT_DURATION_SECONDS))
samples=0
first_revision=0
last_revision=0
while true; do
  fence_topology
  revision="$(run_probe)"
  fence_topology
  (( last_revision == 0 || revision >= last_revision )) ||
    { echo "etcd audit probe revision moved backwards" >&2; exit 1; }
  ((samples += 1))
  (( first_revision == 0 )) && first_revision="$revision"
  last_revision="$revision"
  if (( SECONDS >= deadline_monotonic && samples >= MIN_SAMPLES )); then
    break
  fi
  sleep "$AUDIT_INTERVAL_SECONDS"
done
completed_at="$(date +%s)"

temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.receipt.XXXXXX")"
"$JQ" -cnS --arg format "kubebrain.post-restore-audit.receipt.v1" \
  --arg operation_id "$OPERATION_ID" --arg instance "$INSTANCE" \
  --arg cutover_operation_id "$cutover_operation" --arg service_uid "$state_service_uid" \
  --arg target_instance "$TARGET_INSTANCE" --arg artifact_sha256 "$artifact_sha" \
  --argjson snapshot_revision "$snapshot_revision" --argjson replicas "$EXPECTED_REPLICAS" \
  --argjson duration_seconds "$AUDIT_DURATION_SECONDS" --argjson interval_seconds "$AUDIT_INTERVAL_SECONDS" \
  --argjson samples "$samples" --argjson first_revision "$first_revision" \
  --argjson last_revision "$last_revision" --argjson started_at_unix "$started_at" \
  --argjson completed_at_unix "$completed_at" \
  '{format:$format,operation_id:$operation_id,instance:$instance,
    cutover_operation_id:$cutover_operation_id,service_uid:$service_uid,
    target_instance:$target_instance,artifact_sha256:$artifact_sha256,
    snapshot_revision:$snapshot_revision,replicas:$replicas,
    duration_seconds:$duration_seconds,interval_seconds:$interval_seconds,samples:$samples,
    first_probe_revision:$first_revision,last_probe_revision:$last_revision,
    topology_unchanged:true,all_probes_succeeded:true,completed:true,
    started_at_unix:$started_at_unix,completed_at_unix:$completed_at_unix}' >"$temporary"
atomic_publish "$temporary" "$receipt_file"
