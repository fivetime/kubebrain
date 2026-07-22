#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

ACTION="${ACTION:-}"
OPERATION_ID="${OPERATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
BACKUP_INPUT="${BACKUP_INPUT:-}"
BACKUP_PREFIX="${BACKUP_PREFIX:-/registry}"
BACKUP_MAX_AGE_SECONDS="${BACKUP_MAX_AGE_SECONDS:-86400}"
BACKUP_MIN_RECORDS="${BACKUP_MIN_RECORDS:-1}"
CONFIRM_DESTROY="${CONFIRM_DESTROY:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_PVCS="${EXPECTED_PVCS:-6}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
UID_DELETE="${UID_DELETE:-}"
LOGICAL_STATUS="${LOGICAL_STATUS:-${ROOT_DIR}/hack/backup/logical-status.sh}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"

usage() {
  cat >&2 <<'EOF'
Usage:
  ACTION=prepare|quiesce|destroy|complete \
  OPERATION_ID=<id> INSTANCE=<instance> STATE_DIR=<durable-dir> \
  BACKUP_INPUT=<logical-v2.jsonl> \
    hack/production/destroy-instance.sh

prepare verifies a recent logical backup and records the UID of every owned
KubeBrain/TiKV resource. quiesce requires CONFIRM_DESTROY and scales KubeBrain
to zero. destroy uses UID-preconditioned deletes. complete proves all recorded
resources and instance PVCs are absent, then publishes an immutable receipt.

CONFIRM_DESTROY must exactly equal destroy:<INSTANCE>:<OPERATION_ID>.
EOF
  exit 2
}

for variable in ACTION OPERATION_ID INSTANCE STATE_DIR; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    usage
  fi
done
if [[ ! "$ACTION" =~ ^(prepare|quiesce|destroy|complete)$ ]]; then
  echo "ACTION must be prepare, quiesce, destroy, or complete" >&2
  exit 2
fi
for variable in OPERATION_ID INSTANCE KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET TIDB_NAMESPACE TIDB_CLUSTER; do
  if [[ ! "${!variable}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    echo "${variable} contains unsupported characters" >&2
    exit 2
  fi
done
for variable in BACKUP_MAX_AGE_SECONDS TIMEOUT_SECONDS; do
  if ! [[ "${!variable}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${variable} must be a positive integer" >&2
    exit 2
  fi
done
for variable in BACKUP_MIN_RECORDS EXPECTED_PVCS POLL_INTERVAL_SECONDS; do
  if ! [[ "${!variable}" =~ ^[0-9]+$ ]]; then
    echo "${variable} must be a non-negative integer" >&2
    exit 2
  fi
done

umask 077
mkdir -p "$STATE_DIR"
state_file="${STATE_DIR}/${OPERATION_ID}.state"
quiesced_file="${STATE_DIR}/${OPERATION_ID}.quiesced"
destroyed_file="${STATE_DIR}/${OPERATION_ID}.destroyed"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${OPERATION_ID}.receipt.json}"
expected_confirmation="destroy:${INSTANCE}:${OPERATION_ID}"

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi
if [[ -n "$KUBECONFIG_PATH" ]]; then
  kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")
fi

atomic_publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    if cmp -s "$temporary" "$destination"; then
      rm -f "$temporary"
      return
    fi
    echo "refusing to overwrite existing destroy evidence: ${destination}" >&2
    rm -f "$temporary"
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}

require_jq() {
  command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
}

validate_existing_receipt() {
  local backup_sha="$1" backup_revision="$2"
  "$JQ" -e \
    --arg instance "$INSTANCE" \
    --arg operation "$OPERATION_ID" \
    --arg kbns "$KUBEBRAIN_NAMESPACE" \
    --arg tidbns "$TIDB_NAMESPACE" \
    --arg tidb "$TIDB_CLUSTER" \
    --arg backup_sha "$backup_sha" \
    --argjson backup_revision "$backup_revision" \
    'keys == ["backup_revision","backup_sha256","completed_at_unix","format","instance","kubebrain_namespace","operation_id","resources_absent","tidb_cluster","tidb_namespace"] and
     .format == "kubebrain.destroy.receipt.v1" and
     .instance == $instance and .operation_id == $operation and
     .kubebrain_namespace == $kbns and .tidb_namespace == $tidbns and
     .tidb_cluster == $tidb and
     (.backup_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     .backup_sha256 == $backup_sha and
     .backup_revision == $backup_revision and .resources_absent == true and
     (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$receipt_file" >/dev/null
}

require_confirmation() {
  if [[ "$CONFIRM_DESTROY" != "$expected_confirmation" ]]; then
    echo "CONFIRM_DESTROY must exactly equal ${expected_confirmation}" >&2
    exit 2
  fi
}

resource_snapshot() {
  local kind="$1" namespace="$2" name="$3" expected_name="$4" expected_instance="$5"
  local result uid actual_name actual_instance
  result="$("$KUBECTL" "${kubectl_args[@]}" -n "$namespace" get "$kind" "$name" \
    -o 'jsonpath={.metadata.uid}{"\t"}{.metadata.labels.app\.kubernetes\.io/name}{"\t"}{.metadata.labels.app\.kubernetes\.io/instance}')"
  IFS=$'\t' read -r uid actual_name actual_instance <<<"$result"
  if [[ -z "$uid" || "$actual_name" != "$expected_name" || "$actual_instance" != "$expected_instance" ]]; then
    echo "${kind} ${namespace}/${name} has missing or unexpected ownership labels" >&2
    exit 1
  fi
  printf '%s' "$uid"
}

append_resource() {
  local output="$1" api_version="$2" resource="$3" kind="$4" namespace="$5" name="$6"
  local expected_name="$7" expected_instance="$8" uid
  uid="$(resource_snapshot "$kind" "$namespace" "$name" "$expected_name" "$expected_instance")"
  printf 'RESOURCE\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$api_version" "$resource" "$kind" "$namespace" "$name" "$uid" >>"$output"
}

pvc_snapshot() {
  "$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get persistentvolumeclaims \
    -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER}" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.metadata.labels.app\.kubernetes\.io/component}{"\n"}{end}' |
    LC_ALL=C sort
}

validate_pvc_rows() {
  local rows="$1" count
  count="$(sed '/^$/d' <<<"$rows" | wc -l | tr -d ' ')"
  if [[ "$count" != "$EXPECTED_PVCS" ]]; then
    echo "expected ${EXPECTED_PVCS} owned PD/TiKV PVCs, got ${count}" >&2
    exit 1
  fi
  if ! awk -F '\t' 'NF != 3 || $1 == "" || $2 == "" || ($3 != "pd" && $3 != "tikv") { exit 1 }' <<<"$rows"; then
    echo "owned PVC snapshot contains missing identity or an unexpected component" >&2
    exit 1
  fi
}

validate_state_schema() {
  awk -F '\t' -v expectedPVCs="$EXPECTED_PVCS" '
    $0 == "" {
      bad = "empty row"
      exit 1
    }
    $1 == "HEADER" {
      if (NF != 10) {
        bad = "HEADER row must have 10 fields"
        exit 1
      }
      header++
      next
    }
    $1 == "RESOURCE" {
      if (NF != 7 || $2 == "" || $3 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "") {
        bad = "RESOURCE row must have apiVersion, resource, kind, namespace, name, and uid"
        exit 1
      }
      resources++
      next
    }
    $1 == "PVC" {
      if (NF != 8 || $2 != "v1" || $3 != "persistentvolumeclaims" ||
        $4 != "persistentvolumeclaim" || $5 == "" || $6 == "" ||
        ($7 != "pd" && $7 != "tikv") || $8 == "") {
        bad = "PVC row must have canonical identity, component, and namespace"
        exit 1
      }
      pvcs++
      next
    }
    {
      bad = "unknown row type " $1
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (header != 1 || resources != 10 || pvcs != expectedPVCs) {
        printf("schema counts mismatch: header=%d resources=%d pvcs=%d expectedPVCs=%d\n",
          header, resources, pvcs, expectedPVCs) > "/dev/stderr"
        exit 1
      }
    }
  ' "$state_file" || { echo "destroy state has invalid schema" >&2; exit 1; }
}

validate_marker() {
  local path="$1" format="$2" label="$3"
  awk -F '\t' -v format="$format" -v instance="$INSTANCE" -v operation="$OPERATION_ID" '
    NR == 1 {
      if (NF != 3 || $1 != format || $2 != instance || $3 != operation) {
        bad = "marker row does not match the operation"
        exit 1
      }
      rows++
      next
    }
    {
      bad = "unexpected extra marker row"
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (rows != 1) {
        print "marker must contain exactly one row" > "/dev/stderr"
        exit 1
      }
    }
  ' "$path" || { echo "destroy ${label} marker has invalid schema" >&2; exit 1; }
}

read_header() {
  local kind format state_instance state_operation state_kb_namespace state_kb_name
  local state_tidb_namespace state_tidb_cluster state_backup_sha state_backup_revision
  validate_state_schema
  IFS=$'\t' read -r kind format state_instance state_operation state_kb_namespace state_kb_name \
    state_tidb_namespace state_tidb_cluster state_backup_sha state_backup_revision <"$state_file"
  if [[ "$kind" != "HEADER" ||
    "$format" != "kubebrain.destroy.state.v1" ||
    "$state_instance" != "$INSTANCE" ||
    "$state_operation" != "$OPERATION_ID" ||
    "$state_kb_namespace" != "$KUBEBRAIN_NAMESPACE" ||
    "$state_kb_name" != "$KUBEBRAIN_STATEFULSET" ||
    "$state_tidb_namespace" != "$TIDB_NAMESPACE" ||
    "$state_tidb_cluster" != "$TIDB_CLUSTER" ||
    ! "$state_backup_sha" =~ ^[a-f0-9]{64}$ ||
    ! "$state_backup_revision" =~ ^[1-9][0-9]*$ ]]; then
    echo "destroy state does not match the requested instance operation" >&2
    exit 1
  fi
}

current_uid() {
  local kind="$1" namespace="$2" name="$3"
  "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" get "$kind" "$name" \
    -o 'jsonpath={.metadata.uid}' 2>/dev/null
}

state_has_pvc() {
  local name="$1" uid="$2"
  awk -F '\t' -v name="$name" -v uid="$uid" \
    '$1 == "PVC" && $5 == name && $6 == uid { found=1 } END { exit !found }' "$state_file"
}

validate_remaining_resources() {
  local require_all="$1" row api_version resource kind namespace name expected_uid actual_uid
  while IFS=$'\t' read -r row api_version resource kind namespace name expected_uid; do
    [[ "$row" == "RESOURCE" ]] || continue
    actual_uid="$(current_uid "$kind" "$namespace" "$name" || true)"
    if [[ -z "$actual_uid" ]]; then
      if [[ "$require_all" == "true" ]]; then
        echo "recorded resource disappeared before destruction: ${kind} ${namespace}/${name}" >&2
        exit 1
      fi
      continue
    fi
    if [[ "$actual_uid" != "$expected_uid" ]]; then
      echo "UID fence failed for ${kind} ${namespace}/${name}: expected ${expected_uid}, got ${actual_uid}" >&2
      exit 1
    fi
  done <"$state_file"

  local current_pvcs pvc_name pvc_uid component
  current_pvcs="$(pvc_snapshot)"
  while IFS=$'\t' read -r pvc_name pvc_uid component; do
    [[ -n "$pvc_name" ]] || continue
    if ! state_has_pvc "$pvc_name" "$pvc_uid"; then
      echo "unrecorded or replaced instance PVC detected: ${TIDB_NAMESPACE}/${pvc_name} uid=${pvc_uid}" >&2
      exit 1
    fi
  done <<<"$current_pvcs"
  if [[ "$require_all" == "true" ]]; then
    validate_pvc_rows "$current_pvcs"
  fi
}

wait_for_quiesced() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS)) status desired ready pods
  while true; do
    status="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" \
      -o 'jsonpath={.spec.replicas}{"\t"}{.status.readyReplicas}' 2>/dev/null || true)"
    IFS=$'\t' read -r desired ready <<<"$status"
    ready="${ready:-0}"
    pods="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get pods \
      -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${INSTANCE}" \
      -o name 2>/dev/null | wc -l | tr -d ' ')"
    if [[ "$desired" == "0" && "$ready" == "0" && "$pods" == "0" ]]; then
      return
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for KubeBrain to quiesce: desired=${desired:-missing} ready=${ready:-missing} pods=${pods:-missing}" >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done
}

ensure_quiesced_or_statefulset_absent() {
  if [[ -z "$(current_uid statefulset "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" || true)" ]]; then
    return
  fi
  wait_for_quiesced
}

uid_delete() {
  local api_version="$1" resource="$2" namespace="$3" name="$4" uid="$5"
  local args=(
    --api-version "$api_version" --resource "$resource"
    --namespace "$namespace" --name "$name" --uid "$uid"
    --timeout "${TIMEOUT_SECONDS}s"
  )
  [[ -n "$KUBE_CONTEXT" ]] && args+=(--context "$KUBE_CONTEXT")
  [[ -n "$KUBECONFIG_PATH" ]] && args+=(--kubeconfig "$KUBECONFIG_PATH")
  if [[ -n "$UID_DELETE" ]]; then
    "$UID_DELETE" "${args[@]}"
  else
    (cd "$ROOT_DIR" && go run ./hack/production/cmd/uid-delete "${args[@]}")
  fi
}

delete_recorded_kind() {
  local wanted="$1" row api_version resource kind namespace name uid
  while IFS=$'\t' read -r row api_version resource kind namespace name uid; do
    [[ "$row" == "RESOURCE" && "$kind" == "$wanted" ]] || continue
    uid_delete "$api_version" "$resource" "$namespace" "$name" "$uid"
  done <"$state_file"
}

wait_all_absent() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS)) remaining row api_version resource kind namespace name uid pvcs storage_workloads
  while true; do
    remaining=()
    while IFS=$'\t' read -r row api_version resource kind namespace name uid; do
      [[ "$row" == "RESOURCE" ]] || continue
      if [[ -n "$(current_uid "$kind" "$namespace" "$name" || true)" ]]; then
        remaining+=("${kind}:${namespace}/${name}")
      fi
    done <"$state_file"
    pvcs="$(pvc_snapshot)"
    storage_workloads="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get pods,statefulsets \
      -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER}" \
      -o name 2>/dev/null || true)"
    if [[ "${#remaining[@]}" -eq 0 && -z "$pvcs" && -z "$storage_workloads" ]]; then
      return
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for destroyed resources: ${remaining[*]:-none}; PVCs=${pvcs:-none}; storage workloads=${storage_workloads:-none}" >&2
      exit 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done
}

case "$ACTION" in
  prepare)
    [[ -n "$BACKUP_INPUT" ]] || { echo "BACKUP_INPUT is required for prepare" >&2; exit 2; }
    [[ -f "$BACKUP_INPUT" ]] || { echo "BACKUP_INPUT does not exist: ${BACKUP_INPUT}" >&2; exit 2; }
    backup_sha="$(INPUT="$BACKUP_INPUT" FIELD=sha256 EXPECTED_PREFIX="$BACKUP_PREFIX" \
      MIN_RECORDS="$BACKUP_MIN_RECORDS" MAX_AGE_SECONDS="$BACKUP_MAX_AGE_SECONDS" \
      "$LOGICAL_STATUS")"
    [[ "$backup_sha" =~ ^[a-f0-9]{64}$ ]] ||
      { echo "logical backup SHA-256 is invalid" >&2; exit 1; }
    backup_revision="$(INPUT="$BACKUP_INPUT" FIELD=revision EXPECTED_PREFIX="$BACKUP_PREFIX" \
      MIN_RECORDS="$BACKUP_MIN_RECORDS" MAX_AGE_SECONDS="$BACKUP_MAX_AGE_SECONDS" \
      "$LOGICAL_STATUS")"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.state.XXXXXX")"
    printf 'HEADER\tkubebrain.destroy.state.v1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$INSTANCE" "$OPERATION_ID" "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" \
      "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$backup_sha" "$backup_revision" >"$temporary"
    append_resource "$temporary" apps/v1 statefulsets statefulset \
      "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" kubebrain "$INSTANCE"
    append_resource "$temporary" v1 services service \
      "$KUBEBRAIN_NAMESPACE" "${KUBEBRAIN_STATEFULSET}-client" kubebrain "$INSTANCE"
    append_resource "$temporary" v1 services service \
      "$KUBEBRAIN_NAMESPACE" "${KUBEBRAIN_STATEFULSET}-peer" kubebrain "$INSTANCE"
    append_resource "$temporary" policy/v1 poddisruptionbudgets poddisruptionbudget \
      "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" kubebrain "$INSTANCE"
    append_resource "$temporary" v1 serviceaccounts serviceaccount \
      "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" kubebrain "$INSTANCE"
    append_resource "$temporary" pingcap.com/v1alpha1 tidbclusters tidbcluster \
      "$TIDB_NAMESPACE" "$TIDB_CLUSTER" tidb-cluster "$TIDB_CLUSTER"
    for suffix in pd tikv; do
      append_resource "$temporary" policy/v1 poddisruptionbudgets poddisruptionbudget \
        "$TIDB_NAMESPACE" "${TIDB_CLUSTER}-${suffix}" tidb-cluster "$TIDB_CLUSTER"
      append_resource "$temporary" v1 services service \
        "$TIDB_NAMESPACE" "${TIDB_CLUSTER}-${suffix}-metrics" tidb-cluster "$TIDB_CLUSTER"
    done
    pvcs="$(pvc_snapshot)"
    validate_pvc_rows "$pvcs"
    while IFS=$'\t' read -r pvc_name pvc_uid component; do
      printf 'PVC\tv1\tpersistentvolumeclaims\tpersistentvolumeclaim\t%s\t%s\t%s\t%s\n' \
        "$pvc_name" "$pvc_uid" "$component" "$TIDB_NAMESPACE" >>"$temporary"
    done <<<"$pvcs"
    atomic_publish "$temporary" "$state_file"
    echo "instance destruction prepare passed: instance=${INSTANCE} operation=${OPERATION_ID} backup_revision=${backup_revision}"
    ;;
  quiesce)
    require_confirmation
    [[ -f "$state_file" ]] || { echo "prepare evidence is missing" >&2; exit 1; }
    read_header
    validate_remaining_resources true
    "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" scale \
      statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null
    ensure_quiesced_or_statefulset_absent
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.quiesced.XXXXXX")"
    printf 'kubebrain.destroy.quiesced.v1\t%s\t%s\n' "$INSTANCE" "$OPERATION_ID" >"$temporary"
    atomic_publish "$temporary" "$quiesced_file"
    echo "instance destruction quiesce passed: instance=${INSTANCE} operation=${OPERATION_ID}"
    ;;
  destroy)
    require_confirmation
    [[ -f "$state_file" ]] || { echo "prepare evidence is missing" >&2; exit 1; }
    [[ -f "$quiesced_file" ]] || { echo "quiesce evidence is missing" >&2; exit 1; }
    read_header
    validate_marker "$quiesced_file" "kubebrain.destroy.quiesced.v1" "quiesced"
    validate_remaining_resources false
    ensure_quiesced_or_statefulset_absent
    delete_recorded_kind tidbcluster
    delete_recorded_kind persistentvolumeclaim
    for kind in statefulset service poddisruptionbudget serviceaccount; do
      delete_recorded_kind "$kind"
    done
    # PVC records have a different column layout from RESOURCE records.
    while IFS=$'\t' read -r row api_version resource kind pvc_name pvc_uid component namespace; do
      [[ "$row" == "PVC" ]] || continue
      uid_delete "$api_version" "$resource" "$namespace" "$pvc_name" "$pvc_uid"
    done <"$state_file"
    wait_all_absent
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.destroyed.XXXXXX")"
    printf 'kubebrain.destroy.resources-absent.v1\t%s\t%s\n' "$INSTANCE" "$OPERATION_ID" >"$temporary"
    atomic_publish "$temporary" "$destroyed_file"
    echo "instance destruction resources removed: instance=${INSTANCE} operation=${OPERATION_ID}"
    ;;
  complete)
    require_confirmation
    [[ -f "$state_file" ]] || { echo "prepare evidence is missing" >&2; exit 1; }
    [[ -f "$quiesced_file" ]] || { echo "quiesce evidence is missing" >&2; exit 1; }
    [[ -f "$destroyed_file" ]] || { echo "destroy evidence is missing" >&2; exit 1; }
    read_header
    validate_marker "$quiesced_file" "kubebrain.destroy.quiesced.v1" "quiesced"
    validate_marker "$destroyed_file" "kubebrain.destroy.resources-absent.v1" "destroyed"
    validate_remaining_resources false
    wait_all_absent
    require_jq
    IFS=$'\t' read -r _ _ _ _ _ _ _ _ backup_sha backup_revision <"$state_file"
    completed_at="$(date +%s)"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.receipt.XXXXXX")"
    "$JQ" -cnS \
      --arg instance "$INSTANCE" \
      --arg operation "$OPERATION_ID" \
      --arg kbns "$KUBEBRAIN_NAMESPACE" \
      --arg tidbns "$TIDB_NAMESPACE" \
      --arg tidb "$TIDB_CLUSTER" \
      --arg backup_sha "$backup_sha" \
      --argjson backup_revision "$backup_revision" \
      --argjson completed_at "$completed_at" \
      '{format:"kubebrain.destroy.receipt.v1",instance:$instance,operation_id:$operation,
        kubebrain_namespace:$kbns,tidb_namespace:$tidbns,tidb_cluster:$tidb,
        backup_sha256:$backup_sha,backup_revision:$backup_revision,
        resources_absent:true,completed_at_unix:$completed_at}' >"$temporary"
    if [[ -e "$receipt_file" ]]; then
      if ! validate_existing_receipt "$backup_sha" "$backup_revision"; then
        echo "existing destroy receipt does not match the completed operation" >&2
        rm -f "$temporary"
        exit 1
      fi
      rm -f "$temporary"
    else
      atomic_publish "$temporary" "$receipt_file"
    fi
    echo "instance destruction completion passed: instance=${INSTANCE} operation=${OPERATION_ID} receipt=${receipt_file}"
    ;;
esac
