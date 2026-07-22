#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

ACTION="${ACTION:-}"
CLEANUP_ID="${CLEANUP_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
DESTROY_RECEIPT_INPUT="${DESTROY_RECEIPT_INPUT:-}"
CONFIRM_CLEANUP="${CONFIRM_CLEANUP:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-}"
CREDENTIAL_NAMESPACE="${CREDENTIAL_NAMESPACE:-}"
CREDENTIAL_SECRETS="${CREDENTIAL_SECRETS:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
KUBECTL="${KUBECTL:-kubectl}"
UID_DELETE="${UID_DELETE:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

for variable in ACTION CLEANUP_ID INSTANCE STATE_DIR DESTROY_RECEIPT_INPUT \
  KUBEBRAIN_NAMESPACE TIDB_NAMESPACE CREDENTIAL_NAMESPACE CREDENTIAL_SECRETS; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; exit 2; }
done
[[ "$ACTION" =~ ^(prepare|delete|complete)$ ]] ||
  { echo "ACTION must be prepare, delete, or complete" >&2; exit 2; }
for variable in CLEANUP_ID INSTANCE KUBEBRAIN_NAMESPACE TIDB_NAMESPACE CREDENTIAL_NAMESPACE; do
  [[ "${!variable}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "${variable} contains unsupported characters" >&2; exit 2; }
done
[[ "$KUBEBRAIN_NAMESPACE" != "$TIDB_NAMESPACE" ]] ||
  { echo "KUBEBRAIN_NAMESPACE and TIDB_NAMESPACE must be distinct" >&2; exit 2; }
[[ "$CREDENTIAL_NAMESPACE" != "$KUBEBRAIN_NAMESPACE" &&
  "$CREDENTIAL_NAMESPACE" != "$TIDB_NAMESPACE" ]] ||
  { echo "credential secrets must live outside the dedicated data namespaces" >&2; exit 2; }
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
  { echo "TIMEOUT_SECONDS must be positive" >&2; exit 2; }
[[ "$POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] ||
  { echo "POLL_INTERVAL_SECONDS must be non-negative" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }

IFS=',' read -r -a credential_secrets <<<"$CREDENTIAL_SECRETS"
declare -A seen_secrets=()
for secret in "${credential_secrets[@]}"; do
  [[ "$secret" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] ||
    { echo "credential secret name is invalid: ${secret}" >&2; exit 2; }
  [[ -z "${seen_secrets[$secret]:-}" ]] ||
    { echo "credential secret is duplicated: ${secret}" >&2; exit 2; }
  seen_secrets["$secret"]=true
done

umask 077
mkdir -p "$STATE_DIR"
state_file="${STATE_DIR}/${CLEANUP_ID}.boundaries"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${CLEANUP_ID}.receipt.json}"
expected_confirmation="cleanup:${INSTANCE}:${CLEANUP_ID}"

kubectl_args=()
[[ -n "$KUBE_CONTEXT" ]] && kubectl_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")

atomic_publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    if cmp -s "$temporary" "$destination"; then
      rm -f "$temporary"
      return
    fi
    rm -f "$temporary"
    echo "refusing to overwrite boundary cleanup evidence: ${destination}" >&2
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}

destroy_receipt_sha() {
  sha256sum "$DESTROY_RECEIPT_INPUT" | awk '{print $1}'
}

validate_destroy_receipt() {
  [[ -f "$DESTROY_RECEIPT_INPUT" ]] ||
    { echo "destroy receipt does not exist" >&2; exit 1; }
  "$JQ" -e \
    --arg instance "$INSTANCE" \
    --arg kbns "$KUBEBRAIN_NAMESPACE" \
    --arg tidbns "$TIDB_NAMESPACE" \
    'keys == ["backup_revision","backup_sha256","completed_at_unix","format","instance","kubebrain_namespace","operation_id","resources_absent","tidb_cluster","tidb_namespace"] and
     .format == "kubebrain.destroy.receipt.v1" and
     .instance == $instance and .kubebrain_namespace == $kbns and
     .tidb_namespace == $tidbns and .resources_absent == true and
     (.tidb_cluster | type == "string" and length > 0) and
     (.operation_id | type == "string" and length > 0) and
     (.backup_sha256 | test("^[a-f0-9]{64}$")) and
     (.backup_revision | type == "number" and . > 0 and . == floor) and
     (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$DESTROY_RECEIPT_INPUT" >/dev/null ||
    { echo "destroy receipt does not authorize this boundary cleanup" >&2; exit 1; }
}

snapshot_namespace() {
  local name="$1" result uid instance dedicated
  result="$("$KUBECTL" "${kubectl_args[@]}" get namespace "$name" \
    -o 'jsonpath={.metadata.uid}{"\t"}{.metadata.labels.dbaas\.kubebrain\.io/instance}{"\t"}{.metadata.labels.dbaas\.kubebrain\.io/dedicated}')"
  IFS=$'\t' read -r uid instance dedicated <<<"$result"
  [[ -n "$uid" && "$instance" == "$INSTANCE" && "$dedicated" == "true" ]] ||
    { echo "namespace ${name} is not a dedicated boundary for instance ${INSTANCE}" >&2; exit 1; }
  printf '%s' "$uid"
}

snapshot_secret() {
  local name="$1" result uid instance credential
  result="$("$KUBECTL" "${kubectl_args[@]}" -n "$CREDENTIAL_NAMESPACE" get secret "$name" \
    -o 'jsonpath={.metadata.uid}{"\t"}{.metadata.labels.dbaas\.kubebrain\.io/instance}{"\t"}{.metadata.labels.dbaas\.kubebrain\.io/credential}')"
  IFS=$'\t' read -r uid instance credential <<<"$result"
  [[ -n "$uid" && "$instance" == "$INSTANCE" && "$credential" == "true" ]] ||
    { echo "secret ${CREDENTIAL_NAMESPACE}/${name} is not an owned instance credential" >&2; exit 1; }
  printf '%s' "$uid"
}

read_header() {
  local row format state_instance state_cleanup state_destroy_sha state_kbns state_kbuid
  local state_tidbns state_tidbuid state_credns
  IFS=$'\t' read -r row format state_instance state_cleanup state_destroy_sha \
    state_kbns state_kbuid state_tidbns state_tidbuid state_credns <"$state_file"
  [[ "$row" == "HEADER" && "$format" == "kubebrain.boundary-cleanup.state.v1" &&
    "$state_instance" == "$INSTANCE" && "$state_cleanup" == "$CLEANUP_ID" &&
    "$state_destroy_sha" == "$(destroy_receipt_sha)" &&
    "$state_kbns" == "$KUBEBRAIN_NAMESPACE" && "$state_tidbns" == "$TIDB_NAMESPACE" &&
    "$state_credns" == "$CREDENTIAL_NAMESPACE" && -n "$state_kbuid" && -n "$state_tidbuid" ]] ||
    { echo "boundary cleanup state does not match request or destroy receipt" >&2; exit 1; }
}

current_uid() {
  local kind="$1" namespace="$2" name="$3"
  if [[ -n "$namespace" ]]; then
    "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" get "$kind" "$name" \
      -o 'jsonpath={.metadata.uid}' 2>/dev/null
  else
    "$KUBECTL" "${kubectl_args[@]}" get "$kind" "$name" \
      -o 'jsonpath={.metadata.uid}' 2>/dev/null
  fi
}

uid_delete() {
  local api_version="$1" resource="$2" namespace="$3" name="$4" uid="$5"
  local args=(--api-version "$api_version" --resource "$resource" --name "$name" \
    --uid "$uid" --timeout "${TIMEOUT_SECONDS}s")
  [[ -n "$namespace" ]] && args+=(--namespace "$namespace")
  [[ -n "$KUBE_CONTEXT" ]] && args+=(--context "$KUBE_CONTEXT")
  [[ -n "$KUBECONFIG_PATH" ]] && args+=(--kubeconfig "$KUBECONFIG_PATH")
  if [[ -n "$UID_DELETE" ]]; then
    "$UID_DELETE" "${args[@]}"
  else
    (cd "$ROOT_DIR" && go run ./hack/production/cmd/uid-delete "${args[@]}")
  fi
}

validate_or_absent() {
  local kind="$1" namespace="$2" name="$3" expected_uid="$4" actual
  actual="$(current_uid "$kind" "$namespace" "$name" || true)"
  [[ -z "$actual" || "$actual" == "$expected_uid" ]] ||
    { echo "UID fence failed for ${kind} ${namespace:+${namespace}/}${name}" >&2; exit 1; }
}

wait_absent() {
  local deadline=$((SECONDS + TIMEOUT_SECONDS)) remaining row name uid
  while true; do
    remaining=0
    while IFS=$'\t' read -r row name uid; do
      [[ "$row" == "SECRET" ]] || continue
      [[ -z "$(current_uid secret "$CREDENTIAL_NAMESPACE" "$name" || true)" ]] || remaining=$((remaining + 1))
    done <"$state_file"
    [[ -z "$(current_uid namespace "" "$KUBEBRAIN_NAMESPACE" || true)" ]] || remaining=$((remaining + 1))
    [[ -z "$(current_uid namespace "" "$TIDB_NAMESPACE" || true)" ]] || remaining=$((remaining + 1))
    [[ "$remaining" -eq 0 ]] && return
    (( SECONDS < deadline )) ||
      { echo "timed out waiting for ${remaining} boundary resources to disappear" >&2; exit 1; }
    sleep "$POLL_INTERVAL_SECONDS"
  done
}

case "$ACTION" in
  prepare)
    validate_destroy_receipt
    temporary="$(mktemp "${STATE_DIR}/.${CLEANUP_ID}.boundaries.XXXXXX")"
    kb_uid="$(snapshot_namespace "$KUBEBRAIN_NAMESPACE")"
    tidb_uid="$(snapshot_namespace "$TIDB_NAMESPACE")"
    printf 'HEADER\tkubebrain.boundary-cleanup.state.v1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$INSTANCE" "$CLEANUP_ID" "$(destroy_receipt_sha)" \
      "$KUBEBRAIN_NAMESPACE" "$kb_uid" "$TIDB_NAMESPACE" "$tidb_uid" \
      "$CREDENTIAL_NAMESPACE" >"$temporary"
    for secret in "${credential_secrets[@]}"; do
      secret_uid="$(snapshot_secret "$secret")"
      printf 'SECRET\t%s\t%s\n' "$secret" "$secret_uid" >>"$temporary"
    done
    atomic_publish "$temporary" "$state_file"
    echo "boundary cleanup prepare passed: instance=${INSTANCE} cleanup=${CLEANUP_ID}"
    ;;
  delete)
    [[ "$CONFIRM_CLEANUP" == "$expected_confirmation" ]] ||
      { echo "CONFIRM_CLEANUP must exactly equal ${expected_confirmation}" >&2; exit 2; }
    validate_destroy_receipt
    [[ -f "$state_file" ]] || { echo "boundary cleanup state is missing" >&2; exit 1; }
    read_header
    while IFS=$'\t' read -r row name uid; do
      [[ "$row" == "SECRET" ]] || continue
      validate_or_absent secret "$CREDENTIAL_NAMESPACE" "$name" "$uid"
      uid_delete v1 secrets "$CREDENTIAL_NAMESPACE" "$name" "$uid"
    done <"$state_file"
    IFS=$'\t' read -r _ _ _ _ _ _ kb_uid _ tidb_uid _ <"$state_file"
    validate_or_absent namespace "" "$KUBEBRAIN_NAMESPACE" "$kb_uid"
    validate_or_absent namespace "" "$TIDB_NAMESPACE" "$tidb_uid"
    uid_delete v1 namespaces "" "$KUBEBRAIN_NAMESPACE" "$kb_uid"
    uid_delete v1 namespaces "" "$TIDB_NAMESPACE" "$tidb_uid"
    echo "boundary cleanup delete accepted: instance=${INSTANCE} cleanup=${CLEANUP_ID}"
    ;;
  complete)
    [[ "$CONFIRM_CLEANUP" == "$expected_confirmation" ]] ||
      { echo "CONFIRM_CLEANUP must exactly equal ${expected_confirmation}" >&2; exit 2; }
    validate_destroy_receipt
    [[ -f "$state_file" ]] || { echo "boundary cleanup state is missing" >&2; exit 1; }
    read_header
    wait_absent
    secrets_json="$("$JQ" -Rn '[inputs | split("\t") | select(.[0] == "SECRET") | .[1]]' <"$state_file")"
    if [[ -e "$receipt_file" ]]; then
      "$JQ" -e --arg instance "$INSTANCE" --arg cleanup "$CLEANUP_ID" \
        --arg destroy_sha "$(destroy_receipt_sha)" \
        --arg kbns "$KUBEBRAIN_NAMESPACE" --arg tidbns "$TIDB_NAMESPACE" \
        --arg credns "$CREDENTIAL_NAMESPACE" --argjson secrets "$secrets_json" \
        'keys == ["cleanup_id","completed_at_unix","credential_namespace","credential_secrets","credentials_absent","destroy_receipt_sha256","format","instance","kubebrain_namespace","namespaces_absent","tidb_namespace"] and
         .format == "kubebrain.boundary-cleanup.receipt.v1" and
         .instance == $instance and .cleanup_id == $cleanup and
         .destroy_receipt_sha256 == $destroy_sha and
         .kubebrain_namespace == $kbns and .tidb_namespace == $tidbns and
         .credential_namespace == $credns and .credential_secrets == $secrets and
         .namespaces_absent == true and .credentials_absent == true and
         (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
        "$receipt_file" >/dev/null ||
        { echo "existing boundary cleanup receipt does not match" >&2; exit 1; }
      echo "boundary cleanup completion passed: instance=${INSTANCE} receipt=${receipt_file}"
      exit 0
    fi
    temporary="$(mktemp "${STATE_DIR}/.${CLEANUP_ID}.receipt.XXXXXX")"
    "$JQ" -cnS \
      --arg instance "$INSTANCE" --arg cleanup "$CLEANUP_ID" \
      --arg destroy_sha "$(destroy_receipt_sha)" \
      --arg kbns "$KUBEBRAIN_NAMESPACE" --arg tidbns "$TIDB_NAMESPACE" \
      --arg credns "$CREDENTIAL_NAMESPACE" --argjson secrets "$secrets_json" \
      --argjson completed "$(date +%s)" \
      '{format:"kubebrain.boundary-cleanup.receipt.v1",instance:$instance,
        cleanup_id:$cleanup,destroy_receipt_sha256:$destroy_sha,
        kubebrain_namespace:$kbns,tidb_namespace:$tidbns,
        credential_namespace:$credns,credential_secrets:$secrets,
        namespaces_absent:true,credentials_absent:true,completed_at_unix:$completed}' >"$temporary"
    atomic_publish "$temporary" "$receipt_file"
    echo "boundary cleanup completion passed: instance=${INSTANCE} receipt=${receipt_file}"
    ;;
esac
