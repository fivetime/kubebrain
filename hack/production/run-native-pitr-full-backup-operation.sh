#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-/usr/local/bin/kubebrain-operationctl}"
BACKUP_COMMAND="${BACKUP_COMMAND:-/usr/local/bin/kubebrain-native-pitr-full-backup}"
BR_BINARY="${BR_BINARY:-/usr/local/bin/br}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
ENCRYPTION_DIR="${ENCRYPTION_DIR:-/var/run/secrets/kubebrain-native-pitr-encryption}"
JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_BACKUP_ATTESTATION_BYTES=1048576
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] && operation_is_positive_int64 "$LEASE_SECONDS" || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); fi
operation_is_positive_decimal_less_than_int "$HEARTBEAT_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
[[ -x "$OPERATIONCTL" && -x "$BACKUP_COMMAND" && -x "$BR_BINARY" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
command -v stat >/dev/null || die "stat is required"
command -v id >/dev/null || die "id is required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }
operation_parameters_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]]; }
require_operation_parameters_size() {
  operation_parameters_size_is_valid "$1" || {
    runctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null
    echo "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >&2
    exit 1
  }
}

claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRFullBackup --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "native PITR claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^native-pitr-full-[a-f0-9]{20}$ && "$operation_id" == "$name" &&
  "$instance" == kubebrain && "$type" == NativePITRFullBackup && "$requester" == platform:native-pitr-full-backup &&
  "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" && "$key" == parameters.json &&
  "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "native PITR claim identity is invalid"
[[ "$name" == "native-pitr-full-${expected_sha:0:20}" ]] || die "native PITR operation name does not bind the parameter digest"

capture="$(mktemp -d "$WORK_DIR/native-pitr-full.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || operation_kill_process_group "$child"; [[ $heartbeat == 0 ]] || operation_kill_process_group "$heartbeat"; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    operation_kill_process_group "$heartbeat"
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; native PITR worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; native PITR worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" ]] || { runctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR parameters digest mismatch" >/dev/null; exit 1; }
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_operation_parameters_size "$params"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$expected_sha" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || die "native PITR parameters changed during capture"
$JQ -e '(keys==["backup_ts","pd_addrs","storage_prefix"] or
  keys==["backup_ts","cipher_method","encryption_key_id","pd_addrs","storage_prefix"]) and
  (.backup_ts|type=="string" and test("^[1-9][0-9]*$")) and
  (.storage_prefix|type=="string" and startswith("s3://")) and
  (.pd_addrs|type=="array" and length>0 and length<=32 and all(.[]; type=="string" and length>0 and (contains(",")|not))) and
  ((has("cipher_method")|not) or (.cipher_method=="aes256-ctr" and
    (.encryption_key_id|type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$"))))' "$params" >/dev/null || die "native PITR parameter schema is invalid"
backup_ts="$($JQ -r .backup_ts "$params")"; storage_prefix="$($JQ -r .storage_prefix "$params")"
pd_addrs="$($JQ -r '.pd_addrs|join(",")' "$params")"
encryption_args=(); cipher_method=plaintext
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  cipher_method=aes256-ctr
  encryption_key_id="$($JQ -r .encryption_key_id "$params")"
  [[ -f "$ENCRYPTION_DIR/key" && -f "$ENCRYPTION_DIR/key-id" ]] || die "native PITR encryption key and key-id files are required"
  mounted_key_id="$(<"$ENCRYPTION_DIR/key-id")"
  [[ "$mounted_key_id" == "$encryption_key_id" ]] || die "native PITR encryption key version does not match operation parameters"
  encryption_args=(--crypter-method=aes256-ctr --encryption-key-id="$encryption_key_id" --encryption-key-file="$ENCRYPTION_DIR/key")
fi
attestation="$WORK_DIR/${name}.native-pitr-full-backup-attestation.json"
verify_existing_attestation() {
  local size attributes expected_pd_sha expected_args_json expected_args_sha
  [[ -f "$attestation" && ! -L "$attestation" ]] || return 1
  size="$(stat -Lc '%s' -- "$attestation")" || return 1
  attributes="$(stat -Lc '%a:%u:%h' -- "$attestation")" || return 1
  [[ "$size" =~ ^[0-9]+$ && "$size" -gt 0 && "$size" -le "$MAX_BACKUP_ATTESTATION_BYTES" ]] || return 1
  [[ "$attributes" == "600:$(id -u):1" ]] || return 1
  local expected_pd_json
  expected_pd_json="$("$JQ" -c '.pd_addrs|sort' "$params")" || return 1
  expected_pd_sha="$(printf '%s' "$expected_pd_json" | sha256sum | cut -d ' ' -f1)" || return 1
  if [[ ${#encryption_args[@]} -gt 0 ]]; then
    expected_args_json="$("$JQ" -cn --arg storage "$storage_prefix" --arg backup_ts "$backup_ts" --arg key_id "$encryption_key_id" \
      '["backup","txn","--storage="+$storage,"--backupts="+$backup_ts,"--crypter.method=aes256-ctr","--crypter.key-id="+$key_id]')" || return 1
  else
    expected_args_json="$("$JQ" -cn --arg storage "$storage_prefix" --arg backup_ts "$backup_ts" \
      '["backup","txn","--storage="+$storage,"--backupts="+$backup_ts,"--crypter.method=plaintext"]')" || return 1
  fi
  expected_args_sha="$(printf '%s' "$expected_args_json" | sha256sum | cut -d ' ' -f1)" || return 1
  "$JQ" -e --arg storage "$storage_prefix" --argjson backup_ts "$backup_ts" --arg pd_sha "$expected_pd_sha" \
    --arg args_sha "$expected_args_sha" --argjson args "$expected_args_json" --arg cipher "${cipher_method:-plaintext}" \
    --arg key_id "${encryption_key_id:-}" '
    ((($cipher == "plaintext") and keys == ["backup_ts","br_binary_sha256","br_version","canonical_args","canonical_args_sha256","cipher_method","completed_at_unix","exit_successful","format","pd_addresses_sha256","storage_prefix"]) or
     (($cipher == "aes256-ctr") and keys == ["backup_ts","br_binary_sha256","br_version","canonical_args","canonical_args_sha256","cipher_method","completed_at_unix","encryption_key_id","exit_successful","format","pd_addresses_sha256","storage_prefix"])) and
    .format == "kubebrain.native-pitr-full-backup-attestation.v3" and
    (.br_version|type == "string" and length <= 16384 and contains("Release Version: v7.5.1\n") and contains("Git Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n") and (contains("\u0000")|not)) and
    (.br_binary_sha256|test("^[a-f0-9]{64}$")) and .canonical_args == $args and
    .canonical_args_sha256 == $args_sha and .pd_addresses_sha256 == $pd_sha and
    .storage_prefix == $storage and .backup_ts == $backup_ts and .cipher_method == $cipher and
    (($cipher == "plaintext" and (has("encryption_key_id")|not)) or
     ($cipher == "aes256-ctr" and .encryption_key_id == $key_id)) and
    (.completed_at_unix|type == "number" and . > 0 and . == floor) and .exit_successful == true' "$attestation" >/dev/null || return 1
  local verify_args=(--verify-attestation="$attestation" --pd-addrs="$pd_addrs" --storage-prefix="$storage_prefix" --backup-ts="$backup_ts" --crypter-method="$cipher_method")
  [[ "$cipher_method" == plaintext ]] || verify_args+=(--encryption-key-id="$encryption_key_id")
  "$BACKUP_COMMAND" "${verify_args[@]}" >/dev/null
}
capture_validated_attestation_digest() {
  local first second
  verify_existing_attestation || return 1
  first="$(sha "$attestation")" || return 1
  verify_existing_attestation || return 1
  second="$(sha "$attestation")" || return 1
  [[ "$first" == "$second" ]] || return 1
  printf '%s\n' "$first" >"$capture/attestation.sha"
  chmod 600 "$capture/attestation.sha"
}
if [[ "$attempt" == 2 ]]; then
  set -m
  capture_validated_attestation_digest & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/verifier.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; verify_rc=$?; set -e; : >"$capture/verifier.done"; child=0
  finalize_heartbeat || exit 1
  if [[ "$verify_rc" == 0 ]]; then
    receipt_sha="$(<"$capture/attestation.sha")"
    [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated native PITR attestation digest is invalid"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled durable native PITR full backup attestation without repeating BR" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full backup exhausted without a valid durable attestation; inspect the immutable storage prefix before any new operation" >/dev/null
  exit 1
fi
[[ ! -e "$attestation" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR attestation already exists; inspect before a new operation" >/dev/null; exit 1; }
for file in ca.crt tls.crt tls.key; do [[ -f "$TLS_DIR/$file" ]] || die "native PITR TLS file $file is required"; done

set -m
"$BACKUP_COMMAND" --br-binary="$BR_BINARY" --pd-addrs="$pd_addrs" --storage-prefix="$storage_prefix" \
  --backup-ts="$backup_ts" --ca="$TLS_DIR/ca.crt" --cert="$TLS_DIR/tls.crt" --key="$TLS_DIR/tls.key" \
  "${encryption_args[@]}" \
  --attestation-output="$attestation" >"$capture/backup.log" 2>&1 & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
set +m
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
if [[ $rc != 0 ]] || ! capture_validated_attestation_digest; then
  finalize_heartbeat || exit 1
  echo "native PITR full backup exited ${rc} or left no valid durable attestation; a later claim must inspect the immutable prefix" >&2
  exit 1
fi
receipt_sha="$(<"$capture/attestation.sha")"; [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated native PITR attestation digest is invalid"
finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "native PITR full backup completed" >/dev/null
